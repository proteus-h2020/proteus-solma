/*
 * Copyright (C) 2017 The Proteus Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.proteus.solma.asvi

import breeze.linalg.{DenseMatrix, DenseVector, randomDouble}
import breeze.stats.distributions.Gamma
import eu.proteus.annotations.Proteus
import eu.proteus.solma.asvi.AsynSVI._
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.server.RangePSLogicWithClose
import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
import hu.sztaki.ilab.ps.server.sender.SimplePSSender
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServer, WorkerLogic}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}

@Proteus
class AsynchronousStochasticVariationalInference extends TransformDataStreamOperation[
    AsynSVI,
    UnlabeledVector,
    Either[UnlabeledVector, (Int, ASVIModel)]] {

  def init(n: Int, K: Int): Int => ASVIModel = {
    _ => {
      DenseMatrix(Gamma(100.0, 1.0/100.0).sample(n * K).toArray).reshape(n, K).t
    }
  }

  def rangePartitionerPS[P](featureCount: Int)(psParallelism: Int): WorkerToPS[P] => Int = {
    val partitionSize = Math.ceil(featureCount.toDouble / psParallelism).toInt
    val partitonerFunction = (paramId: Int) => Math.abs(paramId) / partitionSize

    val paramPartitioner: WorkerToPS[P] => Int = {
      case WorkerToPS(_, msg) => msg match {
        case Left(Pull(paramId)) => partitonerFunction(paramId)
        case Right(Push(paramId, _)) => partitonerFunction(paramId)
      }
    }

    paramPartitioner
  }


  override def transformDataStream(
    instance: AsynSVI,
    transformParameters: ParameterMap,
    input: DataStream[UnlabeledVector]
  ): DataStream[Either[UnlabeledVector, (Int, AsynSVI.ASVIModel)]] = {

    val workerParallelism = instance.getWorkerParallelism
    val psParallelism = instance.getPSParallelism
    val pullLimit = instance.getPullLimit
    val featureCount = instance.getFeaturesCount
    val K = instance.getNumOfTopics
    val iterationWaitTime = instance.getIterationWaitTime
    val vocabulary = instance.getInitialVocabulary

    val workerLogic = WorkerLogic.addPullLimiter(
      new ASVIWorkerLogic(new LDA(K, vocab = vocabulary)),
      pullLimit
    )

    val updater = (currModel: ASVIModel, gradient: ASVIModel) => {
      val newModel = currModel + gradient
      newModel :/= workerParallelism.toDouble
      newModel
    }

    val initializer = init(featureCount, K)

    val paramPartitioner: WorkerToPS[ASVIModel] => Int = rangePartitionerPS(featureCount)(psParallelism)

    val wInPartition: PSToWorker[ASVIModel] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    val serverLogic = new RangePSLogicWithClose[ASVIModel](featureCount, initializer, updater) {
      override def onPushRecv(
          id: Int,
          deltaUpdate: ASVIModel,
          ps: ParameterServer[ASVIModel, (Int, ASVIModel)]
      ): Unit = {
        super.onPushRecv(id, deltaUpdate, ps)
        ps.output((0, params(0).get))
      }
    }

    implicit val inputTypeInfo = createTypeInformation[UnlabeledVector]

    val partitionedInput: DataStream[UnlabeledVector] = input.partitionCustom(
      new Partitioner[Long] {
        override def partition(k: Long, total: Int): Int = {
          (k % total).toInt
        }
      },
      (event: UnlabeledVector) => event._1)

   FlinkParameterServer.transform(
     partitionedInput,
     workerLogic,
     serverLogic,
     paramPartitioner,
     wInPartition,
     workerParallelism,
     psParallelism,
     new SimpleWorkerReceiver[ASVIModel],
     new SimpleWorkerSender[ASVIModel],
     new SimplePSReceiver[ASVIModel],
     new SimplePSSender[ASVIModel],
     iterationWaitTime
   )

  }
}
