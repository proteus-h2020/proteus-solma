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

package eu.proteus.solma.pa

import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{DenseMatrix, DenseVector, diag, randomDouble}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.pa.PA.PAStreamEvent
import eu.proteus.solma.pa.algorithm.PAAlgorithm
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.server.RangePSLogicWithClose
import hu.sztaki.ilab.ps.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.util.XORShiftRandom

@Proteus
class PAPrequentialTraining extends TransformDataStreamOperation[
  PA,
  PA.PAStreamEvent,
  Either[(Long, PA.UnlabeledVector, Double), (Int, PA.PAModel)]] {


  def init(n : Int, minW: Double, maxW: Double): Int => PA.PAModel =
    _ => {
      val w = randomDouble(n, (minW, maxW))
      //val bias = randomDouble(1, (minBias, maxBias))
      (w)
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
                                    instance: PA,
                                    transformParameters: ParameterMap,
                                    input: DataStream[PAStreamEvent]
                                  ): DataStream[Either[(Long, PA.UnlabeledVector, Double), (Int, PA.PAModel)]] = {

    val workerParallelism: Int = instance.getWorkerParallelism
    val psParallelism: Int  = instance.getPSParallelism
    val pullLimit: Int  = instance.getPullLimit
    val featureCount: Int  = instance.getFeaturesCount
    val iterationWaitTime: Long  = instance.getIterationWaitTime
    val allowedLateness: Long  = instance.getAllowedLateness

    val updater = (currModel: PA.PAModel, update: PA.PAModel) => {
      currModel + update
    }

    val rnd = new XORShiftRandom()

    val initializer = init(featureCount, -8.0, 4.0)

    val workerLogic =  WorkerLogic.addPullLimiter(
      new PAWorkerLogic(
        new PAAlgorithm(instance)),
      pullLimit)

    val serverLogic = new RangePSLogicWithClose[PA.PAModel](featureCount, initializer, updater)
    val paramPartitioner: WorkerToPS[PA.PAModel] => Int = rangePartitionerPS(featureCount)(psParallelism)

    val wInPartition: PSToWorker[PA.PAModel] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    implicit val inputTypeInfo = createTypeInformation[PAStreamEvent]

    val partitionedInput = input.partitionCustom(
      new Partitioner[Long] {
        override def partition(k: Long, total: Int): Int = {
          (k % total).toInt
        }
      },
      (event: PA.PAStreamEvent) => event match {
        case Left(v) => v._1
        case Right(v) => v._1
      })

    FlinkParameterServer.transform(partitionedInput, workerLogic, serverLogic, workerParallelism, psParallelism, iterationWaitTime)
  }
}
