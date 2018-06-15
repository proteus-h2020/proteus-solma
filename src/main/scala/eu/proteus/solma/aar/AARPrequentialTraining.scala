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

package eu.proteus.solma.aar

import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{DenseVector, diag}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.aar.AAR.AARStreamEvent
import eu.proteus.solma.aar.algorithm.AARAlgorithm
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.server.RangePSLogicWithClose
import hu.sztaki.ilab.ps.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.util.XORShiftRandom

@Proteus
class AARPrequentialTraining extends TransformDataStreamOperation[
      AAR,
      AAR.AARStreamEvent,
      Either[(AAR.UnlabeledVector, Double), (Int, AAR.AARModel)]] {


  def init(a: Double, b: Double, n: Int): Int => AAR.AARModel =
    _ => (diag(DenseVector.fill(n){a}), DenseVector.fill(n){b})

  def rangePartitionerPS[P](featureCount: Int)(psParallelism: Int): (WorkerToPS[P]) => Int = {
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
      instance: AAR,
      transformParameters: ParameterMap,
      input: DataStream[AARStreamEvent]
  ): DataStream[Either[(AAR.UnlabeledVector, Double), (Int, AAR.AARModel)]] = {

    val workerParallelism: Int = instance.getWorkerParallelism
    val psParallelism: Int  = instance.getPSParallelism
    val pullLimit: Int  = instance.getPullLimit
    val featureCount: Int  = instance.getFeaturesCount
    val iterationWaitTime: Long  = instance.getIterationWaitTime
    val allowedLateness: Long  = instance.getAllowedLateness

    val updater = (currModel: AAR.AARModel, gradient: AAR.AARModel) => {
      (gradient._1, gradient._2)
    }

    val rnd = new XORShiftRandom()

    val initializer = init(rnd.nextDouble(), rnd.nextDouble(), featureCount)

    val workerLogic =  WorkerLogic.addPullLimiter(
      new AARWorkerLogic(
        new AARAlgorithm(instance)),
      pullLimit)

    val serverLogic = new RangePSLogicWithClose[AAR.AARModel](featureCount, initializer, updater)
    val paramPartitioner: WorkerToPS[AAR.AARModel] => Int = rangePartitionerPS(featureCount)(psParallelism)

    val wInPartition: PSToWorker[AAR.AARModel] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    implicit val inputTypeInfo = createTypeInformation[AARStreamEvent]
    FlinkParameterServer.transform(input, workerLogic, serverLogic, workerParallelism, psParallelism, iterationWaitTime)
  }
}
