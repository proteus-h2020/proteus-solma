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

package eu.proteus.solma.coirr

import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{DenseVector, diag}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.coirr.COIRR.COIRRStreamEvent
import eu.proteus.solma.coirr.algorithm.COIRRAlgorithm
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.server.RangePSLogicWithClose
import hu.sztaki.ilab.ps.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.util.XORShiftRandom

@Proteus
class COIRRPrequentialTraining extends TransformDataStreamOperation[
      COIRR,
      COIRR.COIRRStreamEvent,
      Either[(COIRR.UnlabeledVector, Double), (Int, COIRR.COIRRModel)]] {


  def init(a: Double, b: Double, c: Double, lambda: Double, n: Int): Int => COIRR.COIRRModel =
    _ => (diag(DenseVector.fill(n){a}), DenseVector.fill(n){b}, DenseVector.fill(n){c}, lambda)

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
      instance: COIRR,
      transformParameters: ParameterMap,
      input: DataStream[COIRRStreamEvent]
  ): DataStream[Either[(COIRR.UnlabeledVector, Double), (Int, COIRR.COIRRModel)]] = {

    val workerParallelism: Int = instance.getWorkerParallelism
    val psParallelism: Int  = instance.getPSParallelism
    val pullLimit: Int  = instance.getPullLimit
    val featureCount: Int  = instance.getFeaturesCount
    val iterationWaitTime: Long  = instance.getIterationWaitTime
    val allowedLateness: Long  = instance.getAllowedLateness

    val updater = (currModel: COIRR.COIRRModel, gradient: COIRR.COIRRModel) => {
      (gradient._1, gradient._2, gradient._3, gradient._4)
    }

    val rnd = new XORShiftRandom()

    val initializer = init(rnd.nextDouble(), rnd.nextDouble(), rnd.nextDouble(), rnd.nextDouble(), featureCount)

    val workerLogic =  WorkerLogic.addPullLimiter(
      new COIRRWorkerLogic(
        new COIRRAlgorithm(instance)),
      pullLimit)

    val serverLogic = new RangePSLogicWithClose[COIRR.COIRRModel](featureCount, initializer, updater)
    val paramPartitioner: WorkerToPS[COIRR.COIRRModel] => Int = rangePartitionerPS(featureCount)(psParallelism)

    val wInPartition: PSToWorker[COIRR.COIRRModel] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    implicit val inputTypeInfo = createTypeInformation[COIRRStreamEvent]
    FlinkParameterServer.transform(input, workerLogic, serverLogic, workerParallelism, psParallelism, iterationWaitTime)
  }
}
