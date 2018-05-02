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

package eu.proteus.solma.osvm

import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{DenseMatrix, DenseVector, diag}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.osvm.OSVM.OSVMStreamEvent
import eu.proteus.solma.osvm.algorithm.OSVMAlgorithm
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.server.RangePSLogicWithClose
import hu.sztaki.ilab.ps.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.util.XORShiftRandom

@Proteus
class OSVMPrequentialTraining extends TransformDataStreamOperation[
      OSVM,
      OSVM.OSVMStreamEvent,
      Either[(OSVM.UnlabeledVector, Double), (Int, OSVM.OSVMModel)]] {


   def init(n : Int, bias: Double): Int => OSVM.OSVMModel =
    _ => (DenseVector.rand(n), bias)

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
      instance: OSVM,
      transformParameters: ParameterMap,
      input: DataStream[OSVMStreamEvent]
  ): DataStream[Either[(OSVM.UnlabeledVector, Double), (Int, OSVM.OSVMModel)]] = {

    val workerParallelism: Int = instance.getWorkerParallelism
    val psParallelism: Int  = instance.getPSParallelism
    val pullLimit: Int  = instance.getPullLimit
    val featureCount: Int  = instance.getFeaturesCount
    val iterationWaitTime: Long  = instance.getIterationWaitTime
    val allowedLateness: Long  = instance.getAllowedLateness

    val updater = (currModel: OSVM.OSVMModel, gradient: OSVM.OSVMModel) => {
      (currModel._1 - gradient._1, currModel._2 - gradient._2)
    }

    val rnd = new XORShiftRandom()

    val initializer = init(featureCount, rnd.nextDouble())

    val workerLogic =  WorkerLogic.addPullLimiter(
      new OSVMWorkerLogic(
        new OSVMAlgorithm(instance)),
    pullLimit)

    val serverLogic = new RangePSLogicWithClose[OSVM.OSVMModel](featureCount, initializer, updater)
    val paramPartitioner: WorkerToPS[OSVM.OSVMModel] => Int = rangePartitionerPS(featureCount)(psParallelism)

    val wInPartition: PSToWorker[OSVM.OSVMModel] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    implicit val inputTypeInfo = createTypeInformation[OSVMStreamEvent]
    FlinkParameterServer.transform(input, workerLogic, serverLogic, workerParallelism, psParallelism, iterationWaitTime)
  }
}
