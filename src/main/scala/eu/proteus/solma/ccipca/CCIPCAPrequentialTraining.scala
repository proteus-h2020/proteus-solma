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

package eu.proteus.solma.ccipca

import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{DenseVector, diag, DenseMatrix}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.ccipca.algorithm.CCIPCAAlgorithm
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.server.RangePSLogicWithClose
import hu.sztaki.ilab.ps.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.util.XORShiftRandom
import scala.math.min

@Proteus
class CCIPCAPrequentialTraining extends TransformDataStreamOperation[
      CCIPCA,
      DenseMatrix[Double],
      Either[(DenseMatrix[Double], Double), (Int, CCIPCA.CCIPCAModel)]] {

  def init(dim: Int, n: Int): Int => CCIPCA.CCIPCAModel =
    _ => DenseMatrix.zeros[Double](min(dim, n), min(dim, n))

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
      instance: CCIPCA,
      transformParameters: ParameterMap,
      input: DataStream[DenseMatrix[Double]]
  ): DataStream[Either[(DenseMatrix[Double], Double), (Int, CCIPCA.CCIPCAModel)]] = {

    val workerParallelism: Int = instance.getWorkerParallelism
    val psParallelism: Int  = instance.getPSParallelism
    val pullLimit: Int  = instance.getPullLimit
    val featureCount: Int  = instance.getFeaturesCount
    val iterationWaitTime: Long  = instance.getIterationWaitTime
    val allowedLateness: Long  = instance.getAllowedLateness

    val updater = (currModel: CCIPCA.CCIPCAModel, gradient: CCIPCA.CCIPCAModel) => {
      gradient
    }

    val rnd = new XORShiftRandom()

    val initializer = init(featureCount, featureCount)

    val workerLogic =  WorkerLogic.addPullLimiter(
      new CCIPCAWorkerLogic(
        new CCIPCAAlgorithm(instance)),
      pullLimit)

    val serverLogic = new RangePSLogicWithClose[CCIPCA.CCIPCAModel](featureCount, initializer, updater)
    val paramPartitioner: WorkerToPS[CCIPCA.CCIPCAModel] => Int = rangePartitionerPS(featureCount)(psParallelism)

    val wInPartition: PSToWorker[CCIPCA.CCIPCAModel] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    implicit val inputTypeInfo = createTypeInformation[DenseMatrix[Double]]
    FlinkParameterServer.transform(input, workerLogic, serverLogic, workerParallelism, psParallelism, iterationWaitTime)
  }
}
