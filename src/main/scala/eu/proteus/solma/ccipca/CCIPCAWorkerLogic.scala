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

import breeze.linalg.{DenseMatrix, DenseVector}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.ccipca.CCIPCA.{CCIPCAModel}
import eu.proteus.solma.ccipca.algorithm.BaseCCIPCAAlgorithm
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}
import org.apache.flink.ml.math.Breeze._

import scala.collection.mutable

@Proteus
class CCIPCAWorkerLogic(
                      algorithm: BaseCCIPCAAlgorithm[DenseMatrix[Double], CCIPCAModel]
                    ) extends WorkerLogic[DenseMatrix[Double], CCIPCA.CCIPCAModel, (DenseMatrix[Double], Double)] {

  val unpredictedVecs = new mutable.Queue[DenseMatrix[Double]]()
  val maxTTL: Long = 60 * 60 * 1000

  override def onRecv(
                      dataPoint: DenseMatrix[Double],
                      ps: ParameterServerClient[CCIPCAModel, (DenseMatrix[Double], Double)]): Unit = {
    // store unlabelled point and pull
    unpredictedVecs.enqueue(dataPoint)
    ps.pull(0)
  }

  override def onPullRecv(
                          paramId: Int,
                          currentModel: CCIPCA.CCIPCAModel,
                          ps: ParameterServerClient[CCIPCAModel, (DenseMatrix[Double], Double)]): Unit = {

    while (unpredictedVecs.nonEmpty) {
      val dataPoint = unpredictedVecs.dequeue()
    //  ps.push(0, algorithm.compute(dataPoint, currentModel))
    }

  }
}
