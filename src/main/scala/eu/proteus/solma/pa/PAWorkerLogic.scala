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

import breeze.linalg.{DenseMatrix, DenseVector}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.pa.PA.{PAModel, PAStreamEvent, UnlabeledVector}
import eu.proteus.solma.pa.algorithm.BasePAAlgorithm
import eu.proteus.solma.utils.BinaryConfusionMatrixBuilder
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}
import org.apache.flink.ml.math.Breeze._

import scala.collection.mutable

@Proteus
class PAWorkerLogic(
                     algorithm: BasePAAlgorithm[UnlabeledVector, Double, PAModel]
                   ) extends WorkerLogic[PAStreamEvent, PA.PAModel, (Long, PA.UnlabeledVector, Double)] {

  val unpredictedVecs = new mutable.Queue[(Long, PA.UnlabeledVector)]()
  val unlabeledVecs = new mutable.HashMap[Long, PA.UnlabeledVector]()
  val labeledVecs = new mutable.Queue[(PA.UnlabeledVector, Double, Long)]()

  val cfBuilder = new BinaryConfusionMatrixBuilder

  override def onRecv(
                       data: PAStreamEvent,
                       ps: ParameterServerClient[PA.PAModel, (Long, PA.UnlabeledVector, Double)]): Unit = {

    data match {
      case Left((index, dataPoint)) =>
        // store unlabelled point and pull
        unpredictedVecs.enqueue((index, dataPoint.data.asBreeze))
        unlabeledVecs(index) = dataPoint.data.asBreeze
      case Right((index, expected)) =>
        // we got a labelled point
        unlabeledVecs.remove(index) match {
          case Some(unlabeledVector) => labeledVecs.enqueue((unlabeledVector, expected, index))
          case None => throw new IllegalStateException("Got label for unseen data point")
        }
    }
    ps.pull(0)
  }

  override def onPullRecv(
                           paramId: Int,
                           currentModel: PA.PAModel,
                           ps: ParameterServerClient[PA.PAModel, (Long, PA.UnlabeledVector, Double)]): Unit = {

    var modelOpt: Option[PA.PAModel] = None

    while (unpredictedVecs.nonEmpty) {
      val (index, dataPoint) = unpredictedVecs.dequeue()
      ps.output((index, dataPoint, algorithm.predict(dataPoint, currentModel)))
    }

    while (labeledVecs.nonEmpty) {
      val (dataPoint, prediction, index) = labeledVecs.dequeue()
      ps.push(0, algorithm.delta(dataPoint, currentModel, prediction, index))
    }

  }
}
