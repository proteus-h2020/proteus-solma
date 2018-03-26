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

package eu.proteus.solma.lasso

import java.util.Calendar
import breeze.linalg.{DenseVector => DenseBreezeVector, Vector => BreezeVector}
import org.apache.flink.ml.math.Breeze._
import eu.proteus.solma.events.StreamEventWithPos
import eu.proteus.solma.lasso.Lasso.{LassoModel, LassoParam, OptionLabeledVector}
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import eu.proteus.solma.lasso.algorithm.{FlatnessMappingAlgorithm, LassoAlgorithm, LassoBasicAlgorithm}
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}

import scala.collection.mutable

class LassoWorkerLogic (modelBuilder: ModelBuilder[LassoParam, LassoModel],
                        lassoMethod: LassoAlgorithm[OptionLabeledVector, LassoParam, ((Long, Double), Double),
                          LassoModel]) extends WorkerLogic[LassoStreamEvent, LassoParam, ((Long, Double), Double)]
{
  val unpredictedVecs = new mutable.Queue[LassoStreamEvent]()
  var unlabeledVecs = new mutable.HashMap[Long, (Long, mutable.Queue[StreamEventWithPos[(Long, Double)]])]()
  val labeledVecs = new mutable.Queue[OptionLabeledVector]
  val maxTTL: Long = 60 * 60 * 1000

  override def onRecv(data: LassoStreamEvent,
                      ps: ParameterServerClient[LassoParam, ((Long, Double), Double)]): Unit = {
    val now = Calendar.getInstance.getTimeInMillis

    data match {
      case Left(v) =>
        if (!unlabeledVecs.keys.exists(x => x == v.pos._1)) {
          unlabeledVecs(v.pos._1) = (now, new mutable.Queue[StreamEventWithPos[(Long, Double)]]())
        }
        unlabeledVecs(v.pos._1)._2.enqueue(v)
        unpredictedVecs.enqueue(data)
        ps.pull(0)
      case Right(v) =>
        if (unlabeledVecs.keys.exists(x => x == v.label)) {
          val poses = unlabeledVecs(v.label)._2.toVector.map(x => x.pos._2)
          var labels = Vector[(Double, Double)]()

          for (i <- 0 until v.labels.data.length) {
            labels = (v.poses(i), v.labels(i)) +: labels
          }

          val interpolatedLabels = new FlatnessMappingAlgorithm(poses, labels).apply

          val processedEvents: Iterable[OptionLabeledVector] = unlabeledVecs(v.label)._2.toVector.zipWithIndex.map(
            zipped => {
              val data: BreezeVector[Double] = DenseBreezeVector.fill(76){0.0}
              data(zipped._1.slice.head) = zipped._1.data(zipped._1.slice.head)
              val vec: OptionLabeledVector = Left(((zipped._1.pos, data/*zipped._1.data.asBreeze*/),
                interpolatedLabels(zipped._2)))
              vec
            }
          )
          labeledVecs ++= processedEvents
        }
        ps.pull(0)
    }
    unlabeledVecs = unlabeledVecs.filter(x => now - x._2._1 < maxTTL)
  }

  override def onPullRecv(paramId: Int,
                          modelValue: LassoParam,
                          ps: ParameterServerClient[LassoParam, ((Long, Double), Double)]):Unit = {

    var model: Option[LassoModel] = None

    while (unpredictedVecs.nonEmpty) {
      val dataPoint = unpredictedVecs.dequeue()
      val prediction = lassoMethod.predict(LassoBasicAlgorithm.toOptionLabeledVector(dataPoint), modelValue)
      ps.output(prediction)
    }

    while (labeledVecs.nonEmpty) {
      val restedData = labeledVecs.dequeue()
      restedData match {
        case Left(v) => model = Some(lassoMethod.delta(restedData, modelValue, v._2).head._2)
        case Right(v) => //It must not be processed here
      }
    }
    if (model.nonEmpty) {
      ps.push(0, model.get)
    }
  }
}

