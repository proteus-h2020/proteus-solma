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
import org.apache.flink.ml.math.{Vector => FlinkVector}
import eu.proteus.solma.events.StreamEventWithPos
import eu.proteus.solma.lasso.Lasso.{LassoModel, LassoParam, OptionLabeledVector}
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import eu.proteus.solma.lasso.algorithm.{FlatnessMappingAlgorithm, LassoAlgorithm, LassoBasicAlgorithm}
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}

import scala.collection.mutable

case class SensorMeasurement(pos: (Long, Double),
                             var slice: IndexedSeq[Int],
                             data: FlinkVector) extends StreamEventWithPos[(Long, Double)]

class LassoWorkerLogic (modelBuilder: ModelBuilder[LassoParam, LassoModel],
                        lassoMethod: LassoAlgorithm[OptionLabeledVector, LassoParam, ((Long, Double), Double),
                          LassoModel], featureCount: Int)
  extends WorkerLogic[LassoStreamEvent, LassoParam, ((Long, Double), Double)]
{
  val unpredictedVecs = new mutable.Queue[LassoStreamEvent]()
  var unlabeledVecs = new mutable.HashMap[Long, (Long, mutable.Queue[StreamEventWithPos[(Long, Double)]])]()
  val labeledVecs = new mutable.Queue[OptionLabeledVector]
  var missingValues: collection.mutable.HashMap[Int, Double] = collection.mutable.HashMap[Int, Double]()
  LassoWorkerLogic.defaultValues.foreach(x => missingValues(x._1) = x._2)
  val maxTTL: Long = 60 * 60 * 1000

  override def onRecv(data: LassoStreamEvent,
                      ps: ParameterServerClient[LassoParam, ((Long, Double), Double)]): Unit = {
    val now = Calendar.getInstance.getTimeInMillis

    data match {
      case Left(v) =>
        val inVec = v.data
        var outVec: FlinkVector = v.data.copy

        (0 until featureCount).foreach{
          x =>
            if (inVec(x) equals Double.NaN){
              outVec(x) = missingValues(x + 1)
            }
            else {
              missingValues(x + 1) = inVec(x)
              outVec(x) = inVec(x)
            }
        }

        if (!unlabeledVecs.keys.exists(x => x == v.pos._1)) {
          unlabeledVecs(v.pos._1) = (now, new mutable.Queue[StreamEventWithPos[(Long, Double)]]())
        }

        val updatedVec = SensorMeasurement(v.pos, v.slice, outVec)

        unlabeledVecs(v.pos._1)._2.enqueue(updatedVec)
        unpredictedVecs.enqueue(Left(updatedVec)/*data*/)
        //FileUtils.writeSimpleLog("Pull-Measurement: (" + v.pos._1 + ", " + v.pos._2 + ")")
        ps.pull(0)
      case Right(v) =>
        if (unlabeledVecs.keys.exists(x => x == v.label)) {
          val dequeueElements = unlabeledVecs(v.label)._2.dequeueAll(x => true).toVector
          unlabeledVecs -= v.label
          val poses = dequeueElements.map(x => x.pos._2)
          var labels = Vector[(Double, Double)]()

          for (i <- 0 until v.labels.data.length) {
            labels = (v.poses(i), v.labels(i)) +: labels
          }

          val interpolatedLabels = new FlatnessMappingAlgorithm(poses, labels).apply

          val processedEvents: Iterable[OptionLabeledVector] = dequeueElements.zipWithIndex.map(
            zipped => {
              val data: BreezeVector[Double] = DenseBreezeVector.fill(76){0.0}
              data(zipped._1.slice.head) = zipped._1.data(zipped._1.slice.head)
              val vec: OptionLabeledVector = Left(((zipped._1.pos, data), interpolatedLabels(zipped._2)))
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

object LassoWorkerLogic {
  val defaultValues: collection.immutable.HashMap[Int, Double] = collection.immutable.HashMap[Int, Double](
    1 -> 3189.888,
    2 -> 2292.732,
    3 -> 2905.604,
    4 -> 2028.754,
    5 -> 1384.5,
    6 -> 690.404,
    7 -> 1559.87,
    8 -> 1078.064,
    9 -> 9322.3,
    10 -> 9722.882,
    11 -> 9119.24,
    12 -> 9501.362,
    13 -> 10114.234,
    14 -> 8901.412,
    15 -> 1.846,
    16 -> 1.846,
    17 -> 1.846,
    18 -> 1.846,
    19 -> 1.846,
    20 -> 1.846,
    21 -> 1.846,
    22 -> 1.846,
    23 -> 1.846,
    24 -> 1.846,
    25 -> 1.846,
    26 -> 1.846,
    27 -> 138.45,
    28 -> 0,
    29 -> 0,
    30 -> 0,
    31 -> 0,
    32 -> 0,
    33 -> 0,
    34 -> 0,
    35 -> 0,
    36 -> 0,
    37 -> 0,
    38 -> 138.45,
    39 -> 55.38,
    40 -> 3.692,
    41 -> 0,
    42 -> 0,
    43 -> 0,
    44 -> 0,
    45 -> 0,
    46 -> 0,
    47 -> 0,
    48 -> 0,
    49 -> 0,
    50 -> 915.616,
    51 -> 9165.39,
    52 -> 184.6,
    53 -> 46.15,
    54 -> 0,
    55 -> 0,
    56 -> 0,
    57 -> 0,
    58 -> 0,
    59 -> 0,
    60 -> 0,
    61 -> 0,
    62 -> 0,
    63 -> 114.452,
    64 -> 40.612,
    65 -> 0,
    66 -> 0,
    67 -> 0,
    68 -> 0,
    69 -> 0,
    70 -> 0,
    71 -> 0,
    72 -> 0,
    73 -> 0,
    74 -> 0,
    75 -> 930.384,
    76 -> 2377.648
  )
}
