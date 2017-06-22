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

package eu.proteus.solma.moments

import eu.proteus.annotations.Proteus
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.contrib.streaming.scala.utils._
import org.scalatest.{FlatSpec, Matchers}
import eu.proteus.solma.events.WeightedStreamEvent

@Proteus
class WeightedMomentsEstimatorITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "Flink's Weighted Moments Estimator"

  import WeightedMomentsEstimator._
  import WeightedMomentsEstimatorITSuite._

  it should "estimate the mean and var of a stream" in {

   val result = weightedMeanAndVariance(data,3)
   val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.setMaxParallelism(3)

    val stream = env.fromCollection(data)

    val estimator = WeightedMomentsEstimator()
      .setFeaturesCount(3)
      .enableAggregation(true)
    val it: scala.Iterator[WeightedMoments] = estimator.transform(stream).map(x=>x._2).collect()
    val eps = 1E-5
    while (it.hasNext) {
      val elem = it.next
      if (elem.counters(0) == 9) {
        elem.mean(0) should be (result.weightedmean(0) +- eps)
        elem.mean(1) should be (result.weightedmean(1) +- eps)
        elem.mean(2) should be (result.weightedmean(2) +- eps)
        elem.variance(0) should be (result.weightedvar(0) +- eps)
        elem.variance(1) should be (result.weightedvar(1) +- eps)
        elem.variance(2) should be (result.weightedvar(2) +- eps)
      }
    }
  }

   it should "estimate the mean and var of a stream on a features subset" in {
     val result = weightedMeanAndVariance(data2,3)
     val env = StreamExecutionEnvironment.getExecutionEnvironment
     env.setParallelism(2)
     env.setMaxParallelism(2)

     val stream = env.fromCollection(data2)

     val estimator = WeightedMomentsEstimator()
       .setFeaturesCount(3)
       .enableAggregation(true)

     val it: scala.Iterator[WeightedMoments] = estimator.transform(stream).map(x=>x._2).collect()
     val eps = 1E-5
     while (it.hasNext) {
       val elem = it.next
       if ((elem.counters(0) == 3)&&(elem.counters(2) == 2)) {
          elem.mean(0) should be (result.weightedmean(0) +- eps)
         elem.mean(1) should be (result.weightedmean(1) +- eps)
         elem.mean(2) should be (result.weightedmean(2) +- eps)
        elem.variance(0) should be (result.weightedvar(0) +- eps)
         elem.variance(1) should be (result.weightedvar(1) +- eps)
         if (!java.lang.Double.isNaN(elem.variance(2))) {
           elem.variance(2) should be (result.weightedvar(2) +- eps)
         }
       }
     }
   }

}

case class WeightedSample (
                    s: IndexedSeq[Int],
                    w:DenseVector,
                    d: DenseVector) extends WeightedStreamEvent {
  override val slice: IndexedSeq[Int] = s
  override val weight:Vector=w
  override val data: Vector = d
}
case class weightedMeanAndVariance(
                                    samples:Seq[WeightedSample],
                                    featurecount:Int) {
  private val meaa=samples.fold(WeightedSample(IndexedSeq(),DenseVector.zeros(featurecount),
                                                          DenseVector.zeros(featurecount)))(
                                (i,j)=> { val temp=i.data.asBreeze.copy
                                temp(j.slice):+=j.weight.asBreeze.copy:*j.data.asBreeze.copy
                                val temp2=i.weight.asBreeze.copy
                                temp2(j.slice):+=j.weight.asBreeze.copy
                                WeightedSample(j.slice, DenseVector(temp2.copy.toArray),DenseVector(temp.copy.toArray))
                                })
  val weightedmean=meaa.data.asBreeze:/meaa.weight.asBreeze

  private val varaa=samples.fold(WeightedSample(IndexedSeq(),DenseVector.zeros(featurecount),
                                                           DenseVector.zeros(featurecount)))(
                                                       (i,j)=> {val temp=i.data.asBreeze.copy
                                temp(j.slice):+=j.weight.asBreeze.copy:*(j.data.asBreeze.copy:-weightedmean(j.slice)):*
                                (j.data.asBreeze.copy-weightedmean(j.slice))
                                WeightedSample(j.slice, DenseVector(0),DenseVector(temp.copy.toArray))
                                })

  val weightedvar=varaa.data.asBreeze:/meaa.weight.asBreeze
}

object WeightedMomentsEstimatorITSuite {

  val data: Seq[WeightedSample] = List(
    WeightedSample(0 to 2, DenseVector(1,1,1), DenseVector(Array(2104.00, 3.00, 0.0))),
    WeightedSample(0 to 2, DenseVector(2,2,2), DenseVector(Array(1600.00, 3.00, 0.0))),
    WeightedSample(0 to 2, DenseVector(4,4,4), DenseVector(Array(2400.00, 3.00, 0.0)))
  )

  val data2: Seq[WeightedSample] = List(
    WeightedSample(0 to 1, DenseVector(1,1), DenseVector(Array(2104.00, 3.00))),
    WeightedSample(0 to 1, DenseVector(1,1), DenseVector(Array(1600.00, 3.00))),
    WeightedSample(0 to 1, DenseVector(1,1), DenseVector(Array(2400.00, 3.00))),
    WeightedSample(2 to 2, DenseVector(1), DenseVector(Array(1.00))),
    WeightedSample(2 to 2, DenseVector(1), DenseVector(Array(0.00)))
  )
}
