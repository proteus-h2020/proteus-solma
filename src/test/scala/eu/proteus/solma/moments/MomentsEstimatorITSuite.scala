/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import breeze.linalg
import breeze.linalg.*
import eu.proteus.annotations.Proteus
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.contrib.streaming.scala.utils._
import org.scalatest.{FlatSpec, Matchers}
import breeze.stats.meanAndVariance
import eu.proteus.solma.events.StreamEvent

@Proteus
class MomentsEstimatorITSuite
    extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "Flink's Moments Estimator"

  import MomentsEstimator._
  import MomentsEstimatorITSuite._

  it should "estimate the mean and var of a stream" in {

    val m = linalg.DenseMatrix.zeros[Double](data.length, data.head.data.size)
    for (i <- data.indices) {
      m(i, ::) := data(i).data.asBreeze.t
    }
    val result = meanAndVariance(m(::, *))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val stream = env.fromCollection(data)

    val estimator = MomentsEstimator()
      .setFeaturesCount(3)
      .enableAggregation(true)

    val it: scala.Iterator[Moments] = estimator.transform(stream).collect()

    val eps = 1e06
    while (it.hasNext) {
      val elem = it.next
      if (elem.counter(0) == 47.0) {
        elem.mean(0) should be (result(0).mean +- eps)
        elem.mean(1) should be (result(1).mean +- eps)
        elem.mean(2) should be (result(2).mean +- eps)
        elem.variance(0) should be (result(0).variance +- eps)
        elem.variance(1) should be (result(1).variance +- eps)
        elem.variance(2) should be (result(2).variance +- eps)
      }
    }
  }

  it should "estimate the mean and var of a stream on a features subset" in {

    val m = linalg.DenseMatrix.zeros[Double](data.length, data.head.data.size)
    for (i <- data.indices) {
      m(i, ::) := data(i).data.asBreeze.t
    }
    val result = meanAndVariance(m(::, *))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val stream = env.fromCollection(data2)

    val estimator = MomentsEstimator()
      .setFeaturesCount(2)
      .enableAggregation(true)

    val it: scala.Iterator[Moments] = estimator.transform(stream).collect()

    val eps = 1e06
    while (it.hasNext) {
      val elem = it.next
      if (elem.counter(0) == 47.0) {
        elem.mean(0) should be (result(0).mean +- eps)
        elem.mean(1) should be (result(1).mean +- eps)
//        elem.mean(2) should be (result(2).mean +- eps)
        elem.variance(0) should be (result(0).variance +- eps)
        elem.variance(1) should be (result(1).variance +- eps)
//        elem.variance(2) should be (result(2).variance +- eps)
      }
    }
  }

}

object MomentsEstimatorITSuite {
  val data: Seq[StreamEvent[Vector]] = List(
    StreamEvent(0 to 2, DenseVector(Array(2104.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1600.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2400.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1416.00, 2.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(3000.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1985.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1534.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1427.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1380.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1494.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1940.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2000.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1890.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(4478.00, 5.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1268.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2300.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1320.00, 2.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1236.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2609.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(3031.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1767.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1888.00, 2.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1604.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1962.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(3890.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1100.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1458.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2526.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2200.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2637.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1839.00, 2.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1000.00, 1.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2040.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(3137.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1811.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1437.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1239.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2132.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(4215.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2162.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1664.00, 2.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2238.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(2567.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1200.00, 3.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(852.00, 2.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1852.00, 4.00, 0.0))),
    StreamEvent(0 to 2, DenseVector(Array(1203.00, 3.00, 0.0)))
  )

  val data2: Seq[StreamEvent[Vector]] = List(
    StreamEvent(0 to 1, DenseVector(Array(2104.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1600.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(2400.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1416.00, 2.00))),
    StreamEvent(0 to 1, DenseVector(Array(3000.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(1985.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(1534.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1427.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1380.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1494.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1940.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(2000.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1890.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(4478.00, 5.00))),
    StreamEvent(0 to 1, DenseVector(Array(1268.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(2300.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(1320.00, 2.00))),
    StreamEvent(0 to 1, DenseVector(Array(1236.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(2609.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(3031.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(1767.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1888.00, 2.00))),
    StreamEvent(0 to 1, DenseVector(Array(1604.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1962.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(3890.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1100.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1458.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(2526.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(2200.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(2637.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1839.00, 2.00))),
    StreamEvent(0 to 1, DenseVector(Array(1000.00, 1.00))),
    StreamEvent(0 to 1, DenseVector(Array(2040.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(3137.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1811.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(1437.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(1239.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(2132.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(4215.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(2162.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(1664.00, 2.00))),
    StreamEvent(0 to 1, DenseVector(Array(2238.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(2567.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(1200.00, 3.00))),
    StreamEvent(0 to 1, DenseVector(Array(852.00, 2.00))),
    StreamEvent(0 to 1, DenseVector(Array(1852.00, 4.00))),
    StreamEvent(0 to 1, DenseVector(Array(1203.00, 3.00)))
  )
}
