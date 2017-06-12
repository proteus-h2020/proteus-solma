
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
import eu.proteus.annotations.Proteus
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.contrib.streaming.scala.utils._
import org.scalatest.{FlatSpec, Matchers}


@Proteus
class MovingMomentsEstimatorITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "Flink's Exponentially Weighted Moments Estimator"

  import MovingMomentsEstimator._
  import MomentsEstimatorITSuite._

  it should "estimate the moving mean and var of a stream" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.setMaxParallelism(3)

    val stream = env.fromCollection(data)

    val estimator = MovingMomentsEstimator()
      .enableAggregation(false)
    .setWindowLength(5)
    .setWindowTriggerInterval(1)
    estimator.transform(stream).print()
    env.execute("Testing Moving Moments")

  }
}



