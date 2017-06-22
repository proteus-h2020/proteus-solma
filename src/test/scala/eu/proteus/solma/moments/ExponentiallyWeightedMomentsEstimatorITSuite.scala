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

import breeze.linalg
import eu.proteus.annotations.Proteus
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.contrib.streaming.scala.utils._
import org.scalatest.{FlatSpec, Matchers}


@Proteus
class ExponentiallyWeightedMomentsEstimatorITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "Flink's Exponentially Weighted Moments Estimator"

  import ExponentiallyWeightedMomentsEstimator._
  import MomentsEstimatorITSuite._

  it should "estimate the mean and var of a stream" in {

    val nb_features=data.head.data.size
    val alpha:Double=0.8
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setMaxParallelism(2)

    val stream = env.fromCollection(data)

    val estimator = ExponentiallyWeightedMomentsEstimator()
                    .setFeaturesCount(nb_features)
                    .setFeaturesAlpha(alpha)
    estimator.transform(stream).map(x=>x._2.toString).print()
    env.execute("Testing Exponentially Weighted Moments")


  }
}


