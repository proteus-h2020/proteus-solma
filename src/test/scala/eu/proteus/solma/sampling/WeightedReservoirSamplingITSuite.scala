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
package eu.proteus.solma.sampling



import eu.proteus.annotations.Proteus
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}

@Proteus
class WeightedReservoirSamplingITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase  {

  behavior of "Flink's Weighted Reservoir Sampling"

  it should "perform weighted reservoir sampling" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setMaxParallelism(1)

    val stream = env.fromCollection(WeightedReservoirSamplingITSuite.data)

    val transformer = WeightedReservoirSampling()
      .setWeightedReservoirSize(4)

    transformer.transform(stream).print()

    env.execute("weighted reservoir sampling")
  }

}
object WeightedReservoirSamplingITSuite{

  val data: Seq[Vector] = List(
    DenseVector( Array(0.2,1.0, 0.3, 5.0)),
    DenseVector(Array(0.1,1.2, 4.2, 5.2)),
    DenseVector(Array(0.3,2.0, 3.2, 5.2)),
    DenseVector(Array(0.5,1.2, 3.5, 7.6)),
    DenseVector(Array(0.8,1.1, 8.8, 5.2)),
    DenseVector(Array(0.8,1.3, 7.2, 5.2)),
    DenseVector(Array(0.4,5.2, 3.1, 5.6)),
    DenseVector(Array(0.5,8.2, 3.1, 5.5)))

}
