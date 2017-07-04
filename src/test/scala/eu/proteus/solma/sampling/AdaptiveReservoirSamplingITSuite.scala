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
class AdaptiveReservoirSamplingITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase  {

  behavior of "Flink's Adaptive Reservoir Sampling"

  it should "perform adaptive reservoir sampling" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
   env.setParallelism(1)
    env.setMaxParallelism(1)
    val stream = env.fromCollection(AdaptiveReservoirSamplingITSuite.data)
    val transformer = AdaptiveReservoirSampling()
      .setAdaptiveReservoirSize(4).setAdaptiveReservoirNewSize(2)

    transformer.transform(stream).print()
    env.execute("adaptive reservoir sampling")

  }

}
object AdaptiveReservoirSamplingITSuite
{


  val data: Seq[Vector] = List(
    DenseVector(Array(1.0, 0.3, 5.0)),
    DenseVector(Array(1.2, 4.2, 5.2)),
    DenseVector(Array(2.0, 3.2, 5.2)),
    DenseVector(Array(1.2, 3.5, 7.6)),
    DenseVector(Array(1.1, 8.8, 5.2)),
    DenseVector(Array(5.2, 3.1, 5.6)),
    DenseVector(Array(8.2, 3.1, 5.5)),
    DenseVector(Array(2.0, 0.3, 5.0)),
    DenseVector(Array(2.2, 4.2, 5.2)),
    DenseVector(Array(3.0, 3.2, 5.2)),
    DenseVector(Array(3.2, 3.5, 7.6)),
    DenseVector(Array(4.1, 8.8, 5.2)),
    DenseVector(Array(4.3, 7.2, 5.2)),
    DenseVector(Array(4.2, 3.1, 5.6)),
    DenseVector(Array(5.2, 3.1, 5.5))
  )
}
