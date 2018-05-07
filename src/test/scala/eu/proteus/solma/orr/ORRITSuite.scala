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

package eu.proteus.solma.orr

import eu.proteus.solma.events.StreamEvent
import breeze.linalg.VectorBuilder
import org.apache.flink.ml.math.{DenseVector, Vector}
import breeze.linalg.{DenseVector => BreezeVector}
import eu.proteus.solma.utils.{FlinkSolmaUtils, FlinkTestBase}
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class ORRITSuite extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "SOLMA Online RR"

  import ORRITSuite.Sample

  it should "perform binary ORR" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val orr = new ORR()

    orr.setFeaturesCount(3)
    orr.setPullLimit(10000)
    orr.setIterationWaitTime(20000)

    val ds: DataStream[Either[(Long, StreamEvent), (Long, Double)]] = env.fromCollection(ORRITSuite.data)

    orr.transform(ds).print()

    env.execute()
  }

}

object ORRITSuite {
   case class Sample(
      var slice: IndexedSeq[Int],
      data: Vector
  ) extends StreamEvent with Serializable


  val rnd = new scala.util.Random()

  private def randomVector: BreezeVector[Double] = {
    val vectorBuilder = new VectorBuilder[Double](length = 3)
    vectorBuilder.toDenseVector
  }

  val data: Seq[Either[(Long, Sample), (Long, Double)]] = List(

    Left(1, Sample(0 to 2, DenseVector(randomVector.toArray))),
    Left(1, Sample(0 to 2, DenseVector(randomVector.toArray))),
    Left(1, Sample(0 to 2, DenseVector(randomVector.toArray))),
    Left(1, Sample(0 to 2, DenseVector(randomVector.toArray))),
    Right((1, rnd.nextDouble())),
    Right((2, rnd.nextDouble())),
    Right((3, rnd.nextDouble())),
    Right((4, rnd.nextDouble()))
    
  )
}
