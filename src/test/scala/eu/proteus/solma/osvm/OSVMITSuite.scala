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

package eu.proteus.solma.osvm

import eu.proteus.solma.events.StreamEvent
import eu.proteus.solma.utils.{FlinkSolmaUtils, FlinkTestBase}
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class OSVMITSuite extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "SOLMA Online SVM"

  import OSVMITSuite.Sample


  it should "perform binary OSVM" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)


//    val mixed = new Array[Either[(Long, Sample), (Long, Double)]](20)
//
//    for (i <- 0l until 10l) {
//      mixed(i.toInt) = Left((i, Sample(0 to 2, DenseVector(Array.fill(3)(rnd.nextDouble)))))
//    }
//
//    for (i <- 10l until 20l) {
//      mixed(i.toInt) = Right((i, rnd.nextDouble() * 2.0 - 1.0))
//    }

    val osvm = OSVM()

    osvm.setFeaturesCount(3)
    osvm.setPullLimit(10000)
    osvm.setIterationWaitTime(20000)

    val ds: DataStream[Either[(Long, StreamEvent), (Long, Double)]] = env.fromCollection(OSVMITSuite.data)

    osvm.transform(ds).print()


    env.execute()
  }

}

object OSVMITSuite {
   case class Sample(
      var slice: IndexedSeq[Int],
      data: Vector
  ) extends StreamEvent with Serializable


  val rnd = new scala.util.Random()

  val data: Seq[Either[(Long, Sample), (Long, Double)]] = List(

    Left(1, Sample(0 to 2, DenseVector(rnd.nextDouble(), rnd.nextDouble(), rnd.nextDouble()))),
    Left(2, Sample(0 to 2, DenseVector(rnd.nextDouble(), rnd.nextDouble(), rnd.nextDouble()))),
    Left(3, Sample(0 to 2, DenseVector(rnd.nextDouble(), rnd.nextDouble(), rnd.nextDouble()))),
    Left(4, Sample(0 to 2, DenseVector(rnd.nextDouble(), rnd.nextDouble(), rnd.nextDouble()))),
    Right((1, rnd.nextDouble())),
    Right((2, rnd.nextDouble())),
    Right((3, rnd.nextDouble())),
    Right((4, rnd.nextDouble()))


  )
}
