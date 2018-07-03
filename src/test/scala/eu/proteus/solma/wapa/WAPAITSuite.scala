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

package eu.proteus.solma.wapa

import breeze.linalg
import eu.proteus.solma.events.StreamEvent
import eu.proteus.solma.wapa.WAPA.UnlabeledVector
import eu.proteus.solma.utils.{BinaryConfusionMatrix, BinaryConfusionMatrixBuilder, FlinkSolmaUtils, FlinkTestBase}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.util.Collector
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

class WAPAITSuite extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with FlinkTestBase {

  import WAPAITSuite._

  val dataSemiShuffled = new mutable.ArrayBuffer[Either[(Long, Sample), (Long, Double)]]()

  val dataNotShuffled = new mutable.ArrayBuffer[Either[(Long, Sample), (Long, Double)]]()

  behavior of "SOLMA Online WAPA"

  override def beforeAll(): Unit = {

    val bufferedSource = Source.fromInputStream(getClass.getResourceAsStream("/nursey.csv"))
    var i = 0L

    val unlabelled = new mutable.HashMap[Long, Sample]()
    val labelled = new mutable.HashMap[Long, Double]()

    for (line <- bufferedSource.getLines) {
      val v = line.split(";").map(_.trim.toDouble)
      if (v.length > 0) {
        unlabelled(i) = Sample(0 to 7, DenseVector(v.take(8)))
        labelled(i) = v(8)
        dataNotShuffled.append(Left((i, unlabelled(i))))
        dataNotShuffled.append(Right((i, labelled(i))))
        i += 1
      }
    }

    var j = 0
    var k = 0

    val max = i
    i *= 2

    // we want to split the data points from their labels
    // invariant: the i-th data point has to appear before than the i-th label

    while (i > 0) {
      val p = rnd.nextDouble()
      if (p > 0.8 && j < max) {
        // take the j-th data point
        if (j >= k) {
          // make sure the j-th data point is after the k-th label
          dataSemiShuffled.append(Left((j, unlabelled(j))))
          j += 1
          i -= 1
        } else {
          // let's take the k-th label
          dataSemiShuffled.append(Right((k, labelled(k))))
          k += 1
          i -= 1
        }
      } else {
        if (k < j) {
          dataSemiShuffled.append(Right((k, labelled(k))))
          k += 1
          i -= 1
        } else {
          dataSemiShuffled.append(Left((j, unlabelled(j))))
          j += 1
          i -= 1
        }
      }
    }

  }

  it should "perform binary WAPA on plain Nursey data" in {
    helper(dataNotShuffled)
  }

  it should "perform binary WAPA on semi shuffled Nursey data" in {
    helper(dataSemiShuffled)
  }


}

object WAPAITSuite {
  case class Sample(
                     var slice: IndexedSeq[Int],
                     data: Vector
                   ) extends StreamEvent with Serializable

  val LOG: slf4j.Logger = LoggerFactory.getLogger(WAPAITSuite.getClass)

  val rnd = new scala.util.Random()


  def helper(data: Seq[Either[(Long, StreamEvent), (Long, Double)]]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val wapa = WAPA()
      .setRhoParam(0.15)
      .setAlgoParam("WAPAI")
      .setCParam(1)
      .setFeaturesCount(8)
      .setPullLimit(10000)
      .setIterationWaitTime(20000)


    val ds: DataStream[Either[(Long, StreamEvent), (Long, Double)]] = env.fromCollection(data)

    val predictionsAndModel = wapa.transform(ds)

    predictionsAndModel.flatMapWith {
      case x =>
        x match {
          case Right(model) =>
            List(model)
          case Left(_) =>
            List()
        }
    }.print

    predictionsAndModel
      .connect(ds.flatMapWith {
        case x =>
          x match {
            case Right(x: (Long, Double)) => List(x)
            case Left(_) => List()
          }
      })
      .flatMap(coFlatMapper = new RichCoFlatMapFunction[Either[(Long, UnlabeledVector, Double),
        (Int, linalg.DenseVector[Double])], (Long, Double), BinaryConfusionMatrix] {

        @transient var cfBuilder: BinaryConfusionMatrixBuilder = _

        @transient var predictions: mutable.HashMap[Long, Double] = _
        @transient var expected: mutable.HashMap[Long, Double] = _

        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          cfBuilder = new BinaryConfusionMatrixBuilder
          predictions = new mutable.HashMap[Long, Double]()
          expected = new mutable.HashMap[Long, Double]()
        }

        override def flatMap1(
                               value: Either[(Long, UnlabeledVector, Double), (Int, linalg.DenseVector[Double])],
                               out: Collector[BinaryConfusionMatrix]
                             ) = {
          value match {
            case Left((index, point, prediction)) => {
              // due to parallel processing, it might happen that an outcome arrives here
              // earlier than the actual prediction
              expected.remove(index) match {
                case Some(expectedPrediction) => {
                  cfBuilder.add(prediction, expectedPrediction)
                  out.collect(cfBuilder.toConfusionMatrix)
                }
                case None => predictions(index) = prediction
              }

            }
            case Right(_) => // do nothing with the last updated model
          }
        }

        override def flatMap2(
                               value: (Long, Double),
                               out: Collector[BinaryConfusionMatrix]): Unit = {

          val (index, expectedPrediction) = value

          predictions.remove(index) match {
            case Some(predictedOutcome) => {
              cfBuilder.add(predictedOutcome, expectedPrediction)
              out.collect(cfBuilder.toConfusionMatrix)
            }
            case None => expected(index) = expectedPrediction
          }


        }
      })
      .setParallelism(1)
      .print.setParallelism(1)


    env.execute()
  }


}
