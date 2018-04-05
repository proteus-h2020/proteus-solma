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

import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.math.Vector
import breeze.linalg.{Vector => BreezeVector}
import org.apache.flink.ml.math.Breeze._
import eu.proteus.solma.events.{StreamEventLabel, StreamEventWithPos}
import eu.proteus.solma.lasso.Lasso.{LassoModel, LassoParam, OptionLabeledVector}
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import eu.proteus.solma.lasso.algorithm.LassoBasicAlgorithm
import eu.proteus.solma.lasso.algorithm.LassoParameterInitializer.initConcrete
import eu.proteus.solma.utils.FlinkTestBase
import eu.proteus.solma.utils.FlinkTestUtils.{SuccessException, executeWithSuccessCheck}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.LoggerFactory


object LassoDelayedFeedbackITSuite {
  val log = LoggerFactory.getLogger(classOf[LassoDelayedFeedbackITSuite])
  val workerParallelism = 1
  val psParallelism = 1
  val pullLimit = 10000
  val rangePartitioning = true
  val iterationWaitTime: Long = 20000
  val featureCount = 76
  val initA = 1.0 // initialization of a value (Lasso model)
  val initB = 0.0 // initialization of b value (Lasso model)
  val gamma = 1.0 // initialization of gamma value
  val maxAllowedAvgDistance = 10.0
  val allowedLateness = 10 //mins

  val featuresFilePath = "resources/features.csv"
  val flatnessFilePath = "resources/flatness.csv"
  val testsFilePath = "resources/tests.csv"

  case class SensorMeasurement(pos: (Long, Double),
                               var slice: IndexedSeq[Int],
                               data: Vector) extends StreamEventWithPos[(Long, Double)]

  case class FlatnessMeasurement(poses: List[Double],
                                 label: Long,
                                 labels: DenseVector,
                                 var slice: IndexedSeq[Int],
                                 data: Vector) extends StreamEventLabel[Long, Double]

  def transformToStreamEvents(line: Array[String]): StreamEventWithPos[(Long, Double)] = {
    val event: StreamEventWithPos[(Long, Double)] = line.length match {
      case 78 =>
        val coilID: Long = line(0).toLong
        val xCoord: Double = line(1).toDouble
        val vector: DenseVector = new DenseVector(line.slice(2, line.length).map(x => x.toDouble))
        val ev: StreamEventWithPos[(Long, Double)] = SensorMeasurement((coilID, xCoord), 0 to 76, vector)
        ev
      case _ => null //TODO Exception
    }
    event
  }

  def transformToOptionLabeledVector(line: Array[String]): OptionLabeledVector = {
    val labelVector: OptionLabeledVector = line.length match {
      case 79 =>
        val label: Double = line(2).toDouble
        val vector: BreezeVector[Double] = BreezeVector[Double](line.slice(3, line.length).map(x => x.toDouble))
        val v: OptionLabeledVector = Left((((0, 0.0), vector), label))
        v
      case _ => null //TODO Exception
    }
    labelVector
  }

  def trainingData: Traversable[OptionLabeledVector] = {
    val testLines = io.Source.fromFile(testsFilePath).getLines
    testLines.map(x => transformToOptionLabeledVector(x.split(","))).toTraversable
  }

}

class LassoDelayedFeedbackITSuite extends FunSuite with Matchers with FlinkTestBase{
  import LassoDelayedFeedbackITSuite._

  test("Basic test") {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val env2: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val features: DataSet[Array[String]] = env2.readTextFile(featuresFilePath).map(x =>
      x.split(",")).setParallelism(1)
    val flatness: DataSet[Array[String]] = env2.readTextFile(flatnessFilePath).map(x =>
      x.split(",")).setParallelism(1)

    val joinFlatness: DataSet[StreamEventLabel[Long, Double]] = flatness.groupBy(x => x(0)).reduceGroup {
      (in: Iterator[Array[String]], out: Collector[Option[StreamEventLabel[Long, Double]]]) =>
        val iter = in.toList
        if (iter.nonEmpty) {
          val poses: List[Double] = iter.map(x => x(1).toDouble)
          val labels: DenseVector = new DenseVector(iter.map(x => x(2).toDouble).toArray)
          val flat: LassoDelayedFeedbackITSuite.FlatnessMeasurement =
            FlatnessMeasurement(poses, iter.toArray.head(0).toLong, labels, null, null)
          val ev: StreamEventLabel[Long, Double] = flat
          out.collect(Some(ev))
        }
        else {
          out.collect(None)
        }
    }.filter(x => x.nonEmpty).map(x => x.get).sortPartition(x => x.label, Order.ASCENDING).setParallelism(1)

    val processedFlatness: Seq[LassoStreamEvent] = joinFlatness.map(x => Right(x)).collect()

    val processedFeatures: Seq[LassoStreamEvent] = features.map(x =>
      Left(LassoDelayedFeedbackITSuite.transformToStreamEvents(x))).collect()

    val allEvents: DataStream[LassoStreamEvent] = env.fromCollection(processedFeatures ++ processedFlatness)

    val lasso = new LassoDelayedFeedbacks

    implicit def transformStreamImplementation[T <: LassoStreamEvent] = {
      new LassoDFStreamTransformOperation[T](workerParallelism, psParallelism, pullLimit, featureCount,
        rangePartitioning, iterationWaitTime, allowedLateness)
    }

    val output = lasso.transform[LassoStreamEvent, Either[((Long, Double), Double), (Int, LassoParam)] ](allEvents,
      ParameterMap.Empty)

    output.addSink(new RichSinkFunction[Either[((Long, Double), Double), (Int, LassoParam)]] {

      val modelBuilder = new LassoModelBuilder(initConcrete(initA,
        initB, gamma, featureCount)(0))
      var model: Option[LassoModel] = None

      override def invoke(value: Either[((Long, Double), Double), (Int, LassoParam)]): Unit = {
        value match {
          case Right((id, modelValue)) =>
            model = Some(modelBuilder.add(id, modelValue))
          case Left(label) =>
          // prediction channel is deaf
        }
      }

      override def close(): Unit = {
        if (model.nonEmpty) {
          val distance = LassoBasicModelEvaluation.accuracy(model.get,
            trainingData.map { case Left((vec, lab)) => (vec._2, Some(lab)) }, featureCount,
            LassoBasicAlgorithm.buildLasso())
          throw SuccessException(distance)
        }
      }
    })

    executeWithSuccessCheck[Double](env) {
      distance =>
        println(distance)
        if (distance > LassoDelayedFeedbackITSuite.maxAllowedAvgDistance) {
          fail(s"Got average distance: $distance, expected lower than " +
            s"$LassoDelayedFeedbackITSuite.maxAllowedAvgDistance." +
            s" Note that the result highly depends on environment due to the asynchronous updates.")
        }
    }

  }

}
