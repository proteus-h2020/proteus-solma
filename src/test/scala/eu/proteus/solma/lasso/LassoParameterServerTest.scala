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

import breeze.linalg.{DenseVector, SparseVector, VectorBuilder}
import eu.proteus.solma.lasso.Lasso.{LassoParam, OptionLabeledVector, UnlabeledVector}
import eu.proteus.solma.lasso.algorithm.LassoBasicAlgorithm
import eu.proteus.solma.lasso.algorithm.LassoParameterInitializer.initConcrete
import eu.proteus.solma.utils.FlinkTestUtils.{SuccessException, executeWithSuccessCheck}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks

import scala.util.Random


object LassoParameterServerTest {
  val featureCount = 500
  val spareFeatureCount = 100
  val numberOfTraining = 800
  val numberOfTest = 20
  val random = new Random(100L)

  private def randomVector = {
    val vectorBuilder = new VectorBuilder[Double](length = featureCount)
    0 to spareFeatureCount foreach { i =>
      vectorBuilder.add(random.nextInt(featureCount), random.nextDouble())
    }
    vectorBuilder.toDenseVector
  }

  val trainingData: Seq[OptionLabeledVector] =  Seq.fill(numberOfTraining)(
    Left((randomVector, random.nextDouble()))
  )

  val testData: Seq[(UnlabeledVector, Double)] =  Seq.fill(numberOfTest)(
    (randomVector, random.nextDouble())
  )

}

class LassoParameterServerTest extends FlatSpec with PropertyChecks with Matchers {

  import LassoParameterServerTest._

  "Lasso with PS" should "give reasonable error on test data" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /*DataStream[OptionLabeledVector[Double]]*/

    val src: DataStream[OptionLabeledVector] = env.fromCollection(trainingData)

    LassoParameterServer.transformLasso(None)(src, workerParallelism = 3,
      psParallelism = 3, lassoMethod = LassoBasicAlgorithm.buildLasso(), pullLimit = 10000,
      featureCount = LassoParameterServerTest.featureCount, rangePartitioning = true, iterationWaitTime = 20000
    ).addSink(new RichSinkFunction[Either[Double, (Int, LassoParam)]] {
      //val modelBuilder = new VectorBuilder[Double](length = featureCount)

      val modelBuilder = new LassoModelBuilder(initConcrete(1.0, 0.0, featureCount)(0))

      override def invoke(value: Either[Double, (Int, LassoParam)]): Unit = {
        value match {
          case Right((id, modelValue)) =>
            modelBuilder.add(id, modelValue)
          case Left(label) =>
          // prediction channel is deaf
        }
      }

      override def close(): Unit = {
        val model = modelBuilder.baseModel
        // compute percent
        //        Note: It would be better if the testData was used here but the random data does not fit to evaluation
        //        the algorithm
        //        The part of the training dataset is used here to test the model
        //        val percent = ModelEvaluation.processModel(model, testData, featureCount,
        val distance = LassoBasicModelEvaluation.accuracy(model,
          trainingData.take(20).map { case Left((vec, lab)) => (vec.toDenseVector, Some(lab)) },
          featureCount,
          LassoBasicAlgorithm.buildLasso())
        throw SuccessException(distance)
      }


    }).setParallelism(1)

    val maxAllowedAvgDistance = 1.0

    executeWithSuccessCheck[Double](env) {
      distance =>
        println(distance)
        if (distance > maxAllowedAvgDistance) {
          fail(s"Got average distance: $distance, expected lower than $maxAllowedAvgDistance." +
            s" Note that the result highly depends on environment due to the asynchronous updates.")
        }
    }
  }


}

