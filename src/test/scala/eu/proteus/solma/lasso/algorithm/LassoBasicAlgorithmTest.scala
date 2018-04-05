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

package eu.proteus.solma.lasso.algorithm

import scala.io.{BufferedSource, Source}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import breeze.linalg.{Vector => BreezeVector}
import breeze.numerics.abs
import eu.proteus.solma.lasso.Lasso.OptionLabeledVector
import eu.proteus.solma.lasso.algorithm.LassoParameterInitializer.initConcrete


object LassoBasicAlgorithmTest {
  val testsFilePath = "/tests.csv"
  val initA = 1.0 // initialization of a value (Lasso model)
  val initB = 0.0 // initialization of b value (Lasso model)
  val featureCount = 76

  def lineToDataPoint(line: String): OptionLabeledVector = {
    val params = line.split(",")
    val coilId = params(0).toLong
    val x = params(1).toDouble
    val label = params(2).toDouble
    val features = params.slice(3, params.length)
    val vector = BreezeVector[Double](features.map(x => x.toDouble))

    Left((((coilId, x), vector), label))
  }
}

class LassoBasicAlgorithmTest extends FlatSpec with PropertyChecks with Matchers {
  import LassoBasicAlgorithmTest._

  "Lasso Basic Algorithm" should "give reasonable error on test data" in {
    val TOTAL_SAMPLES = 478
    val TRAIN_SAMPLES = 400
    val PREDICT_SAMPLES = 78
    val initGamma = 20.0
    var processedSamples = 0
    var basicAlgorithm = LassoBasicAlgorithm.buildLasso()
    var model = initConcrete(initA, initB, initGamma, featureCount)(0)
    var sumDifferences = 0.0

    for (line <- Source.fromURL(getClass.getResource(testsFilePath)).getLines) {
      if (processedSamples < TRAIN_SAMPLES) {
        // TRAIN THE MODEL
        val dataPoint = lineToDataPoint(line)
        val newModel = basicAlgorithm.delta(dataPoint, model, dataPoint.left.get._2)
        model = newModel.head._2
      }
      else {
        val dataPoint = lineToDataPoint(line)
        val prediction = basicAlgorithm.predict(dataPoint, model)
        println("Prediccion: " + prediction._2 + " Label: " + dataPoint.left.get._2)
        sumDifferences += abs(prediction._2 - dataPoint.left.get._2)
      }
      processedSamples += 1
    }

    println("Differences average: " + (sumDifferences / PREDICT_SAMPLES))
  }
}
