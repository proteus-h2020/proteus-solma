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

package eu.proteus.solma.orr.algorithm

import scala.io.{BufferedSource, Source}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import breeze.linalg.{Vector => BreezeVector}
import breeze.numerics.abs
import eu.proteus.solma.orr.ORR
import eu.proteus.solma.orr.ORR.{ORRStreamEvent, ORRModel, UnlabeledVector}
import eu.proteus.solma.orr.algorithm.ORRParameterInitializer.initConcrete


object ORRAlgorithmTest {
  val testsFilePath = "/fried_delve.txt"
  val initA = 1.0 // initialization of a value (RR model)
  val initB = 0.0 // initialization of b value (RR model)
  val featureCount = 10

  def lineToDataPoint(line: String): (UnlabeledVector, Double) = {
    val params = line.split(" ")
    val features = params.slice(0, 10)
    val dataPoint = BreezeVector[Double](features.map(x => x.toDouble))
    val label = params(10).toDouble

    (dataPoint, label)
  }

}


class ORRAlgorithmTest extends FlatSpec with PropertyChecks with Matchers {
  import ORRAlgorithmTest._

  "ORR Algorithm" should "give reasonable error on test data" in {
    val TOTAL_SAMPLES = Source.fromURL(getClass.getResource(testsFilePath)).getLines.size
    val initLambda = 0.0005
    var instance = new ORRAlgorithm(new ORR())
    var model = initConcrete(initA, initB, initLambda, featureCount)(0)
    var sumDifferences = 0.0

    for (line <- Source.fromURL(getClass.getResource(testsFilePath)).getLines) {
      // TRAIN THE MODEL
      val dataPoint = lineToDataPoint(line)

      val prediction = instance.predict(dataPoint._1, model)
      println("Prediction: " + prediction + " Label: " + dataPoint._2)
      sumDifferences += (prediction - dataPoint._2) * (prediction - dataPoint._2)
      
      val newModel = instance.delta(dataPoint._1, model, dataPoint._2)
      model = newModel
    }
    println("Differences average: %.3f".format(sumDifferences / TOTAL_SAMPLES))
  }
}
