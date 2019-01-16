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

package eu.proteus.solma.ccipca.algorithm

import scala.io.{BufferedSource, Source}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import breeze.linalg.{Vector => BreezeVector}
import breeze.numerics.abs
import eu.proteus.solma.ccipca.CCIPCA
import eu.proteus.solma.ccipca.CCIPCA.CCIPCAModel
import eu.proteus.solma.ccipca.algorithm.CCIPCAParameterInitializer.initConcrete

import java.io._
import breeze.linalg._


object CCIPCAAlgorithmTest {
  val testsFilePath = "/matrix.csv"
  val dim = 10
  val n = 5

//  def lineToDataPoint(line: String): UnlabeledVector = {
//    val params = line.split(" ")
//    val features = params.slice(0, dim)
//    val dataPoint = BreezeVector[Double](features.map(x => x.toDouble))
//    dataPoint
//  }
}


class CCIPCAAlgorithmTest extends FlatSpec with PropertyChecks with Matchers {
  import CCIPCAAlgorithmTest._

  "CCIPCA Algorithm" should "give reasonable error on test data" in {
    var instance = new CCIPCAAlgorithm(new CCIPCA())
    var model = initConcrete(dim, n)(0)
    val matrix = csvread(new File("./src/test/resources/fried_delve.txt"), separator=' ')
    val v = instance.compute(matrix, model)
    println(v)
    // println(v.rows, v.cols)
  }
}
