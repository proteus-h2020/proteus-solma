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

package eu.proteus.solma.ad

import eu.proteus.annotations.Proteus
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}
import org.apache.flink.contrib.streaming.scala.utils._
import breeze.stats._
import breeze.stats.distributions._
import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.generic.UFunc

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

import eu.proteus.solma.ccipca.CCIPCA
import eu.proteus.solma.ccipca.CCIPCA.CCIPCAModel
import eu.proteus.solma.ccipca.algorithm.CCIPCAAlgorithm
import eu.proteus.solma.ccipca.algorithm.CCIPCAParameterInitializer.initConcrete


@Proteus
class AnomalyDetectionITSuite
    extends FlatSpec
    with Matchers
    with FlinkTestBase {

  import AnomalyDetection._
  import CCIPCA._

  behavior of "PCA's Anamaly Detection"

  it should "perform PCA anomaly detection" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setMaxParallelism(1)

    val ell = 200 
    val num_comp = 30

    // Frequent Directions
    var sketch = DenseMatrix.zeros[Double](ell, num_comp)
    for (idx <- 0 until 4) { // 4 workers
      var instance = AnomalyDetection()
      var model = AnomalyDetection.init(ell, num_comp)
      for (vector <- AnomalyDetectionITSuite.trainingData.slice(idx*70000, (idx+1)*70000+1)) {
        model = instance.append(vector, model)
      }
      if (idx == 0) {
        sketch = instance.get(model)
      } else {
        sketch = DenseMatrix.vertcat(sketch, instance.get(model))
      }
    }

    // CCIPCA function
    val ccipca = new CCIPCAAlgorithm(new CCIPCA())
    val pca_model = initConcrete(ell, num_comp)(0)
    val v = ccipca.compute(sketch, pca_model)
    val (eigenvectors, eigenvalues) = (v._1, v._2)

    // T squared and Q stats 
    // val obs = DenseVector.ones[Double](num_comp)
    // val q = ccipca.q(eigenvectors, obs)
    // println("Q:", q)
    // val t2 = ccipca.t2(eigenvectors,eigenvalues,obs)
    // println("T2:", t2)
    val tlim = ccipca.tlimit(eigenvalues)
    // println("T Limit:", tlim)
    val qlim = ccipca.qlimit(eigenvalues)
    // println("Q Limit:", qlim)

    for (obs <- AnomalyDetectionITSuite.testData) {
      val q = ccipca.q(eigenvectors, obs)
      // println("Abnormal", "Q", q, "Q Limit", qlim)
      val t2 = ccipca.t2(eigenvectors,eigenvalues,obs)
      println("Abnormal", "Q", q, "Q Limit", qlim)
    }

  }

}

object AnomalyDetectionITSuite {

  def toVector(line: String): DenseVector[Double] = {
    val features = line.split(",").dropRight(1)
    val dataPoint = DenseVector(features.map(x => x.toDouble))
    dataPoint
  }

  val trainingFilePath = "/creditcard_train.csv"
  val testFilePath = "/creditcard_test.csv"

  val trainingData: Seq[DenseVector[Double]] = (for (
    line <- Source.fromURL(getClass.getResource(trainingFilePath)).getLines()) yield toVector(line)).toList

  val testData: Seq[DenseVector[Double]] = (for (
    line <- Source.fromURL(getClass.getResource(testFilePath)).getLines()) yield toVector(line)).toList

}
