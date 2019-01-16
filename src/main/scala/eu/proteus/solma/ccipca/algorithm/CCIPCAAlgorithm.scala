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

import scala.math.min
import scala.math.pow
import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.generic.UFunc
import org.apache.flink.ml.math.Breeze._
import eu.proteus.annotations.Proteus
import eu.proteus.solma.ccipca.CCIPCA
import eu.proteus.solma.ccipca.CCIPCA.{CCIPCAModel}
import breeze.stats.distributions._
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.distribution.ChiSquaredDistribution

@Proteus class CCIPCAAlgorithm(instance: CCIPCA) extends BaseCCIPCAAlgorithm[DenseMatrix[Double], CCIPCAModel] {

  override def compute(
      data: DenseMatrix[Double],
      model: CCIPCAModel
  ): (DenseMatrix[Double], DenseVector[Double]) = {

    val dim:Int = 11
    val l:Double = 2.5
    val v:DenseMatrix[Double] = data(0 until dim,::).t
    val a:Int = v.cols
    val b:Int = v.rows
    val u: DenseMatrix[Double] = DenseMatrix.horzcat(v,DenseMatrix.zeros[Double](b,1))

    for (t <- 0 until data.rows) {
      u(::, 0) := data(t, ::).t
      for (i <- 0 until min(dim, t)) {
        if (i == t) {
          v(::, i) := u(::, i)
        } else {
          val w1 = (t - 1 - l)/t
          val w2 = (1 + l)/t
          v(::,i) := w1*v(::,i) + w2*u(::,i)*u(::,i).t*v(::,i)/sqrt(v(::,i).t*v(::,i))
          u(::,(i + 1)) := u(::,i) - (u(::,i).t*v(::,i)/sqrt(v(::,i).t*v(::,i)))*(v(::,i)/sqrt(v(::,i).t*v(::,i)))
        }
      }
    }
    // Output eigen values and eigen vectors
    var values = DenseVector.zeros[Double](dim)
    var vectors = DenseMatrix.zeros[Double](v.rows, dim)
    for (i <- 0 until v.cols) {
      values(i) = sqrt(v(::,i).t*v(::,i))
      vectors(::,i) := v(::,i)/sqrt(v(::,i).t*v(::,i))
    }

    (vectors, values)
  }

  def q(
      x_t: DenseMatrix[Double],
      obs: DenseVector[Double]
    ): Double = {

    val b: Int = x_t.rows

    val e: DenseVector[Double] = (DenseMatrix.eye[Double](b) - (x_t * x_t.t)) * obs

    e.t * e
  }


  def t2(
    x_t: DenseMatrix[Double],
    lamda: DenseVector[Double],
    obs: DenseVector[Double]
    ): Double = {

    val samples = lamda.toArray.sorted.reverse
    val dv = new DenseVector[Double](samples)
    val eigVal:DenseMatrix[Double] = diag(dv) 

    obs.t * x_t * inv(eigVal) * x_t.t * obs

  }
  
  def qlimit(
    lamda: DenseVector[Double]
    ): Double = {

    val T1:Double = sum(lamda)

    val lam2: DenseVector[Double] = lamda *:* lamda

    val lam3: DenseVector[Double] = (lamda *:* lamda) *:* lamda

    val T2:Double  = sum(lam2)

    val T3:Double  = sum(lam3)

    val ho:Double = 1.0 - (2.0 * T1 * T3) / (3.0 * T2 * T2)

    val standardNormal = new NormalDistribution(0, 1)

    val infNorm = standardNormal.inverseCumulativeProbability(0.98).toDouble

    val term1:Double = sqrt(ho * infNorm * 2.0 * T2) / T1

    val term2:Double = (T2 * ho * (ho - 1.0)) / (T1 *T1)

    val p:Double = 1.0 / ho

    val ans:Double = T1 * (term1 + 1.0 + term2)

    pow(ans,p) 
   
  }

  def tlimit(
    lamda:DenseVector[Double]
  ): Double = {

  val l = lamda.length 

  val chi = new ChiSquaredDistribution(l,0.98)

  chi.inverseCumulativeProbability(0.98).toDouble

  }

}
