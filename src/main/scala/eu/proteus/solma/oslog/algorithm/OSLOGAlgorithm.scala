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

package eu.proteus.solma.oslog.algorithm

import breeze.linalg._
import breeze.numerics.{abs, sqrt}
import org.apache.flink.ml.math.Breeze._

import breeze.linalg.DenseVector
import eu.proteus.annotations.Proteus
import eu.proteus.solma.oslog.OSLOG
import eu.proteus.solma.oslog.OSLOG.{OSLOGModel, UnlabeledVector}


@Proteus
class OSLOGAlgorithm(instance: OSLOG) extends BaseOSLOGAlgorithm[UnlabeledVector, Double, OSLOGModel]  {

  override def delta(
      dataPoint: UnlabeledVector,
      model: OSLOGModel,
      label: Double
  ): (DenseMatrix[Double], DenseVector[Double], DenseVector[Double], Double) = {

    val lambda =  model._4

    val x_t = dataPoint.toDenseVector

    val a_t: DenseMatrix[Double] = x_t * x_t.t

    val A_t: DenseMatrix[Double] = model._1 + a_t

    val D_t: DenseMatrix[Double] = (diag(abs(model._3)))

    val Add1: DenseMatrix[Double] = lambda * diag(DenseVector.ones[Double](10)) 

    val Add2: DenseMatrix[Double] =  sqrt(D_t) * A_t * sqrt(D_t)

    val Add: DenseMatrix[Double] = Add1 + Add2

    val InvA: DenseMatrix[Double] = pinv(Add)

    val AA_t: DenseMatrix[Double] = sqrt(D_t) * InvA * sqrt(D_t)

    val b_t: DenseVector[Double] = model._2 + label * x_t

    val w_t: DenseVector[Double] = AA_t * b_t

    (A_t, b_t, w_t, lambda)
  }

  override def predict(
      dataPoint: UnlabeledVector,
      model: OSLOGModel): Double = {

     val w_t: DenseVector[Double] = model._3

    val x_t = dataPoint.toDenseVector
    
    val y_t = w_t.t * x_t

    y_t
  }
}
