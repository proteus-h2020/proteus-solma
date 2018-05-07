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

package eu.proteus.solma.aar.algorithm

import breeze.linalg._
import breeze.numerics.{abs, sqrt}
import org.apache.flink.ml.math.Breeze._

import breeze.linalg.DenseVector
import eu.proteus.annotations.Proteus
import eu.proteus.solma.aar.AAR
import eu.proteus.solma.aar.AAR.{AARModel, UnlabeledVector}


@Proteus
class AARAlgorithm(instance: AAR) extends BaseAARAlgorithm[UnlabeledVector, Double, AARModel]  {

  override def delta(
      dataPoint: UnlabeledVector,
      model: AARModel,
      label: Double
  ): (DenseMatrix[Double], DenseVector[Double]) = {

    val x_t = dataPoint.toDenseVector

    val a_t: DenseMatrix[Double] = x_t * x_t.t

    val A_t: DenseMatrix[Double] = model._1 + a_t

    val b_t: DenseVector[Double] = model._2 + label * x_t

    (A_t, b_t)
  }

  override def predict(
      dataPoint: UnlabeledVector,
      model: AARModel): Double = {

    val x_t = dataPoint.toDenseVector

    val A_t = model._1

    val b_t = model._2

    val invA = pinv(A_t)

    val r_t = b_t.t * invA * x_t

    val y_t = r_t /  (1 + (x_t.t * invA * x_t))

    y_t
  }
}
