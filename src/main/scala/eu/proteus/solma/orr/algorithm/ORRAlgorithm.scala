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

import breeze.linalg._
import breeze.numerics.{abs, sqrt}
import org.apache.flink.ml.math.Breeze._

import breeze.linalg.DenseVector
import eu.proteus.annotations.Proteus
import eu.proteus.solma.orr.ORR
import eu.proteus.solma.orr.ORR.{ORRModel, UnlabeledVector}


@Proteus
class ORRAlgorithm(instance: ORR) extends BaseORRAlgorithm[UnlabeledVector, Double, ORRModel]  {

  override def delta(
      dataPoint: UnlabeledVector,
      model: ORRModel,
      label: Double
  ): (DenseMatrix[Double], DenseVector[Double]) = {

    val x_t = dataPoint.toDenseVector

    val a_t: DenseMatrix[Double] = x_t.asDenseMatrix.t * x_t.asDenseMatrix

    val n: Int = a_t.cols

    val A_t: DenseMatrix[Double] = model._1 + a_t

    val inve: DenseMatrix[Double] = pinv(A_t)

    val dif: Double = 1 + (x_t.t * inve * x_t)

    println(model._2)

    val b_t: DenseVector[Double] = model._2 + label * x_t

    println(b_t)

    (A_t, b_t)
  }

  override def predict(
      dataPoint: UnlabeledVector,
      model: ORRModel): Double = {

    val x_t = dataPoint.toDenseVector

    val A_t = model._1
    val b_t = model._2

    val y_t = b_t.toDenseMatrix * pinv(A_t) * x_t

    y_t.data(0)
  }
}
