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

package eu.proteus.solma.tnlms.algorithm

import breeze.linalg._
import breeze.numerics.{abs, sqrt}
import org.apache.flink.ml.math.Breeze._

import breeze.linalg.DenseVector
import eu.proteus.annotations.Proteus
import eu.proteus.solma.tnlms.TNLMS
import eu.proteus.solma.tnlms.TNLMS.{TNLMSModel, UnlabeledVector}


@Proteus
class TNLMSAlgorithm(instance: TNLMS) extends BaseTNLMSAlgorithm[UnlabeledVector, Double, TNLMSModel]  {

  override def delta(
      dataPoint: UnlabeledVector,
      model: TNLMSModel,
      label: Double
  ): (DenseVector[Double], Double) = {

    val eta =  model._2

    val x_t = dataPoint.toDenseVector

    var w_t: DenseVector[Double] = model._1 

    val lambda: Double = (label - w_t.t * x_t) / (eta + x_t.t * x_t)

    w_t = w_t + lambda * x_t

    (w_t, eta)
  }

  override def predict(
      dataPoint: UnlabeledVector,
      model: TNLMSModel): Double = {

    val x_t = dataPoint.toDenseVector

    val w_t = model._1

    val y_t = w_t.t * x_t

    y_t
  }
}
