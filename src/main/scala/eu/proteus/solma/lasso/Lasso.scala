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

import breeze.linalg.{DenseMatrix, DenseVector}
import eu.proteus.solma.pipeline.StreamPredictor
import org.apache.flink.ml.pipeline.Estimator
import org.slf4j.Logger


object Lasso {
  type LabeledVector = (DenseVector[Double], Double)
  type UnlabeledVector = DenseVector[Double]
  type OptionLabeledVector = Either[LabeledVector, UnlabeledVector]

  type LassoParam = (DenseMatrix[Double], DenseVector[Double])
  type LassoModel = LassoParam

  /**
    * Class logger.
    */
  private val Log: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

  implicit def fitImplementation[T] = {
    new LassoFitOperation[T]
  }

  implicit def predictImplementation[K <: Lasso.OptionLabeledVector] = {
    new LassoPredictOperation[K]
  }

  implicit def transformImplementation[T <: Lasso.OptionLabeledVector] = {
    new LassoStreamTransformOperation[T]
  }
}

class Lasso extends StreamPredictor[Lasso] with Estimator[Lasso]{
  import eu.proteus.solma.lasso.Lasso.Log

  /**
    * Apply helper.
    * @return A new [[Lasso]].
    */
  def apply(): Lasso = {
    new Lasso()
  }
}
