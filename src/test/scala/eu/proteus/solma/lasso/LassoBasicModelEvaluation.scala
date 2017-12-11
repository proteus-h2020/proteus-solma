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

import breeze.linalg.{DenseVector, SparseVector}
import breeze.numerics.abs
import eu.proteus.solma.lasso.Lasso.LassoModel
import eu.proteus.solma.lasso.algorithm.LassoBasicAlgorithm
import org.slf4j.LoggerFactory

class LassoBasicModelEvaluation

object LassoBasicModelEvaluation {

  private val log = LoggerFactory.getLogger(classOf[LassoBasicModelEvaluation])


  def accuracy(model: LassoModel,
               testLines: Traversable[(DenseVector[Double], Option[Double])],
               featureCount: Int,
               pac: LassoBasicAlgorithm): Double = {

    var cnt: Int = 0
    var sumDiffs: Double = 0.0
    testLines.foreach { case (vector, label) => label match {
      case Some(lab) =>
        val real = lab
        val predicted = pac.predict(Right(vector), model)
        sumDiffs += abs(predicted - real)
        cnt += 1
      case _ => throw new IllegalStateException("Labels shold not be missing.")
    }
    }
    val percent = sumDiffs / cnt

    percent
  }


}

