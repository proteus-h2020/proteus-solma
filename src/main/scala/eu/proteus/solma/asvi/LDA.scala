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

package eu.proteus.solma.asvi

import breeze.linalg.{DenseMatrix, DenseVector}

import scala.collection.mutable


class LDA(K: Int, maxIter: Int = 100, nu: Double = 0.1, vocab: mutable.Map[String, Long]) extends GradientAlgorithm[
  DenseVector[String],
  DenseMatrix[Double]] {

  private val vi = new StreamVI(
    data_from=false,
    K=K,
    dim = vocab.size,
    nu=0.001,
    covar_0=vocab,
    max_itr=5,
    v_0 = 0.01)

  override def gradient(x: DenseVector[String], eta: DenseMatrix[Double]): DenseMatrix[Double] = {

    vi.findLambda(x, 0.01, eta)

  }

}
