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

package eu.proteus.solma.lasso.algorithm

import breeze.linalg.{DenseMatrix, VectorBuilder}
import eu.proteus.solma.lasso.LassoDelayedFeedbacks.LabelBundleVectors
import org.scalatest.FlatSpec

import scala.util.Random


class FlatnessMappingAlgorithmTest extends FlatSpec {
  val inputSize = 10
  val labelSize = 5
  val random = new Random(100L)

  private def randomVector(size: Int) = {
    val vectorBuilder = new VectorBuilder[Double](length = size)
    0 until size foreach { i =>
      vectorBuilder.add(i, random.nextDouble())
    }
    vectorBuilder.toDenseVector
  }
  val positions: Vector[Double] = Array(1.11, 2.22, 3.33, 4.44, 5.55).toVector
  val labels: Vector[Double] = Array(1.55, 2.44, 3.33, 4.22, 5.11).toVector
  val positionsToInterpolate: Vector[Double] = Array (2.33, 4.55, 1.21, 12.23, 49.02, 2.33, 4.55, 1.21, 12.23,
    49.02).toVector
  val testLabelData: Vector[(Double, Double)] = positions.zipAll(labels, 0.0, 0.0)

  "Flatness Mapping Algorithm" should "give reasonable error on test data" in {
    val labelOutput = new FlatnessMappingAlgorithm(positionsToInterpolate, testLabelData).apply
    assert(labelOutput.head == 2.528198198198198)
  }

}
