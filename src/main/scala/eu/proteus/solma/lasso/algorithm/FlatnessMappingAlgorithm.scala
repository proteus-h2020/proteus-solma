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

import scala.util.Try

/**
  * Algorithm to interpolate flatness values
  *
  */
class FlatnessMappingAlgorithm(input: Vector[Double],
                               labels: Vector[(Double, Double)]){

  val nearests: Vector[(Double, Double)] = input.map{x => labels.sortBy(v => math.abs(v._1 - x)).toList.head}
  val secondNearests: Vector[(Double, Double)] = labels.length match {
    case 1 => nearests
    case _ => input.map { x => labels.sortBy(v => math.abs(v._1 - x)).toList(1) }
  }

  def flatnessInterpolation(x: Double, x1: Double, x2: Double, y1: Double, y2: Double): Double = {
    if (x1 != x2) {
      ((y1 - y2) / (x1 - x2)) * (x - x1) + y1
    }
    else {
      y1
    }
  }

  def apply: Vector[Double] = {
    def processPosition(position: Int, x: Double): Double = {
      val (x1, y1) = nearests(position)
      val (x2, y2) = secondNearests(position)

      Try(flatnessInterpolation(x, x1, x2, y1, y2)).getOrElse(labels.head._2)
    }

    input.zipWithIndex.map{x => processPosition(x._2, x._1)}
  }
}
