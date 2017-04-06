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
package eu.proteus.solma.sax

/**
 * SAX cuts definition. For calculating the cuts or breakpoints, the area under the Normal
 * curve is split into equally sized ranges. In this object we store the precalculated cuts
 * and associated distance matrix to avoid calculating them online. In order to obtain the
 * cuts, use R to compute the following:
 * {{{
 *   > qnorm(seq(0, 1, by=1/2), 0, 1)
 *   [1] -Inf    0  Inf
 *   > qnorm(seq(0, 1, by=1/3), 0, 1)
 *   [1]       -Inf -0.4307273  0.4307273        Inf
 *   > qnorm(seq(0, 1, by=1/4), 0, 1)
 *   [1]       -Inf -0.6744898  0.0000000  0.6744898        Inf
 *   > ...
 * }}}
 *
 * From the results of R, the ranges are obtained. For example, for 4 cuts, the ranges are
 * [-Inf, -0.6744898), [-0.6744898, 0), [0, 0.6744898), [0.6744898, Inf). Based on those ranges,
 * we next calculate the distance matrix between the ranges.
 */
// Deactivate the magic number check as this class contains raw numbers.
object Cuts{

  /**
   * Breakpoints for 2 areas.
   */
  private val Cuts2 : Array[Double] = Array(0.0d)

  /**
   * Distances for 2 areas.
   */
  private val Distances2 : Array[Array[Double]] = Array(Array(0.0d, 0.0d), Array(0.0d, 0.0d))

  /**
   * Get the breakpoints resulting from splitting the Normal curve into n cuts.
   * @param numberCuts The number of cuts.
   * @return An array of doubles without the +- infinity limits.
   */
  def breakpoints(numberCuts: Int) : Array[Double] = {
    numberCuts match {
      case 2 => Cuts.Cuts2
      case _ => throw new RuntimeException("Number of cuts not supported")
    }
  }

  /**
   * Get the distance matrix resulting from splitting the normal curve into n cuts that form
   * a set of ranges. The matrix contains the distance between the ranges.
   * @param numberCuts The number of cuts.
   * @return An matrix of doubles.
   */
  def distanceMatrix(numberCuts: Int) : Array[Array[Double]] = {
    numberCuts match {
      case 2 => Cuts.Distances2
      case _ => throw new RuntimeException("Number of cuts not supported")
    }
  }

}
