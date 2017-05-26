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
object Cuts{

  /**
   * The alphabet for the words.
   */
  private val Alphabet : Array[Char] = Array('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
    'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' )

  /**
   * Breakpoints for 2 areas.
   */
  private val Cuts2 : Array[Double] = Array(0.0d)

  /**
   * Breakpoints for 3 areas.
   */
  private val Cuts3 : Array[Double] = Array(-0.4307273, 0.4307273)

  /**
   * Breakpoints for 4 areas.
   */
  private val Cuts4 : Array[Double] = Array(-0.6744898, 0.0000000, 0.6744898)

  /**
   * Breakpoints for 5 areas.
   */
  private val Cuts5 : Array[Double] = Array(-0.8416212, -0.2533471, 0.2533471, 0.8416212)

  /**
   * Breakpoints for 6 areas.
   */
  private val Cuts6 : Array[Double] = Array(-0.9674216, -0.4307273, 0.0000000, 0.4307273,
    0.9674216)

  /**
   * Breakpoints for 7 areas.
   */
  private val Cuts7 : Array[Double] = Array(-1.0675705, -0.5659488, -0.1800124, 0.1800124,
    0.5659488, 1.0675705)

  /**
   * Breakpoints for 8 areas.
   */
  private val Cuts8 : Array[Double] = Array(-1.1503494, -0.6744898, -0.3186394, 0.0000000,
    0.3186394, 0.6744898, 1.1503494)

  /**
   * Breakpoints for 9 areas.
   */
  private val Cuts9 : Array[Double] = Array(-1.2206403, -0.7647097, -0.4307273, -0.1397103,
    0.1397103, 0.4307273, 0.7647097, 1.2206403)

  /**
   * Breakpoints for 10 areas.
   */
  private val Cuts10 : Array[Double] = Array(-1.2815516, -0.8416212, -0.5244005, -0.2533471,
    0.0000000, 0.2533471,  0.5244005, 0.8416212, 1.2815516)

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
      case 3 => Cuts.Cuts3
      case 4 => Cuts.Cuts4
      case 5 => Cuts.Cuts5
      case 6 => Cuts.Cuts6
      case 7 => Cuts.Cuts7
      case 8 => Cuts.Cuts8
      case 9 => Cuts.Cuts9
      case 10 => Cuts.Cuts10
      case _ => throw new UnsupportedOperationException("Number of cuts not supported")
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
      case _ => throw new UnsupportedOperationException("Number of cuts not supported")
    }
  }

  /**
   * Find the letter associated with a given number using a set of cuts.
   * @param cuts The cuts.
   * @param number The number.
   * @return A character.
   */
  def findLetter(cuts: Array[Double], number: Double) : Char = {
    var position : Int = 0
    while((position < cuts.length) && (cuts(position) <= number)){
      position = position + 1
    }
    Cuts.Alphabet(position)
  }

}
