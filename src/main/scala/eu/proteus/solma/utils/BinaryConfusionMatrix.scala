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

package eu.proteus.solma.utils

class BinaryConfusionMatrix(
  val numTruePositives: Long,
  val numFalsePositives: Long,
  val numFalseNegatives: Long,
  val numTrueNegatives: Long
 ) extends Serializable {

  def numPositives: Long = numTruePositives + numFalseNegatives

  def numNegatives: Long = numFalsePositives + numTrueNegatives

  def accuracy: Double = {
    val total = numPositives + numNegatives
    if (total == 0) {
      0.0
    } else {
      (numTruePositives + numTrueNegatives).toDouble / total
    }
  }

  def precision: Double = {
    val totalPositives = numTruePositives + numFalseNegatives
    if (totalPositives == 0) {
      1.0
    } else {
      numTruePositives.toDouble / totalPositives
    }
  }

  def falsePositivesRate: Double = {
    val totalNegatives = numFalsePositives + numTrueNegatives
    if (totalNegatives == 0) {
      0.0
    } else {
      numFalseNegatives.toDouble / totalNegatives
    }
  }

  def recall: Double = {
    val totalPositives = numTruePositives + numFalseNegatives
    if (totalPositives == 0) {
      0.0
    } else {
      numTruePositives.toDouble / totalPositives
    }
  }

  def fMeasure(beta: Double): Double = {
    val p = precision
    val r = recall
    if (p + r == 0) {
      0.0
    } else {
      val beta2 = beta * beta
      (1.0 + beta2) * (p * r) / (beta2 * p + r)
    }
  }

  def f1Score: Double = fMeasure(1.0)

  override def toString: String = {
    s"BinaryConfusionMatrix: TP=$numTruePositives TN=$numTrueNegatives " +
      s"FP=$numFalsePositives FN=$numFalseNegatives precision=$precision " +
      s"accuracy=$accuracy " +
      s"recall=$recall fpRate=$falsePositivesRate F1-Score=$f1Score"
  }
}

object BinaryConfusionMatrix {
  def apply(
    numTruePositives: Long,
    numFalsePositives: Long,
    numFalseNegatives: Long,
    numTrueNegatives: Long
  ): BinaryConfusionMatrix = {
    new BinaryConfusionMatrix(
      numTruePositives,
      numFalsePositives,
      numFalseNegatives,
      numTrueNegatives
    )
  }
}

class BinaryConfusionMatrixBuilder extends Serializable {

  var totalPositives: Long = 0
  var totalNegatives: Long = 0
  var truePositives: Long = 0
  var trueNegatives: Long = 0

  def add(outcome: Double, expected: Double) : this.type = {
    if (outcome > 0) {
      totalPositives += 1
      if (expected > 0) {
        truePositives += 1
      }
    } else {
      totalNegatives += 1
      if (expected < 0) {
        trueNegatives += 1
      }
    }
    this
  }

  def toConfusionMatrix: BinaryConfusionMatrix = {
    BinaryConfusionMatrix(
      truePositives,
      totalNegatives - trueNegatives,
      totalPositives - truePositives,
      trueNegatives
    )
  }

}
