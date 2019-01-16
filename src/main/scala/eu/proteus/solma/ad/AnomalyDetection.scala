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

package eu.proteus.solma.ad

import breeze.linalg
import scala.math.pow
import breeze.numerics.sqrt

import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.streaming.api.scala._
import eu.proteus.solma._
import breeze.linalg.svd.{SVD => BreezeSVD}
import breeze.linalg.{DenseMatrix, DenseVector}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

@Proteus
class AnomalyDetection extends StreamTransformer[AnomalyDetection] {

  import AnomalyDetection._

  def setFeaturesNumber(count: Int): AnomalyDetection = {
    parameters.add(FeaturesNumber, count)
    this
  }

  def setSketchSize(size: Int): AnomalyDetection = {
    parameters.add(SketchSize, size)
    this
  }

  def append(vector: DenseVector[Double], sketch: Sketch): Sketch = {
    var ell = sketch.ell
    var matrix = sketch.matrix
    var nextZeroRow = sketch.nextZeroRow

    if (nextZeroRow >= 2*ell) {
      var sk = rotate(sketch)
      ell = sk.ell
      matrix = sk.matrix
      nextZeroRow = sk.nextZeroRow
    }
    matrix(nextZeroRow, ::) := vector.t
    nextZeroRow += 1

    Sketch(ell, matrix, nextZeroRow)
  } 

  def rotate(sketch: Sketch): Sketch = {
    val ell = sketch.ell
    var matrix = sketch.matrix
    var nextZeroRow = sketch.nextZeroRow

    val BreezeSVD(_, s, vt) = linalg.svd.reduced(matrix)

    if (s.length >= ell) {
      val sShrunk = sqrt((s(0 until ell) :* s(0 until ell)) -(pow(s(ell-1),2)))
      matrix(0 until ell,::) := linalg.diag(sShrunk).t * vt
      matrix(ell until matrix.rows,::) := 0.0
      nextZeroRow = ell
    } else {
      matrix(0 until s.length,::) := linalg.diag(s).t * vt
      matrix(s.length until matrix.rows,::) := 0.0
      nextZeroRow = s.length
    }

    Sketch(ell, matrix, nextZeroRow)
  }

  def get(sketch: Sketch): DenseMatrix[Double] = {
    sketch.matrix(0 until sketch.ell,::)
  }

}

object AnomalyDetection {

  // ====================================== Helpers =============================================

  case class Sketch(ell: Int, matrix: DenseMatrix[Double], nextZeroRow: Int)

  // ====================================== Parameters =============================================

  case object SketchSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  case object FeaturesNumber extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  // ==================================== Factory methods ==========================================

  def apply(): AnomalyDetection = {
    new AnomalyDetection()
  }

  // ==================================== Operations ==========================================

  def init(ell: Int, d: Int): Sketch = {
    var matrix: DenseMatrix[Double] = DenseMatrix.zeros(2*ell, d) 
    var nextZeroRow: Int = 0
    Sketch(ell, matrix, nextZeroRow)
  }

}
