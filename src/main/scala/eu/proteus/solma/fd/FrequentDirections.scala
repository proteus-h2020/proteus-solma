/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.proteus.solma.fd

import breeze.linalg
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
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, Vector => BreezeVector}
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

@Proteus
class FrequentDirections extends StreamTransformer[FrequentDirections] {

  import FrequentDirections._

  def setFeaturesNumber(count: Int): FrequentDirections = {
    parameters.add(FeaturesNumber, count)
    this
  }

  def setSketchSize(size: Int): FrequentDirections = {
    parameters.add(SketchSize, size)
    this
  }

  def enableAggregation(enabled: Boolean): FrequentDirections = {
    parameters.add(AggregateSketches, enabled)
    this
  }

}

object FrequentDirections {

  // ====================================== Helpers =============================================

  case class Sketch(queue: mutable.Queue[Int], matrix: BreezeDenseMatrix[Double])

  // ====================================== Parameters =============================================

  case object SketchSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  case object AggregateSketches extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(false)
  }

  case object FeaturesNumber extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  // ==================================== Factory methods ==========================================

  def apply(): FrequentDirections = {
    new FrequentDirections()
  }

  // ==================================== Operations ==========================================


  implicit def fitNoOp[T] = {
    new StreamFitOperation[FrequentDirections, T]{
      override def fit(
          instance: FrequentDirections,
          fitParameters: ParameterMap,
          input: DataStream[T])
        : Unit = {}
    }
  }

  var ell, d: Int = -1

  implicit def treansformFrequentDirections[T <: Vector : TypeInformation : ClassTag] = {
    new TransformDataStreamOperation[FrequentDirections, T, T] {
      override def transformDataStream(
          instance: FrequentDirections,
          transformParameters: ParameterMap,
          input: DataStream[T])
      : DataStream[T] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[T](input, resultingParameters.get(PartitioningOperation))
        ell = resultingParameters(SketchSize)
        d = resultingParameters(FeaturesNumber)
        assert(ell < d * 2, "the sketch size should be smaller than twice the number of features")
        val sketchesStream = statefulStream.flatMapWithState((in, state: Option[Sketch]) => {
          val (elem, _) = in
          val out = new ListBuffer[BreezeVector[Double]]()
          val sketch = updateSketch(elem.asBreeze, state, out)
          (out, Some(sketch))
        })
        if (resultingParameters(AggregateSketches)) {
          sketchesStream.fold(None.asInstanceOf[Option[Sketch]])((acc, item) => {
            Some(updateSketch(item, acc))
          }).flatMap((acc, out) => {
            val Sketch(zeroRows, matrix) = acc.get
            val toOutputType = (x: BreezeVector[Double]) => x.copy.fromBreeze.asInstanceOf[T]
            if (zeroRows.isEmpty) {
              (0 until matrix.rows) foreach (i => out collect toOutputType(matrix(i, ::).t))
            }
          })
        } else {
          sketchesStream.map(x => {
            x.fromBreeze.asInstanceOf[T]
          })
        }
      }
    }
  }

  private [solma] def updateSketch(
      elem: BreezeVector[Double],
      state: Option[Sketch],
      out: mutable.Buffer[BreezeVector[Double]] = null
  ): Sketch = {
    state match {
      case Some(Sketch(zeroRows, matrix)) => {
        if (zeroRows.isEmpty) {
          // sketch is full
          if (out != null) {
            for (i <- 0 until matrix.rows) {
              out += matrix(i, ::).t.copy
            }
          }
          // perform compression
          compressMatrix(matrix, zeroRows)
        }
        matrix(zeroRows.dequeue, ::) := elem.t
        Sketch(zeroRows, matrix)
      }
      case None => {
        val zeroRows = new mutable.Queue[Int]()
        val matrix = BreezeDenseMatrix.zeros[Double](ell, d)
        matrix(0, ::) := elem.t
        for (i <- 1 until ell) {
          zeroRows += i
        }
        Sketch(zeroRows, matrix)
      }
    }
  }

  def compressMatrix(matrix: BreezeDenseMatrix[Double], zeroRows: mutable.Queue[Int]): Unit = {
    val BreezeSVD(u, sigma, vt) = linalg.svd.reduced(matrix)
    val delta = math.pow(sigma(math.floor(ell / 2).toInt), 2.0)
    val sigmaMinusDeltaMatrix = BreezeDenseMatrix.zeros[Double](ell, d)
    sigma.foreachPair((idx, s) => {
      val x = math.pow(s, 2.0) - delta
      sigmaMinusDeltaMatrix(idx, idx) = if (x < 0.0) {
        0.0
      } else {
        math.sqrt(x)
      }
    })
    matrix := sigmaMinusDeltaMatrix * vt
    val nzr = linalg.sum(matrix, linalg.Axis._1)
    nzr.foreachPair((idx, v) => {
      if (math.round(v) == 0) {
        zeroRows += idx
      }
    })
  }

}
