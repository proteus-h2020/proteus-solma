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

package eu.proteus.solma.moments


import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.common.ParameterMap
import eu.proteus.solma._
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{Vector => BreezeVector}

import scala.collection.mutable

@Proteus
class MomentsEstimator extends StreamTransformer[MomentsEstimator] {
}

object MomentsEstimator {

  // ====================================== Parameters =============================================


  // ====================================== Extra =============================================

  class Moments(
    var counter: Long,
    var currMean: BreezeVector[Double],
    var M2: BreezeVector[Double]) {

    def this(x: BreezeVector[Double]) = this(1L, x.copy, BreezeVector.zeros[Double](x.size))

    def process(x: BreezeVector[Double]): this.type = {
      counter += 1L
      val delta = x :- currMean
      currMean :+= (delta :/ counter.toDouble)
      val delta2 = x :- currMean
      M2 :+= (delta :*= delta2)
      this
    }

    def mean: BreezeVector[Double] = {
      currMean
    }

    def variance: BreezeVector[Double] = {
      M2 :/ (counter.toDouble - 1)
    }

    def merge(that: Moments): this.type = {
      val cnt = counter.toDouble + that.counter.toDouble
      val delta = that.currMean :- currMean
      val m_a = variance :* (counter.toDouble - 1)
      val m_b = that.variance :* (that.counter.toDouble - 1)
      currMean :*= counter.toDouble
      currMean :+= (that.currMean :* that.counter.toDouble)
      currMean :/= cnt
      delta :^= 2.0
      delta :*= (counter.toDouble * that.counter.toDouble / cnt)
      M2 := (m_a :+ m_b) :+= delta
      counter += that.counter
      this
    }

    override def clone(): Moments = {
      new Moments(counter, currMean.copy, M2.copy)
    }

    override def toString: String = {
      "[counter=" + counter + ",mean=" + mean.toString + ",variance=" + variance.toString + "]"
    }
  }

  // ==================================== Factory methods ==========================================

  def apply(): MomentsEstimator = {
    new MomentsEstimator()
  }

  // ==================================== Operations ==========================================

  implicit def fitNoOp[T] = {
    new StreamFitOperation[MomentsEstimator, T]{
      override def fit(
          instance: MomentsEstimator,
          fitParameters: ParameterMap,
          input: DataStream[T])
        : Unit = {}
    }
  }

  implicit def transformMomentsEstimators[T <: Vector] = {
    new TransformDataStreamOperation[MomentsEstimator, T, Moments]{
      override def transformDataStream(
        instance: MomentsEstimator,
        transformParameters: ParameterMap,
        input: DataStream[T])
        : DataStream[Moments] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[T](input)
        statefulStream.mapWithState((in, state: Option[Moments]) => {
          val (element, pid) = in
          val x = element.asBreeze
          val metrics = state match {
            case Some(curr) => {
              curr.process(x)
            }
            case None => {
              new Moments(x)
            }
          }
          ((pid, metrics), Some(metrics))
        }).fold(new mutable.HashMap[Int, Moments]())((acc: mutable.HashMap[Int, Moments], in) => {
            val (pid, moments) = in
            acc(pid) = moments
            acc.remove(-1)
            val it = acc.values.iterator
            val ret = it.next.clone()
            while (it.hasNext) {
              ret.merge(it.next)
            }
            acc(-1) = ret
            acc
          }
        ).map(data => data(-1))
      }
    }
  }


}
