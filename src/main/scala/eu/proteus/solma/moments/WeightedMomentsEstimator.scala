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
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import eu.proteus.solma._
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{Vector => BreezeVector}
import eu.proteus.solma.events.WeightedStreamEvent
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation

import scala.collection.mutable


@Proteus
class WeightedMomentsEstimator extends StreamTransformer[WeightedMomentsEstimator] {
  import WeightedMomentsEstimator._

  /**
    * Enable the aggregation of all the parallel Weightedmoments, which
    * will be sent downstream to the next operation.
    * @param enabled
    */
  def enableAggregation(enabled: Boolean): WeightedMomentsEstimator = {
    parameters.add(AggregateWeightedMoments, enabled)
    this
  }

  /**
    * The total number of features on which the mean and the variance
    * are calculated.
    * @param n
    */
  def setFeaturesCount(n: Int): WeightedMomentsEstimator = {
    parameters.add(FeaturesCount, n)
    this
  }
}

object WeightedMomentsEstimator {

  // ====================================== Parameters =============================================

  case object AggregateWeightedMoments extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(false)
  }

  case object FeaturesCount extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  // ====================================== Extra =============================================

  class WeightedMoments(
                 private [moments] var counters: BreezeVector[Double],
                 private [moments] var currMean: BreezeVector[Double],
                 private [moments] var M2: BreezeVector[Double],
                 private [moments] var currweight:BreezeVector[Double] ) {

    def this(x: BreezeVector[Double], slice: IndexedSeq[Int], n: Int,weight:BreezeVector[Double]) = {
      this(BreezeVector.zeros[Double](n), BreezeVector.zeros[Double](n),
            BreezeVector.zeros[Double](n),BreezeVector.zeros[Double](n))

      counters(slice) :+= 1.0
      currMean(slice) :+= x
      currweight(slice):+=weight
    }

    def process(x: BreezeVector[Double], slice: IndexedSeq[Int],weight:BreezeVector[Double]): this.type = {
      counters(slice) :+= 1.0
      val oldmean=currMean(slice).copy
      currMean(slice):*= currweight(slice):/(currweight(slice):+weight)
      currMean(slice):+=x:*(weight:/(currweight(slice):+weight))
      M2(slice) :+= (weight:*((x:-oldmean):*(x:-currMean(slice))))
      currweight(slice):+=weight
      this
    }

    def counter(id: Int): Double = {
      counters(id)
    }

    def mean: BreezeVector[Double] = {
      currMean
    }

    def variance: BreezeVector[Double] = {
      M2 :/ currweight
    }

    def merge(that: WeightedMoments): this.type = {
      counters :+= that.counters
     M2:+=that.M2
      M2:+=currweight:*(currMean:*currMean)
      M2:+=that.currweight:*(that.currMean:*that.currMean)
      val del=currMean.copy
      del:*=currweight
      del:+=that.currweight:*that.currMean
      del:^= 2.0
      del:/=currweight:+that.currweight
      M2:-=del
      currMean=currweight:*currMean
      val temp=that.currMean:*that.currweight
      currweight:+=that.currweight
      currMean=(temp:+currMean):/currweight
      this
    }

    override def clone(): WeightedMoments = {
      new WeightedMoments(counters.copy, currMean.copy, M2.copy,currweight.copy)
    }

    override def toString: String = {
      "[counter=" + counters.toString + ",mean=" + mean.toString + ",variance=" + variance.toString + ",weight=" + currweight.toString+"]"
    }
  }

  // ==================================== Factory methods ==========================================

  def apply(): WeightedMomentsEstimator = {
    new WeightedMomentsEstimator()
  }

  // ==================================== Operations ==========================================

  implicit def fitNoOp[T] = {
    new StreamFitOperation[WeightedMomentsEstimator, T]{
      override def fit(
                        instance: WeightedMomentsEstimator,
                        fitParameters: ParameterMap,
                        input: DataStream[T])
      : Unit = {}
    }
  }

  implicit def transformWeightedMomentsEstimators[E <: WeightedStreamEvent] = {
    new TransformDataStreamOperation[WeightedMomentsEstimator, E, (Int, WeightedMoments)]{
      override def transformDataStream(
                                        instance: WeightedMomentsEstimator,
                                        transformParameters: ParameterMap,
                                        input: DataStream[E])
      : DataStream[(Int, WeightedMoments)] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val featuresCount = resultingParameters(FeaturesCount)
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[E](input, resultingParameters.get(PartitioningOperation))

        val intermediate = statefulStream.mapWithState((in, state: Option[WeightedMoments]) => {
          val (event, pid) = in
          val slice = event.slice
          val x = event.data.asBreeze.copy
          val w=event.weight.asBreeze.copy
          val metrics = state match {
            case Some(curr) => {

              curr.process(x, slice,w)
            }
            case None => {
              new WeightedMoments(x, slice, featuresCount,w)
            }
          }
          ((pid, metrics), Some(metrics))
        })
        if (resultingParameters(AggregateWeightedMoments)) {
          intermediate.fold(new mutable.HashMap[Int, WeightedMoments]())((acc: mutable.HashMap[Int, WeightedMoments], in) => {
            val (pid, weightedmoments) = in
            acc(pid) = weightedmoments
            acc.remove(-1)
            val it = acc.values.iterator
            val ret = it.next.clone()
            while (it.hasNext) {
              ret.merge(it.next)
            }
            acc(-1) = ret
            acc
          }
          ).map(data => (0, data(-1)))
        } else {
          intermediate
        }
      }
    }
  }
}