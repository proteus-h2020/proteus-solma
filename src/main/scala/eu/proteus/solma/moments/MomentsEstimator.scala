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
package eu.proteus.solma.moments


import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import eu.proteus.solma._
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{Vector => BreezeVector}
import eu.proteus.solma.events.StreamEvent
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * A simple stream transformer that ingests a stream of samples and outputs
  * simple mean and variance of the input stream. It requires to know the
  * total number of features in advance, but it can work with features
  * coming at different "speed", i.e., asynchronously.
  * The output is the stream of the parallel moments, outputted as soon as
  * an instance is updated. Optionally, the output may be an aggregation
  * of all the parallel moments (with loss of parallelism).
  */
@Proteus
class MomentsEstimator extends StreamTransformer[MomentsEstimator] {
  import MomentsEstimator._

  /**
    * Enable the aggregation of all the parallel moments, which
    * will be sent downstream to the next operation.
    * @param enabled
    */
  def enableAggregation(enabled: Boolean): MomentsEstimator = {
    parameters.add(AggregateMoments, enabled)
    this
  }

  /**
    * The total number of features on which the mean and the variance
    * are calculated.
    * @param n
    */
  def setFeaturesCount(n: Int): MomentsEstimator = {
    parameters.add(FeaturesCount, n)
    this
  }
}

object MomentsEstimator {

  // ====================================== Parameters =========================================

  case object AggregateMoments extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(false)
  }

  case object FeaturesCount extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }



  // ====================================== Extra =============================================

  class Moments(
     private [moments] var counters: BreezeVector[Double],
     private [moments] var currMean: BreezeVector[Double],
     private [moments] var M2: BreezeVector[Double]) {

    def this(x: BreezeVector[Double], slice: IndexedSeq[Int], n: Int) = {
      this(BreezeVector.zeros[Double](n),
        BreezeVector.zeros[Double](n),
        BreezeVector.zeros[Double](n))
      counters(slice) :+= 1.0
      currMean(slice) :+= x
    }

    def process(x: BreezeVector[Double], slice: IndexedSeq[Int]): this.type = {
      counters(slice) :+= 1.0
      val delta = x :- currMean(slice)
      currMean(slice) :+= (delta :/ counters(slice))
      val delta2 = x :- currMean(slice)
      M2(slice) :+= (delta :*= delta2)
      this
    }

    def counter(id: Int): Double = {
      counters(id)
    }

    def mean: BreezeVector[Double] = {
      currMean
    }

    def variance: BreezeVector[Double] = {
      M2 :/ (counters :- 1.0)
    }

    def merge(that: Moments): this.type = {
      val cnt = counters :+ that.counters
      val delta = that.currMean :- currMean
      val m_a = variance :* (counters :- 1.0)
      val m_b = that.variance :* (that.counters :- 1.0)
      currMean :*= counters
      currMean :+= (that.currMean :* that.counters)
      currMean :/= cnt
      delta :^= 2.0
      delta :*= (counters :* that.counters :/ cnt)
      M2 := (m_a :+ m_b) :+= delta
      counters :+= that.counters
      this
    }

    def copy(): Moments = {
      new Moments(counters.copy, currMean.copy, M2.copy)
    }

    override def toString: String = {
      "[counter=" + counters.toString + ",mean=" +
        mean.toString + ",variance=" + variance.toString + "]"
    }
  }

  // ==================================== Factory methods ==========================================

  def apply(): MomentsEstimator = {
    new MomentsEstimator()
  }

  // ==================================== Operations ==========================================

  implicit def fitNoOp[T : TypeInformation] = {
    new StreamFitOperation[MomentsEstimator, T]{
      override def fit(
          instance: MomentsEstimator,
          fitParameters: ParameterMap,
          input: DataStream[T])
        : Unit = {}
    }
  }

  implicit def transformMomentsEstimators[E <: StreamEvent : TypeInformation : ClassTag] = {
    new TransformDataStreamOperation[MomentsEstimator, E, (Long, Moments)]{
      override def transformDataStream(
        instance: MomentsEstimator,
        transformParameters: ParameterMap,
        input: DataStream[E])
        : DataStream[(Long, Moments)] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val featuresCount = resultingParameters(FeaturesCount)
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[E](input,
          resultingParameters.get(PartitioningOperation))

        val intermediate = statefulStream.mapWithState((in, state: Option[Moments]) => {
          val (event, pid) = in
          val slice = event.slice
          val x = event.data.asBreeze
          val metrics = state match {
            case Some(curr) => {
              curr.process(x, slice)
            }
            case None => {
              new Moments(x, slice, featuresCount)
            }
          }
          ((pid, metrics), Some(metrics))
        })

        if (resultingParameters(AggregateMoments)) {
          intermediate.fold(new mutable.HashMap[Long, Moments]())(
            (acc: mutable.HashMap[Long, Moments], in) => {
              val (pid, moments) = in
              acc(pid) = moments
              acc.remove(-1)
              val it = acc.values.iterator
              val ret = it.next.copy()
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
