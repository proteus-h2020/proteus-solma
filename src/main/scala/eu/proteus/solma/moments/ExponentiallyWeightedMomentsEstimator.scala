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
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{Vector => BreezeVector}
import eu.proteus.solma.events.StreamEvent
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.collection.mutable
import scala.reflect.ClassTag
@Proteus
class     ExponentiallyWeightedMomentsEstimator extends StreamTransformer[ExponentiallyWeightedMomentsEstimator ] {
  import ExponentiallyWeightedMomentsEstimator ._

  /**
    * The total number of features on which the mean and the variance
    * are calculated.
    * @param n
    */
  def setFeaturesCount(n: Int): ExponentiallyWeightedMomentsEstimator  = {
    parameters.add(FeaturesCount, n)
    this
  }
  /**
    * Alpha  of the standard formulat for exponentially weighted moving average
    * @param a
    */
  def setFeaturesAlpha(a: Double): ExponentiallyWeightedMomentsEstimator  = {
    if(a>0&&a<1) parameters.add(Alpha, a)
    this
  }
}

object ExponentiallyWeightedMomentsEstimator  {

  // ====================================== Parameters =============================================

  case object FeaturesCount extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }
  case object Alpha extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.8)
  }



  // ====================================== Extra =============================================

  class ExponentiallyWeightedMoments(
                         private [moments] var counters: BreezeVector[Double],
                         private [moments] var currMean: BreezeVector[Double],
                         private [moments] var M2: BreezeVector[Double],
                         private [moments] var alpha:Double) {

    def this(x: BreezeVector[Double], slice: IndexedSeq[Int], n: Int,a:Double) = {
      this(BreezeVector.zeros[Double](n), BreezeVector.zeros[Double](n), BreezeVector.zeros[Double](n),a)
     process(x,slice)
    }

    def process(x: BreezeVector[Double], slice: IndexedSeq[Int]): this.type = {
      counters(slice) :+= 1.0
      val oldmean=currMean(slice).copy
      currMean(slice)*= alpha
      currMean(slice):+=(1-alpha)*x
      M2(slice)*=(1-alpha)
      M2(slice) :+= alpha*((x:-oldmean):*(x:-currMean(slice)))
      this
    }

    def counter(id: Int): Double = {
      counters(id)
    }

    def mean: BreezeVector[Double] = {
      currMean
    }

    def variance: BreezeVector[Double] = {
      M2
    }

     def copy(): ExponentiallyWeightedMoments = {
      new ExponentiallyWeightedMoments(counters.copy, currMean.copy, M2.copy,alpha)
    }

    override def toString: String = {
      "[counter=" + counters.toString + ",mean=" + mean.toString + ",variance=" + variance.toString + "]"
    }
  }

  // ==================================== Factory methods ==========================================

  def apply(): ExponentiallyWeightedMomentsEstimator = {
    new ExponentiallyWeightedMomentsEstimator()
  }

  // ==================================== Operations ==========================================

  implicit def fitNoOp[T] = {
    new StreamFitOperation[ExponentiallyWeightedMomentsEstimator, T]{
      override def fit(
                        instance: ExponentiallyWeightedMomentsEstimator,
                        fitParameters: ParameterMap,
                        input: DataStream[T])
      : Unit = {}
    }
  }

  implicit def transformExponentiallyWeightedMomentsEstimator[E <: StreamEvent: TypeInformation : ClassTag] = {
    new TransformDataStreamOperation[ExponentiallyWeightedMomentsEstimator, E, (Int, ExponentiallyWeightedMoments)]{
      override def transformDataStream(
                                        instance: ExponentiallyWeightedMomentsEstimator,
                                        transformParameters: ParameterMap,
                                        input: DataStream[E])
      : DataStream[(Int, ExponentiallyWeightedMoments)] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val featuresCount = resultingParameters(FeaturesCount)
        val alp=resultingParameters(Alpha)
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[E](input, resultingParameters.get(PartitioningOperation))

        statefulStream.mapWithState((in, state: Option[ExponentiallyWeightedMoments]) => {
          val (event, pid) = in
          val slice = event.slice
          val x = event.data.asBreeze.copy
          val metrics = state match {
            case Some(curr) => {
              curr.process(x, slice)
            }
            case None => {
              new ExponentiallyWeightedMoments(x, slice, featuresCount,alp)
            }
          }
          ((pid.toInt, metrics), Some(metrics))
        })
      }
    }
  }
}
