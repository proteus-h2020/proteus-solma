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
import breeze.linalg.{Vector => BreezeVector}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{Vector => BreezeVector}
import eu.proteus.solma.events.StreamEvent
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic

import scala.collection.mutable.ArrayBuffer
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
class MovingMomentsEstimator extends StreamTransformer[MovingMomentsEstimator] {
  import MovingMomentsEstimator._

  /**
    * Enable the aggregation of all the parallel moments, which
    * will be sent downstream to the next operation.
    * @param enabled
    */
  def enableAggregation(enabled: Boolean): MovingMomentsEstimator = {
    parameters.add(AggregateMoments, enabled)
    this
  }
  def setWindowLength(length: Long): MovingMomentsEstimator = {
    parameters.add(WindowLength, length)
    this
  }
  def setWindowTriggerInterval(triggerInterval: Long): MovingMomentsEstimator = {
    parameters.add(WindowTriggerInterval, triggerInterval)
    this
  }

}

object MovingMomentsEstimator {

  // ====================================== Parameters =============================================

  case object AggregateMoments extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(false)
  }


  case object WindowLength extends Parameter[Long] {
    override val defaultValue: Option[Long] = Some(30)

  }
  case object WindowTriggerInterval extends Parameter[Long] {
    override val defaultValue: Option[Long] = Some(10)

  }


  // ====================================== Extra =============================================

  case class Moment[T]( mean:T, variance:T)

  // ==================================== Factory methods ==========================================

  def apply(): MovingMomentsEstimator = {
    new MovingMomentsEstimator()
  }

  // ==================================== Operations ==========================================

  implicit def fitNoOp[E] = {
    new StreamFitOperation[MovingMomentsEstimator, E]{
      override def fit(
                        instance: MovingMomentsEstimator,
                        fitParameters: ParameterMap,
                        input: DataStream[E])
      : Unit = {}
    }
  }

  implicit def transformMovingMomentsEstimator[E <: StreamEvent: TypeInformation : ClassTag] = {
    new TransformDataStreamOperation[MovingMomentsEstimator, E, Moment[BreezeVector[Double] ]]{
      override def transformDataStream(
                                        instance: MovingMomentsEstimator,
                                        transformParameters: ParameterMap,
                                        input: DataStream[E])
      : DataStream[Moment[BreezeVector[Double] ]] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val kstream = FlinkSolmaUtils.ensureKeyedStream[E](input, resultingParameters.get(PartitioningOperation))
        val windowLength = resultingParameters(WindowLength)
        val windowTriggerInterval=resultingParameters(WindowTriggerInterval)
        val aggregateMoments=resultingParameters(AggregateMoments)

        val intermidiate=kstream.timeWindow(Time.milliseconds(windowLength),Time.milliseconds(windowTriggerInterval))
          .aggregate(new AggregateFunction[(E, Long), ArrayBuffer[BreezeVector[Double]], Moment[BreezeVector[Double]]]
          (){override def add(value: (E, Long),  accumulator: ArrayBuffer[BreezeVector[Double]])
            : ArrayBuffer[BreezeVector[Double]] ={accumulator+=value._1.data.asBreeze}

            override def createAccumulator(): ArrayBuffer[BreezeVector[Double] ] = ArrayBuffer[BreezeVector[Double] ]()

            override def getResult(accumulator:ArrayBuffer[BreezeVector[Double] ]): Moment[BreezeVector[Double] ] ={
              val nb:Int=accumulator.length
              val nb_feature=accumulator(0).size
               val mean=accumulator.reduce((a,b)=>{a:+b}).map(i=>i/nb)
              val variance=accumulator.fold(BreezeVector.zeros[Double](nb_feature))((acc:BreezeVector[Double],in)=>{
                                                                          acc:+=(in:-mean):*(in:-mean)}).map(i=>i/nb)
              Moment[BreezeVector[Double] ](mean,variance)

            }
          override def merge(a: ArrayBuffer[BreezeVector[Double] ] , b: ArrayBuffer[BreezeVector[Double] ] )
                                                                  : ArrayBuffer[BreezeVector[Double] ]  = ???
        })

        /*if(aggregateMoments)
          {

            // this code is not yet implemented

           intermidiate.timeWindowAll(Time.milliseconds(windowLength),Time.milliseconds(windowTriggerInterval))
                                                  .aggregate(new AggregateFunction[Moment[BreezeVector[Double]],
                                 ArrayBuffer[Moment[BreezeVector[Double]]], Moment[BreezeVector[Double] ] ]() {

              override def add(value:(Moment[BreezeVector[Double]]),  accumulator
              : ArrayBuffer[Moment[BreezeVector[Double]]]): Unit= accumulator+=value

              override def createAccumulator(): ArrayBuffer[Moment[BreezeVector[Double]]] =
                                                 ArrayBuffer[Moment[BreezeVector[Double]]]()

              override def getResult(accumulator:ArrayBuffer[Moment[BreezeVector[Double]]])
                                                          : Moment[BreezeVector[Double] ]= {
               val nb:Int=accumulator.length
               val resu=accumulator.reduce((a,b)=>{Moment[BreezeVector[Double]](a.mean:+b.mean,a.variance:+b.variance)})
               Moment[BreezeVector[Double]](resu.mean.map(c1=>c1/nb),resu.variance.map(c2=>c2/nb))


              }

              override def merge(a: ArrayBuffer[Moment[BreezeVector[Double]] ] ,
                                 b: ArrayBuffer[Moment[BreezeVector[Double]]] )
                                  : ArrayBuffer[Moment[BreezeVector[Double]] ]  = ???
            })

          }
        else {*/
          intermidiate
        //}
        }
    }
  }
}
