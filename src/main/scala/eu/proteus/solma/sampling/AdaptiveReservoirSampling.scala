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

package  eu.proteus.solma.sampling
import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation
import eu.proteus.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.math.Vector
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.XORShiftRandom
import org.apache.flink.streaming.api.scala.createTypeInformation
import scala.annotation.tailrec

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random




@Proteus
class AdaptiveReservoirSampling extends  StreamTransformer[AdaptiveReservoirSampling]{
  import AdaptiveReservoirSampling._
  def setAdaptiveReservoirSize(size: Int): AdaptiveReservoirSampling = {
    parameters.add(AdaptiveReservoirSize, size)
    this
  }

  def setAdaptiveReservoirNumberToChange(NumberToChange: Int): AdaptiveReservoirSampling = {
    parameters.add(AdaptiveReservoirNumberToChange, NumberToChange)
    this
  }

  def setAdaptiveReservoirThreshold(Threshold: Double): AdaptiveReservoirSampling = {
    parameters.add(AdaptiveReservoirThreshold, Threshold)
    this
  }

  def setAdaptiveReservoirNewSize(Ch: Int): AdaptiveReservoirSampling = {
    parameters.add(Change, Ch)
    this
  }
}



object AdaptiveReservoirSampling{

  // ====================================== Parameters =============================================

  case object AdaptiveReservoirSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(4)
  }
  case object AdaptiveReservoirNumberToChange extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(2)
  }
  case object AdaptiveReservoirThreshold extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.99)
  }
  case object Change extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(5)
  }

  //=====================================Extra=================================================


  def factorial(n: Int): Int={ @tailrec def factorial2(n:Int,result:Int):Int = if (n==0) result
  else factorial2(n-1,n*result)
  factorial2(n,1)}
  def minm (i: Int,reservoirSize: Int,delta: Int,threshold: Double): Int = {
    var m = delta
    var UC = 0.0
    var x = reservoirSize
    var f1 = factorial(i)*factorial(m)*factorial(reservoirSize +delta)*factorial(i +m-reservoirSize-delta)
    var f2 = factorial(x)*factorial(i-x)*factorial(reservoirSize +delta-x)*factorial(m-reservoirSize-delta +x)*
                                                                                               factorial(i +m)
    UC = UC + f1/f2
    while(UC <= threshold){
      m += 1
      UC = 0.0
      for (x <- math.max(0,reservoirSize +delta-m) to reservoirSize){
        f1 = factorial(i)*factorial(m)*factorial(reservoirSize +delta)*factorial(i +m-reservoirSize-delta)
        f2 = factorial(x)*factorial(i-x)*factorial(reservoirSize +delta-x)*factorial(m-reservoirSize-delta +x)*
                                                                                               factorial(i +m)
        UC = UC + f1/f2
      }
    }
    m
  }
  case class Inp[T](var streamCounter1:Long,var streamCounter2:Long, var streamCounter3:Long,
                    var adaptedReserS1:mutable.ArrayBuffer[T], var adaptedReserS2:mutable.ArrayBuffer[T],
                    var adaptivereservoir:mutable.ArrayBuffer[T])

  // ==================================== Factory methods ==========================================

  def apply(): AdaptiveReservoirSampling = {
    new AdaptiveReservoirSampling()
  }

  // ==================================== Operations ==========================================


  implicit def fitNoOp[T] = {
    new StreamFitOperation[AdaptiveReservoirSampling, T]{
      override def fit(
                        instance: AdaptiveReservoirSampling,
                        fitParameters: ParameterMap,
                        input: DataStream[T])
      : Unit = {}
    }
  }


  implicit def treansformAdaptiveReservoirSampling[T <: Vector : TypeInformation : ClassTag] = {
    new TransformDataStreamOperation[AdaptiveReservoirSampling, T, mutable.ArrayBuffer[T]] {
      override def transformDataStream(
                                        instance: AdaptiveReservoirSampling,
                                        transformParameters: ParameterMap,
                                        input: DataStream[T])
      : DataStream[mutable.ArrayBuffer[T]] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[T](input, resultingParameters.get(PartitioningOperation))
        var k = resultingParameters(AdaptiveReservoirSize)
        val nboCh=resultingParameters(AdaptiveReservoirNumberToChange)
        val thresh=resultingParameters(AdaptiveReservoirThreshold)
        val changedReservoirSize = resultingParameters(Change)
        val gen = new XORShiftRandom()
        implicit val typeInfo = createTypeInformation[Inp[T]]
        implicit val retInfo = createTypeInformation[mutable.ArrayBuffer[T]]
        var j : Int = 0
        var m : Int = 0
        var x : Int = 0
        var remainingSize = 0
        statefulStream.flatMapWithState((in, state: Option[Inp[T]]) => {
          val (element,_)=in
          state match {
            case Some(curr) => {
              val mini_reservoir = curr
              var ret: Seq[mutable.ArrayBuffer[T]] = Seq()
              if (mini_reservoir.streamCounter1 < k) {
                mini_reservoir.adaptivereservoir(mini_reservoir.streamCounter1.toInt) = element
                mini_reservoir.streamCounter1 += 1
                mini_reservoir.streamCounter2 +=1}
              else if (mini_reservoir.streamCounter1 == nboCh*k)
                {
                  if (mini_reservoir.streamCounter1==mini_reservoir.streamCounter2)
                  {
                    val  delta = changedReservoirSize - k
                    if (delta <= 0) {
                      mini_reservoir.adaptivereservoir =  Random.shuffle(mini_reservoir.adaptivereservoir.toList)
                                                              .take(changedReservoirSize).to[mutable.ArrayBuffer]
                      mini_reservoir.streamCounter1 +=1
                    }else{
                      m = minm(mini_reservoir.streamCounter1.toInt,k,delta,thresh)
                      x = changedReservoirSize - m + gen.nextInt(m-delta + 1)
                      remainingSize = changedReservoirSize - x
                      mini_reservoir.adaptedReserS1 = Random.shuffle(mini_reservoir.adaptivereservoir).take(x)
                      mini_reservoir.adaptedReserS2 = mutable.ArrayBuffer.fill[T](remainingSize)(null.asInstanceOf[T])
                      mini_reservoir.adaptedReserS2(0) = element
                    }
                    mini_reservoir.streamCounter2 +=1
                  }else{
                    if (mini_reservoir.streamCounter2 < mini_reservoir.streamCounter1 + remainingSize){
                      mini_reservoir.adaptedReserS2(mini_reservoir.streamCounter3.toInt) = element
                      mini_reservoir.streamCounter3 += 1
                      mini_reservoir.streamCounter2 +=1
                    } else {
                      j = gen.nextInt(m)
                      if (j < remainingSize){
                        mini_reservoir.adaptedReserS2(j)  = element
                        mini_reservoir.streamCounter2 +=1}
                    }

                    mini_reservoir.adaptivereservoir = mini_reservoir.adaptedReserS1 ++ mini_reservoir.adaptedReserS2
                  
                    mini_reservoir.streamCounter1 += m*((mini_reservoir.streamCounter2==mini_reservoir.streamCounter1
                                                                                                  +m).compare(true) +1)
                  }
                  k = changedReservoirSize
                }else {
                  j = gen.nextInt(mini_reservoir.streamCounter1.toInt)
                  if ((j < k)&&(mini_reservoir.adaptivereservoir.length<=j))
                     {
                      mini_reservoir.adaptivereservoir=mini_reservoir.adaptivereservoir ++mutable.ArrayBuffer.fill[T](
                                                    j-mini_reservoir.adaptivereservoir.length +1)(null.asInstanceOf[T])
                      mini_reservoir.adaptivereservoir(j) = element
                    }
                mini_reservoir.streamCounter1 += 1
                mini_reservoir.streamCounter2 +=1
                }
              ret = Seq(mini_reservoir.adaptivereservoir.filter(i=>i != null))
              (ret, Some(mini_reservoir))
              }
            case None => {
              var adaptivereservoir = mutable.ArrayBuffer.fill[T](k)(null.asInstanceOf[T])
              var adaptedReserS1 = mutable.ArrayBuffer.fill[T](0)(null.asInstanceOf[T])
              var adaptedReserS2 = mutable.ArrayBuffer.fill[T](0)(null.asInstanceOf[T])
              adaptivereservoir(0)=element
              (Seq(adaptivereservoir.slice(0,1)),Some(Inp(1L,1L,1L,adaptedReserS1,adaptedReserS2,adaptivereservoir)))
            }
          }
        }
        )
      }
    }
  }
}

