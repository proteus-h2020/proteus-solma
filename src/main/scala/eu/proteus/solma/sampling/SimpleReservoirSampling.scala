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

package eu.proteus.solma.sampling

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.math.Vector
import SimpleReservoirSampling.ReservoirSize
import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.XORShiftRandom

import scala.collection.mutable
import scala.reflect.ClassTag

@Proteus
class SimpleReservoirSampling extends StreamTransformer[SimpleReservoirSampling] {

  def setReservoirSize(size: Int): SimpleReservoirSampling = {
    parameters.add(ReservoirSize, size)
    this
  }

}

object SimpleReservoirSampling {


  // ====================================== Parameters =============================================

  case object ReservoirSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(10)
  }


  // ==================================== Factory methods ==========================================

  def apply(): SimpleReservoirSampling = {
    new SimpleReservoirSampling()
  }

  // ==================================== Operations ==========================================


  implicit def fitNoOp[T] = {
    new StreamFitOperation[SimpleReservoirSampling, T]{
      override def fit(
          instance: SimpleReservoirSampling,
          fitParameters: ParameterMap,
          input: DataStream[T])
        : Unit = {}
    }
  }

  implicit def treansformSimpleReservoirSampling[T <: Vector : TypeInformation : ClassTag] = {
    new TransformDataStreamOperation[SimpleReservoirSampling, T, T]{
      override def transformDataStream(
        instance: SimpleReservoirSampling,
        transformParameters: ParameterMap,
        input: DataStream[T])
        : DataStream[T] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[T](input)
        val k = resultingParameters(ReservoirSize)
        val gen = new XORShiftRandom()
        implicit val typeInfo = TypeInformation.of(classOf[(Long, Array[T])])
        statefulStream.flatMapWithState((in, state: Option[(Long, Array[T])]) => {
          val (element, _) = in
          state match {
            case Some(curr) => {
              val (streamCounter, reservoir) = curr
              val data = new mutable.ListBuffer[T]()
              if (streamCounter < k) {
                reservoir(streamCounter.toInt) = element
                data += element
              } else {
                val j = gen.nextInt(streamCounter.toInt + 1)
                if (j < k) {
                  reservoir(j) = element
                  data += element
                }
              }
              (data, Some((streamCounter + 1, reservoir)))
            }
            case None => {
              val reservoir = Array.ofDim[T](k)
              reservoir(0) = element
              (Seq(element), Some((1L, reservoir)))
            }
          }
        })

      }
    }
  }

}
