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

import breeze.linalg.{Vector => BreezeVector}
import java.util.concurrent.TimeUnit

import eu.proteus.solma.events.StreamEvent
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation
import eu.proteus.solma.pipeline.{StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.Vector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

class RollingMean extends StreamTransformer[RollingMean] {



}

object RollingMean {

  case object WindowLength extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(30)
  }

  implicit def transformRollingMean[E <: StreamEvent] = {
    new TransformDataStreamOperation[MomentsEstimator, E, Vector]{
      override def transformDataStream(
          instance: MomentsEstimator,
          transformParameters: ParameterMap,
          input: DataStream[E]
      ): DataStream[Vector] = {

        val resultingParameters = instance.parameters ++ transformParameters

        val kstream = FlinkSolmaUtils.ensureKeyedStream[E](input, resultingParameters.get(PartitioningOperation))

        val windowLength = resultingParameters(WindowLength)
        val time = Time.seconds(windowLength)

        val windowAssigner = kstream.executionEnvironment.getStreamTimeCharacteristic match {
          case TimeCharacteristic.EventTime => TumblingEventTimeWindows.of(time)
          case TimeCharacteristic.ProcessingTime => TumblingProcessingTimeWindows.of(time)
        }

        kstream
          .window(windowAssigner)
          .aggregate(new AggregateFunction[(E, Int), BreezeVector[Double], Vector]() {

            override def add(value: (E, Int), accumulator: BreezeVector[Double]): Unit = ???

            override def createAccumulator(): BreezeVector[Double] = ???

            override def getResult(accumulator: BreezeVector[Double]): Vector = ???

            override def merge(a: BreezeVector[Double], b: BreezeVector[Double]): BreezeVector[Double] = ???
          })


        null
      }
    }
  }


}
