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

package eu.proteus.solma.lasso

import eu.proteus.solma.lasso.Lasso.{LassoParam, OptionLabeledVector}
import eu.proteus.solma.lasso.algorithm.LassoBasicAlgorithm
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import eu.proteus.solma.events.StreamEvent
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import breeze.linalg.DenseVector


class LassoStreamTransformOperation[T <: StreamEvent]
    extends TransformDataStreamOperation[Lasso, StreamEvent, Option[Double]]{

    override def transformDataStream(instance: Lasso,
                                     transformParameters: ParameterMap,
                                     input: DataStream[StreamEvent]): DataStream[Option[Double]] = {
      def eventToLabelVector(event: StreamEvent): OptionLabeledVector = {
        Right(new DenseVector(event.data.toArray.map(x => x._2)))
      }

      val transSource = input.map(x => eventToLabelVector(x))
      val output = LassoParameterServer.transformLasso(None)(transSource, workerParallelism = 3,
        psParallelism = 3, lassoMethod = LassoBasicAlgorithm.buildLasso(), pullLimit = 10000,
        featureCount = 500/*LassoParameterServerTest.featureCount*/, rangePartitioning = true, iterationWaitTime = 20000
      )

      def f (x: Either[Double, (Int, LassoParam)]): Option[Double] = {
        x match {
          case Left(label) => Some(label)
          case _ => None
        }
      }

      output.map(x => f(x))
    }

  }
