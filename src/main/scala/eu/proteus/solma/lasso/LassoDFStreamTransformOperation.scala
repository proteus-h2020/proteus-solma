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

import eu.proteus.solma.lasso.Lasso.LassoParam
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import eu.proteus.solma.lasso.algorithm.LassoParameterInitializer.initConcrete
import eu.proteus.solma.lasso.algorithm.LassoBasicAlgorithm
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream


class LassoDFStreamTransformOperation[T <: LassoStreamEvent](workerParallelism: Int, psParallelism: Int,
                                                             pullLimit: Int, featureCount: Int,
                                                             rangePartitioning: Boolean, iterationWaitTime: Long,
                                                             allowedLateness: Long)
    extends TransformDataStreamOperation[LassoDelayedFeedbacks, LassoStreamEvent, Either[((Long, Double), Double),
      (Int, LassoParam)]]{

    override def transformDataStream(instance: LassoDelayedFeedbacks,
                                     transformParameters: ParameterMap,
                                     rawInput: DataStream[LassoStreamEvent]):
    DataStream[Either[((Long, Double), Double), (Int, LassoParam)]] = {

      val workerLogic: LassoWorkerLogic = new LassoWorkerLogic(
        new LassoModelBuilder(initConcrete(1.0, 0.0, featureCount)(0)), LassoBasicAlgorithm.buildLasso())

      val output = LassoParameterServer.transformLasso(None)(rawInput, workerLogic, workerParallelism,
        psParallelism, lassoMethod = LassoBasicAlgorithm.buildLasso(), pullLimit, featureCount, rangePartitioning,
        iterationWaitTime)

      output
    }

  }
