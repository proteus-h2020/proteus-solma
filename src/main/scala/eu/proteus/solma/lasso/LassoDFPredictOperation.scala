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

import eu.proteus.solma.lasso.Lasso.OptionLabeledVector
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import eu.proteus.solma.lasso.algorithm.LassoBasicAlgorithm
import eu.proteus.solma.pipeline.PredictDataStreamOperation
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream

class LassoDFPredictOperation[T <: LassoStreamEvent](workerParallelism: Int, psParallelism: Int,
                                                     pullLimit: Int, featureCount: Int,
                                                     rangePartitioning: Boolean, iterationWaitTime: Long,
                                                     allowedLateness: Long)
  extends PredictDataStreamOperation[LassoDelayedFeedbacks, LassoStreamEvent, Option[((Long, Double), Double)]]{

  /** Calculates the predictions for all elements in the [[DataStream]] input
    *
    * @param instance          The Predictor instance that we will use to make the predictions
    * @param predictParameters The parameters for the prediction
    * @param input             The DataStream containing the unlabeled examples
    * @return
    */
  override def predictDataStream(instance: LassoDelayedFeedbacks,
                                 predictParameters: ParameterMap,
                                 input: DataStream[LassoStreamEvent]): DataStream[Option[((Long, Double), Double)]] = {

    val lassoMethod = LassoBasicAlgorithm.buildLasso()

    val processedInput: DataStream[OptionLabeledVector] = input.map{
      x => x match {
        case Left(ev) => Some(Right(ev.pos, ev.data.asBreeze))
        case Right(ev) => None
      }
    }.filter(x => x.isDefined).map(x => x.get)


    val lassoResults = LassoParameterServer.transformLasso(None)(processedInput, workerParallelism, psParallelism,
      lassoMethod, pullLimit, featureCount, rangePartitioning, iterationWaitTime)

    lassoResults.map{
      x =>
        x match {
          case Left(ev) => Some(ev)
          case Right(ev) => None
        }
    }

  }


}
