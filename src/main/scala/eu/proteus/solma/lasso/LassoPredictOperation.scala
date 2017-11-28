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

import eu.proteus.solma.pipeline.PredictDataStreamOperation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

class LassoPredictOperation[T <: Lasso.OptionLabeledVector]
  extends PredictDataStreamOperation[Lasso, (T, Int), Double]{

  /** Calculates the predictions for all elements in the [[DataStream]] input
    *
    * @param instance          The Predictor instance that we will use to make the predictions
    * @param predictParameters The parameters for the prediction
    * @param input             The DataSet containing the unlabeled examples
    * @return
    */
  override def predictDataStream(instance: Lasso,
                                 predictParameters: ParameterMap,
                                 input: DataStream[(T, Int)]): DataStream[Double] = {
    //Very naive prediction
    input.map(x => 0.0)
  }


}

