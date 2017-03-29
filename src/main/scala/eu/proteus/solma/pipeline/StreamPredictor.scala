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

package eu.proteus.solma.pipeline

import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala._

trait StreamPredictor[Self] extends StreamEstimator[Self] {
  that: Self =>

  def predict[Testing, Prediction](
      testing: DataStream[Testing],
      predictParameters: ParameterMap = ParameterMap.Empty)(implicit
      predictor: PredictDataStreamOperation[Self, Testing, Prediction])
    : DataStream[Prediction] = {
    FlinkSolmaUtils.registerFlinkMLTypes(testing.getExecutionEnvironment)
    predictor.predictDataStream(this, predictParameters, testing)
  }

}

object StreamPredictor {
  implicit def defaultPredictDataStreamOperation[
      Instance <: StreamEstimator[Instance],
      Model,
      Testing,
      PredictionValue](
      implicit predictOperation: StreamPredictOperation[Instance, Model, Testing, PredictionValue],
      testingTypeInformation: TypeInformation[Testing],
      predictionValueTypeInformation: TypeInformation[PredictionValue])
    : PredictDataStreamOperation[Instance, Testing, (Testing, PredictionValue)] = {
    new PredictDataStreamOperation[Instance, Testing, (Testing, PredictionValue)] {
      override def predictDataStream(
          instance: Instance,
          predictParameters: ParameterMap,
          input: DataStream[Testing])
        : DataStream[(Testing, PredictionValue)] = {
        val resultingParameters = instance.parameters ++ predictParameters
        val model = predictOperation.getModel(instance, resultingParameters)
        implicit val resultTypeInformation = createTypeInformation[(Testing, PredictionValue)]
        input.map(element => {
          (element, predictOperation.predict(element, model))
        })
      }
    }
  }
}

trait PredictDataStreamOperation[Self, Testing, Prediction] extends Serializable{

  /** Calculates the predictions for all elements in the [[DataStream]] input
    *
    * @param instance The Predictor instance that we will use to make the predictions
    * @param predictParameters The parameters for the prediction
    * @param input The DataSet containing the unlabeled examples
    * @return
    */
  def predictDataStream(
      instance: Self,
      predictParameters: ParameterMap,
      input: DataStream[Testing])
    : DataStream[Prediction]
}

trait StreamPredictOperation[Instance, Model, Testing, Prediction] extends Serializable{

  /** Defines how to retrieve the model of the type for which this operation was defined
    *
    * @param instance The Predictor instance that we will use to make the predictions
    * @param predictParameters The parameters for the prediction
    * @return A DataSet with the model representation as its only element
    */
  def getModel(instance: Instance, predictParameters: ParameterMap): Model

  /** Calculates the prediction for a single element given the model of the [[StreamPredictor]].
    *
    * @param value The unlabeled example on which we make the prediction
    * @param model The model representation of the prediciton algorithm
    * @return A label for the provided example of type [[Prediction]]
    */
  def predict(value: Testing, model: Model):
    Prediction
}
