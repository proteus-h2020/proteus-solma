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
import org.apache.flink.ml.common.{ParameterMap, WithParameters}
import org.apache.flink.streaming.api.scala._

import scala.reflect.runtime.universe._

trait StreamEstimator[Self] extends WithParameters with Serializable {
  that: Self =>

  /** Fits the estimator to the given input data. The fitting logic is contained in the
    * [[StreamFitOperation]]. The computed state will be stored in the implementing class.
    *
    * @param training Training data stream
    * @param fitParameters Additional parameters for the [[StreamFitOperation]]
    * @param fitOperation [[StreamFitOperation]] which encapsulates the algorithm logic
    * @tparam Training Type of the training data
    * @return
    */
  def fit[Training](
      training: DataStream[Training],
      fitParameters: ParameterMap = ParameterMap.Empty)(implicit
      fitOperation: StreamFitOperation[Self, Training]): Unit = {
    FlinkSolmaUtils.registerFlinkMLTypes(training.executionEnvironment)
    fitOperation.fit(this, fitParameters, training)
  }
}

object StreamEstimator {

  implicit def fallbackFitOperation[
      Self: TypeTag,
      Training: TypeTag]
    : StreamFitOperation[Self, Training] = {
    new StreamFitOperation[Self, Training]{
      override def fit(
          instance: Self,
          fitParameters: ParameterMap,
          input: DataStream[Training])
        : Unit = {
        val self = typeOf[Self]
        val training = typeOf[Training]

        throw new RuntimeException("There is no StreamFitOperation defined for " + self +
          " which trains on a DataStream[" + training + "]")
      }
    }
  }

  implicit def fallbackTransformOperation[
      Self: TypeTag,
      IN: TypeTag]
    : TransformDataStreamOperation[Self, IN, Any] = {
    new TransformDataStreamOperation[Self, IN, Any] {
      override def transformDataStream(
        instance: Self,
        transformParameters: ParameterMap,
        input: DataStream[IN])
      : DataStream[Any] = {
        val self = typeOf[Self]
        val in = typeOf[IN]

        throw new RuntimeException("There is no StreamTransformOperation defined for " +
          self +  " which takes a DataStream[" + in +
          "] as input.")
      }
    }
  }

  implicit def fallbackPredictOperation[
      Self: TypeTag,
      Testing: TypeTag]
    : PredictDataStreamOperation[Self, Testing, Any] = {
    new PredictDataStreamOperation[Self, Testing, Any] {
      override def predictDataStream(
          instance: Self,
          predictParameters: ParameterMap,
          input: DataStream[Testing])
        : DataStream[Any] = {
        val self = typeOf[Self]
        val testing = typeOf[Testing]

        throw new RuntimeException("There is no StreamPredictOperation defined for " + self +
          " which takes a DataStream[" + testing + "] as input.")
      }
    }
  }
}

trait StreamFitOperation[Self, Training]{
  def fit(instance: Self, fitParameters: ParameterMap, input: DataStream[Training]): Unit
}
