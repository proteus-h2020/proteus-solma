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

package eu.proteus.solma.sax

import java.util.concurrent.atomic.AtomicLong
import java.util.{HashMap => JHashMap}

import eu.proteus.solma.pipeline.PredictDataStreamOperation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * Predict operation using the dictionary.
 */
class SAXDictionaryPredictOperation[T] extends PredictDataStreamOperation[SAXDictionary, T, SAXPrediction]{

  /** Calculates the predictions for all elements in the [[DataStream]] input
   *
   * @param instance          The Predictor instance that we will use to make the predictions
   * @param predictParameters The parameters for the prediction
   * @param input             The DataSet containing the unlabeled examples
   * @return
   */
  override def predictDataStream(
    instance: SAXDictionary,
    predictParameters: ParameterMap,
    input: DataStream[T]): DataStream[SAXPrediction] = {

    val windowSize = instance.getNumberWords()

    val predictWindowFunction = new AllWindowFunction[T, SAXPrediction, GlobalWindow] {
      override def apply(
        window: GlobalWindow,
        input: Iterable[T],
        out: Collector[SAXPrediction]): Unit = {

        val freq = new JHashMap[String, AtomicLong]
        input.foreach(w => {
          freq.putIfAbsent(w.toString, new AtomicLong(0))
          freq.get(w.toString).incrementAndGet()
        })

        val prediction = instance.dictionary.get.predict(freq)
        out.collect(prediction)
      }
    }

    input.countWindowAll(windowSize).apply(predictWindowFunction)

  }
}
