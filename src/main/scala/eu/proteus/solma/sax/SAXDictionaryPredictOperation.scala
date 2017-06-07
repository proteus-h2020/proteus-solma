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

import java.util.{HashMap => JHashMap}

import eu.proteus.solma.pipeline.PredictDataStreamOperation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * Predict operation using the dictionary.
 */
class SAXDictionaryPredictOperation[T <: String]
  extends PredictDataStreamOperation[SAXDictionary, (T, Int), SAXPrediction]{

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
    input: DataStream[(T, Int)]): DataStream[SAXPrediction] = {

    val windowSize = instance.getNumberWords()
    val partitionedStream : KeyedStream[(T, Int), Int] = SAX.toKeyedStream[T](input)

    val predictFunction = new WindowFunction[(T, Int), SAXPrediction, Int, GlobalWindow] {
      override def apply(
        key: Int,
        window: GlobalWindow,
        input: Iterable[(T, Int)],
        out: Collector[SAXPrediction]): Unit = {

        val freq = new JHashMap[String, Long]
        input.foreach(w => {
          freq.putIfAbsent(w._1, 0)
          val previous = freq.get(w._1)
          freq.put(w._1, previous + 1)
        })

        val prediction = instance.dictionary.get.predict(key, freq)
        out.collect(prediction)
      }
    }
    partitionedStream.countWindow(windowSize).apply(predictFunction)

  }


}
