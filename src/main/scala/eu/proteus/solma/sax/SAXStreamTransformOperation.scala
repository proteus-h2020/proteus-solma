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

import eu.proteus.solma.pipeline.TransformDataStreamOperation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.apache.flink.util.Collector

class SAXStreamTransformOperation[T] extends TransformDataStreamOperation[SAX, T, String]{

  override def transformDataStream(
    instance: SAX,
    transformParameters: ParameterMap,
    input: DataStream[T]): DataStream[String] = {

    val avg = instance.trainingAvg
    val std = instance.trainingStd
    if(avg.isEmpty || std.isEmpty){
      throw new RuntimeException("You must train the SAX before calling transform")
    }

    val wordSize = instance.getWordSize()
    val paaFragmentSize = instance.getPAAFragmentSize()

    val avgNormWindowFunction = new AllWindowFunction[T, Double, GlobalWindow] {
      override def apply(window: GlobalWindow, input: Iterable[T], out: Collector[Double]): Unit = {

        val norm = input.map(in => {(in.toString.toDouble - instance.trainingAvg.get) / instance.trainingStd.get})

        val avg = norm.foldLeft(0.0)(_ + _) / norm.foldLeft(0)((acc, cur) => acc + 1)
        // println(s"Input: ${input.mkString(", ")} => ${norm.mkString(", ")} => PAA => ${avg}")
        out.collect(avg)
      }
    }

    val paaNorm = input.countWindowAll(paaFragmentSize).apply(avgNormWindowFunction)

    val cuts = instance.getAlphabetCuts()

    val saxTransformFunction = new AllWindowFunction[Double, String, GlobalWindow] {
      override def apply(window: GlobalWindow, input: Iterable[Double], out: Collector[String]): Unit = {
        val word = input.map(Cuts.findLetter(cuts, _)).mkString
        // println(s"Word: ${word}")
        out.collect(word)
      }
    }

    paaNorm.countWindowAll(wordSize).apply(saxTransformFunction)

  }

  /**
   * Normalize a number using the following expression:
   * {{{
   *   x_i' = (x_i - avg(X_training)) / std(X_training)
   * }}}
   * @param number The number to be normalized.
   * @param avg The average of the training set.
   * @param std The standard deviation of the training set.
   * @return A normalized value.
   */
  private def zNormalize(number: Double, avg: Double, std: Double) : Double = {
    println(s"zN(${number}) = " + (number - avg) / std)
    (number - avg) / std
  }

  private[sax] def obtainSymbol() : String = {
    "s"
  }

}

