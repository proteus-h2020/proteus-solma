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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.apache.flink.util.Collector

/**
 * The SAX transform operation performs two operations for a given stream: PAA and SAX. For the
 * PAA algorithm, the stream is divided into windows of the user selected PAA size. On each
 * window, a Z-normalization operation is applied first:
 *
 * {{{
 *   x_i' = (x_i - avg(X_training)) / std(X_training)
 * }}}
 *
 * Then, we average the values of the window. With the averaged values, the algorithm converts
 * each value into a symbol of the alphabet. Several symbols are grouped as to form a word of
 * a user-selected size.
 *
 * @tparam T The type of the datastream.
 */
class SAXStreamTransformOperation[T <: Double]
  extends TransformDataStreamOperation[SAX, (T, Int), (String, Int)]{

  override def transformDataStream(
    instance: SAX,
    transformParameters: ParameterMap,
    input: DataStream[(T, Int)]): DataStream[(String, Int)] = {

    val avg = instance.trainingAvg
    val std = instance.trainingStd
    if(avg.isEmpty || std.isEmpty){
      throw new RuntimeException("You must train the SAX before calling transform")
    }

    val wordSize = instance.getWordSize()
    val paaFragmentSize = instance.getPAAFragmentSize()

    val partitionedStream : KeyedStream[(T, Int), Int] = SAX.toKeyedStream[T](input)

    // The PAA function averages n consecutive values to smooth the signal and reduce the number
    // of points in the SAX result.
    val paaFunction = new WindowFunction[(T, Int), (Double, Int), Int, GlobalWindow] {
      override def apply(
        key: Int,
        window: GlobalWindow,
        input: Iterable[(T, Int)], out: Collector[(Double, Int)]): Unit = {

        // Z-normalize the input data.
        val norm = input.map(in => {
          (in._1.toString.toDouble - instance.trainingAvg.get) / instance.trainingStd.get
        })

        // Average the values.
        val avg = norm.foldLeft(0.0)(_ + _) / norm.foldLeft(0)((acc, cur) => acc + 1)
        out.collect((avg, key))
      }
    }

    val paaNormPartioned : KeyedStream[(Double, Int), Int] = partitionedStream
      .countWindow(paaFragmentSize)
      .apply(paaFunction)
      .keyBy(r => r._2)

    val cuts = instance.getAlphabetCuts()

    val saxFunction = new WindowFunction[(Double, Int), (String, Int), Int, GlobalWindow] {
      override def apply(
        key: Int,
        window: GlobalWindow,
        input: Iterable[(Double, Int)],
        out: Collector[(String, Int)]): Unit = {

        val word = input.map(t => Cuts.findLetter(cuts,t._1)).mkString
        out.collect((word, key))
      }
    }

    paaNormPartioned.countWindow(wordSize).apply(saxFunction)

  }

}

