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

import java.lang.Iterable

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.pipeline.FitOperation
import org.apache.flink.util.Collector

import scala.collection.JavaConversions

/**
 * Fit operation for the SAX dictionary.
 */
class SAXDictionaryFitOperation[T] extends FitOperation[SAXDictionary, T]{

  override def fit(
    instance: SAXDictionary,
    fitParameters: ParameterMap,
    input: DataSet[T]): Unit = {

    val wordBagReduceFunction = new GroupReduceFunction[(String, String), WordBag] {

      override def reduce(
        iterable: Iterable[(String, String)], collector: Collector[WordBag]): Unit = {
        var classId : Option[String] = None
        val wordBag = WordBag()
        JavaConversions.asScalaIterator(iterable.iterator()).foreach(
          x => {
            if(classId.isEmpty){
              classId = Some(x._2)
            }
            wordBag.addWord(x._1)
          }
        )
        wordBag.id = classId
        collector.collect(wordBag)
      }
    }

    val toDictionary = new GroupReduceFunction[WordBag, BagDictionary] {
      override def reduce(
        iterable: Iterable[WordBag], collector: Collector[BagDictionary]): Unit = {
        val dictionary = BagDictionary()
        JavaConversions.asScalaIterator(iterable.iterator())
          .foreach(wb => dictionary.bags.put(wb.id.get, wb))
        collector.collect(dictionary)
      }
    }

    val fitting = input.map(_ match {
      case (value : String, classId : String) => (value.toString, classId.toString)
      case (value : String, classId : Int) => (value.toString, classId.toString)
      case _ => throw new IllegalArgumentException("Expecting tuples of (String, String) or (String, Int)")
    }).groupBy(1)
      .reduceGroup[WordBag](wordBagReduceFunction)
      .reduceGroup[BagDictionary](toDictionary)

    val result = fitting.collect()
    if(result.size != 1){
      throw new RuntimeException("Expecting to receive a single dictionary")
    }

    if(instance.dictionary.isEmpty){
      instance.dictionary = Some(result.head)
    }else{
      instance.dictionary.get.bags.putAll(result.head.bags)
    }

    if(instance.dictionary.isEmpty){
      throw new RuntimeException("Unable to build class dictionary")
    }else{
      instance.dictionary.get.buildTFIDFMatrix()
    }

  }
}
