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

import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.flink.api.scala.createTypeInformation

/**
 * Tests for the SAX dictionary
 */
class SAXDictionaryITSuite extends FunSuite with Matchers with FlinkTestBase {

  test("Single class fit"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData : Seq[String] = List("aa", "bb", "cc", "aa", "bb", "cc")
    val dataset : DataSet[String] = env.fromCollection(trainingData)
    val toFit = dataset.map((_, 1))
    val saxDictionary = new SAXDictionary()
    saxDictionary.fit(toFit)
    val fitted = saxDictionary.getDictionary()
    assert(fitted.isDefined, "Expected fitted dictionary")
    assertResult(1, "Invalid number of classes")(fitted.get.bags.size())
  }

  test("Two classes fit"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData : Seq[String] = List("aa", "bb", "cc", "aa", "bb", "cc")
    val dataset : DataSet[String] = env.fromCollection(trainingData)
    val toFit1 = dataset.map((_, 1))
    val toFit2 = dataset.map((_, 2))
    val saxDictionary = new SAXDictionary()
    saxDictionary.fit(toFit1)
    saxDictionary.fit(toFit2)
    val fitted = saxDictionary.getDictionary()
    assert(fitted.isDefined, "Expected fitted dictionary")
    assertResult(2, "Invalid number of classes")(fitted.get.bags.size())
  }

  test("Single class prediction"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData : Seq[String] = List("aa", "bb", "cc")
    val dataset : DataSet[String] = env.fromCollection(trainingData)
    val toFit = dataset.map((_, "A"))
    val saxDictionary = new SAXDictionary()
    saxDictionary.fit(toFit)
    val fitted = saxDictionary.getDictionary()
    assert(fitted.isDefined, "Expected fitted dictionary")

    val evalData : Seq[(String, Int)] = List(
      ("aa", 0), ("bb", 0), ("cc", 0),
      ("aa", 0), ("bb", 0), ("dd", 0),
      ("aa", 0), ("dd", 0), ("dd", 0),
      ("dd", 0), ("dd", 0), ("dd", 0)
    )
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamingEnv.setParallelism(4)
    streamingEnv.setMaxParallelism(4)
    val evalDataSet : DataStream[(String, Int)] = streamingEnv.fromCollection(evalData)
    val evalDictionary = saxDictionary
      .setNumberWords(saxDictionary.getRecommendedNumberWords().getOrElse(3))

    val predictions = evalDictionary.predict[(String, Int), SAXPrediction](evalDataSet)
    val r : Iterator[SAXPrediction] = predictions.collect()
    val result = r.toList
    assertResult(4, "Invalid number of predictions")(result.size)
    assertResult(1.0, "Expecting perfect similarity")(result.head.similarity)
    assertResult(0.0, "Expecting 0 similarity")(result.last.similarity)
  }

  test("Two class prediction"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData1 : Seq[String] = List("aa", "bb", "cc")
    val dataset1 : DataSet[String] = env.fromCollection(trainingData1)
    val toFit1 = dataset1.map((_, "A"))
    val trainingData2 : Seq[String] = List("dd", "ee", "ff")
    val dataset2 : DataSet[String] = env.fromCollection(trainingData2)
    val toFit2 = dataset2.map((_, "B"))
    val saxDictionary = new SAXDictionary()
    saxDictionary.fit(toFit1)
    saxDictionary.fit(toFit2)
    val fitted = saxDictionary.getDictionary()
    assert(fitted.isDefined, "Expected fitted dictionary")

    val evalData : Seq[(String, Int)] = List(
      ("aa", 0), ("bb", 0), ("cc", 0),
      ("aa", 0), ("bb", 0), ("dd", 0),
      ("aa", 0), ("dd", 0), ("ee", 0),
      ("dd", 0), ("ee", 0), ("ff", 0)
    )
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamingEnv.setParallelism(4)
    streamingEnv.setMaxParallelism(4)
    val evalDataSet : DataStream[(String, Int)] = streamingEnv.fromCollection(evalData)
    val evalDictionary = saxDictionary.setNumberWords(3)

    val predictions = evalDictionary.predict[(String, Int), SAXPrediction](evalDataSet)
    val r : Iterator[SAXPrediction] = predictions.collect()
    val result = r.toList
    assertResult(4, "Invalid number of predictions")(result.size)
    assertResult(1.0, "Expecting perfect similarity")(result.head.similarity)
    assertResult("A", "Expecting match on class A")(result.head.classId)
    assertResult(1.0, "Expecting perfect similarity")(result.last.similarity)
    assertResult("B", "Expecting match on class B")(result.last.classId)

  }

}
