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
import eu.proteus.solma.sax.SAX.fitImplementation
import eu.proteus.solma.sax.SAX.transformImplementation

object SAXITSuite {

  /**
   * Epsilon for double comparisons.
   */
  private val DoubleEps : Double = 0.0001
}

/**
 * SAX tests.
 */
class SAXITSuite extends FunSuite with Matchers with FlinkTestBase{

  /**
   * Calculate the average and standard deviation.
   * @param data The data.
   * @return A tuple with the average and standard deviation.
   */
  private def getStatistics(data : Seq[Double]) : (Double, Double) = {
    val sum = data.sum
    val size = data.size
    val avg = sum / size
    val sqDiff = data.map(x => (x - avg) * (x - avg)).sum
    val std = Math.sqrt(sqDiff / size)
    (avg, std)
  }

  /**
   * Basic fit tests.
   */
  test("Basic fit with 5 elements"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData : Seq[Double] = List(0.0d, 1.0d, 2.0d, 3.0d, 4.0d)
    val dataset : DataSet[Double] = env.fromCollection(trainingData)
    val sax = new SAX().setPAAFragmentSize(1).setWordSize(2)
    sax.fit(dataset)
    sax.printInternalParameters()
    val expected = this.getStatistics(trainingData)
    val fitted = sax.getFittedParameters()
    assert(fitted.isDefined, "Expected fitted parameters")
    assertResult(expected._1, "Invalid avg")(fitted.get._1)
    assertResult(expected._2, "Invalid std")(fitted.get._2)
  }

  test("Basic fit with 15 elements"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData : Seq[Double] = List(2, 3, 4, 8, 8, 9, 8, 6, 6, 5, 5, 5, 6, 2, 1)
    val dataset : DataSet[Double] = env.fromCollection(trainingData)
    val sax = new SAX()
    sax.fit(dataset)
    sax.printInternalParameters()
    val expected = this.getStatistics(trainingData)
    val fitted = sax.getFittedParameters()
    assert(fitted.isDefined, "Expected fitted parameters")
    assert(expected._1 === (fitted.get._1 +- SAXITSuite.DoubleEps), "Invalid avg")
    assert(expected._2 === (fitted.get._2 +- SAXITSuite.DoubleEps), "Invalid std")
  }

  test("Alphabet=2, PAA=3, WordSize=1"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    val trainingData : Seq[Double] = List(2, 3, 4, 8, 8, 9, 8, 6, 6, 5, 5, 5, 6, 2, 1)
    val trainingDataSet : DataSet[Double] = env.fromCollection(trainingData)
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamingEnv.setParallelism(4)
    streamingEnv.setMaxParallelism(4)
    val evalDataSet : DataStream[Double] = streamingEnv.fromCollection(trainingData)
    val sax = new SAX().setPAAFragmentSize(3).setWordSize(1)
    sax.fit(trainingDataSet)
    sax.printInternalParameters()
    val transformed = sax.transform(evalDataSet)
    val r : Iterator[String] = transformed.collect()
    val result = r.toList
    val expected : Seq[String] = List("a", "b", "b", "a", "a")
    assert(result === expected, "Result should match")
  }

  test("Alphabet=2, PAA=2, WordSize=2"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    val trainingData : Seq[Double] = List(2, 3, 4, 8, 8, 9, 8, 6, 6, 5, 5, 5, 6, 2, 1)
    val trainingDataSet : DataSet[Double] = env.fromCollection(trainingData)
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamingEnv.setParallelism(4)
    streamingEnv.setMaxParallelism(4)
    val evalDataSet : DataStream[Double] = streamingEnv.fromCollection(trainingData)
    val sax = new SAX().setPAAFragmentSize(2).setWordSize(2)
    sax.fit(trainingDataSet)
    sax.printInternalParameters()
    val transformed = sax.transform(evalDataSet)
    val r : Iterator[String] = transformed.collect()
    val result = r.toList
    val expected : Seq[String] = List("ab", "bb", "ba")
    assert(result === expected, "Result should match")
  }


}
