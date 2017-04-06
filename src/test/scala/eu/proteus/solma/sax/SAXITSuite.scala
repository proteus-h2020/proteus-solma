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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.DataStreamUtils
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.flink.api.scala.createTypeInformation
import eu.proteus.solma.sax.SAX.fitImplementation
import eu.proteus.solma.sax.SAX.transformImplementation
import scala.collection.JavaConverters.asScalaIteratorConverter

import scala.collection.mutable

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

  test("Basic transform with 5 elements"){
    val env = ExecutionEnvironment.createLocalEnvironment(4)
    val trainingData : Seq[Double] = List(0.0d, 1.0d, 2.0d, 3.0d, 4.0d)
    val trainingDataSet : DataSet[Double] = env.fromCollection(trainingData)
    val streamingEnv = StreamExecutionEnvironment.createLocalEnvironment(4)
    streamingEnv.fromCollection(trainingData)
    val evalDataSet : DataStream[Double] = streamingEnv.fromCollection(trainingData)
    val sax = new SAX().setPAAFragmentSize(1).setWordSize(2)
    sax.fit(trainingDataSet)
    sax.printInternalParameters()
    val pre : DataStream[String] = sax.transform(evalDataSet)
    val result : Iterator[String] = DataStreamUtils.collect[String](pre.javaStream).asScala
    streamingEnv.execute()
    println("Result: " + result.mkString(", "))
  }

}
