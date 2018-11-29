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

package eu.proteus.solma.asvi

import breeze.linalg.DenseVector
import eu.proteus.solma.asvi.AsynSVI.{ASVIModel, DataType}
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

class ASVIITSuite extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "SOLMA Online ASVI"


  it should "perform ASVY on wikipedia dataset" in {

    implicit val inputTypeInfo = createTypeInformation[String]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val dict = Source.fromInputStream(getClass.getResourceAsStream("/ldadict.txt"))
    val documents = Source.fromInputStream(getClass.getResourceAsStream("/documents.txt"))
    val data = new mutable.ArrayBuffer[(Long, DenseVector[String])]()
    val dictMap = new mutable.HashMap[String, Long]()
    var i = 0L
    for (line <- documents.getLines) {
      data.append((i, DenseVector(line.split(" ").map(_.trim))))
      i = i + 1
    }
    for (word <- dict.getLines) {
      val prev = dictMap.getOrElse(word.trim, 0L)
      dictMap.put(word, prev + 1L)
    }

    val asvi = AsynSVI()
      .setNumOfTopics(10)
      .setWorkerParallelism(2)
      .setPSParallelism(1)
      .setInitialVocabulary(dictMap)
      .setFeaturesCount(dictMap.size)
      .setPullLimit(50)
      .setIterationWaitTime(20000)

    val ds: DataStream[(Long, DenseVector[String])] = env.addSource(new SourceFunction[(Long, DenseVector[String])] {

      var running: Boolean = true

      override def run(ctx: SourceFunction.SourceContext[(Long, DenseVector[String])]): Unit = {
        for (x <- data) {
          ctx.collect(x)
          Thread.sleep(500)
        }
      }

      override def cancel(): Unit = {
        running = false
      }
    })

    asvi
      .transform(ds)
      .addSink(new SinkFunction[Either[(Long, DataType), (Int, ASVIModel)]] {
        override def invoke(value: Either[(Long, DataType), (Int, ASVIModel)]): Unit = {
          value match {
            case Right((id, model)) => {
              ASVIITSuite.LOG.info("Model {}", model)
              println("Model " + model)
            }
          }
        }
      })
      .setParallelism(1)

    env.execute()
  }

}

object ASVIITSuite {
  val LOG: slf4j.Logger = LoggerFactory.getLogger(ASVIITSuite.getClass)
}
