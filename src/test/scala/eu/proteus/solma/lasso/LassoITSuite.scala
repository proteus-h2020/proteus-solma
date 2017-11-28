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

package eu.proteus.solma.lasso

import breeze.linalg.DenseVector
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object LassoITSuite

/**
  * Lasso tests.
  */
class LassoITSuite extends FunSuite with Matchers with FlinkTestBase{

  /**
    * Basic naive fit test.
    */
  test("Basic naive fit"){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val v = new DenseVector[Double](2)
    val trainingData : Seq[DenseVector[Double]] = List(v, v, v, v, v)
    val datastream : DataStream[DenseVector[Double]] = env.fromCollection(trainingData)
    val lasso = new Lasso()
    lasso.train(datastream)
  }

}

