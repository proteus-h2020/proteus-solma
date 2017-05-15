/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.proteus.solma.utils

import eu.proteus.annotations.Proteus
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.XORShiftRandom

@Proteus
object FlinkSolmaUtils {
  def registerFlinkMLTypes(env: StreamExecutionEnvironment): Unit = {

    // Vector types
    env.registerType(classOf[org.apache.flink.ml.math.DenseVector])
    env.registerType(classOf[org.apache.flink.ml.math.SparseVector])

    // Matrix types
    env.registerType(classOf[org.apache.flink.ml.math.DenseMatrix])
    env.registerType(classOf[org.apache.flink.ml.math.SparseMatrix])

    // Breeze Vector types
    env.registerType(classOf[breeze.linalg.DenseVector[_]])
    env.registerType(classOf[breeze.linalg.SparseVector[_]])

    // Breeze specialized types
    env.registerType(breeze.linalg.DenseVector.zeros[Double](0).getClass)
    env.registerType(breeze.linalg.SparseVector.zeros[Double](0).getClass)

    // Breeze Matrix types
    env.registerType(classOf[breeze.linalg.DenseMatrix[Double]])
    env.registerType(classOf[breeze.linalg.CSCMatrix[Double]])

    // Breeze specialized types
    env.registerType(breeze.linalg.DenseMatrix.zeros[Double](0, 0).getClass)
    env.registerType(breeze.linalg.CSCMatrix.zeros[Double](0, 0).getClass)

    // Solma Stream events
    env.registerType(classOf[eu.proteus.solma.events.StreamEvent])

  }

  def ensureKeyedStream[T](
      input: DataStream[T],
      funOpt: Option[(DataStream[Any]) => KeyedStream[(Any, Int), Int]]
    ): KeyedStream[(T, Int), Int] = {
    input match {
      case keyed : KeyedStream[(T, Int), Int] => keyed
      case _ => {
        funOpt match {
          case Some(fun) => {
            fun(input.asInstanceOf[DataStream[Any]]).asInstanceOf[KeyedStream[(T, Int), Int]]
          }
          case None => {
            val gen = new XORShiftRandom()
            val max = input.executionEnvironment.getParallelism
            implicit val typeInfo = TypeInformation.of(classOf[(T, Int)])
            input
              .map(x => (x, gen.nextInt(max)))
              .keyBy(x => x._2)
          }
        }

      }
    }
  }
}
