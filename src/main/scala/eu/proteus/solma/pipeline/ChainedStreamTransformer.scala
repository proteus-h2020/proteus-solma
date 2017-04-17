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

package eu.proteus.solma.pipeline

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala._

case class ChainedStreamTransformer[
    L <: StreamTransformer[L],
    R <: StreamTransformer[R]
  ](left: L, right: R)
  extends StreamTransformer[ChainedStreamTransformer[L, R]] {
}

object ChainedStreamTransformer {
   implicit def chainedStreamTransformOperation[
      L <: StreamTransformer[L],
      R <: StreamTransformer[R],
      I,
      T,
      O](implicit
      transformOpLeft: TransformDataStreamOperation[L, I, T],
      transformOpRight: TransformDataStreamOperation[R, T, O])
    : TransformDataStreamOperation[ChainedStreamTransformer[L,R], I, O] = {
    new TransformDataStreamOperation[ChainedStreamTransformer[L, R], I, O] {
      override def transformDataStream(
          instance: ChainedStreamTransformer[L, R],
          transformParameters: ParameterMap,
          input: DataStream[I]): DataStream[O] = {
        val intermediateResult = transformOpLeft.transformDataStream(
          instance.left,
          transformParameters,
          input)
        transformOpRight.transformDataStream(
            instance.right,
            transformParameters,
            intermediateResult
        )
      }
    }
  }

  implicit def chainedStreamFitOperation[
      L <: StreamTransformer[L], R <: StreamTransformer[R], I, T](
      implicit leftFitOperation: StreamFitOperation[L, I],
      leftTransformOperation: TransformDataStreamOperation[L, I, T],
      rightFitOperation: StreamFitOperation[R, T])
  : StreamFitOperation[ChainedStreamTransformer[L, R], I] = {
    new StreamFitOperation[ChainedStreamTransformer[L, R], I] {
      override def fit(
          instance: ChainedStreamTransformer[L, R],
          fitParameters: ParameterMap,
          input: DataSet[I]): Unit = {
        // TODO Check implementation
        // val intermediateResult = instance.left.transform(input, fitParameters)
        // instance.right.fit(intermediateResult, fitParameters)
      }
    }
  }
}
