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

import eu.proteus.annotations.Proteus
import eu.proteus.solma.asvi.AsynSVI.{ASVIModel, UnlabeledVector, DataType}
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}

import scala.collection.mutable

@Proteus
class ASVIWorkerLogic[IN, OUT](algorithm: GradientAlgorithm[DataType, ASVIModel])  extends WorkerLogic[
    UnlabeledVector,
    ASVIModel,
    UnlabeledVector] {

  val unprocessedData = new mutable.Queue[UnlabeledVector]

  override def onRecv(
      data: UnlabeledVector,
      ps: ParameterServerClient[ASVIModel, UnlabeledVector]
  ): Unit = {
    unprocessedData.enqueue(data)
    ps.pull(0)
  }

  override def onPullRecv(
    paramId: Int,
    eta: ASVIModel,
    ps: ParameterServerClient[ASVIModel, UnlabeledVector]): Unit = {

    while (unprocessedData.nonEmpty) {
      val (index, dataPoint) = unprocessedData.dequeue()
      val naturalGradient = algorithm.gradient(dataPoint, eta)

      ps.push(0, naturalGradient)
    }
  }
}
