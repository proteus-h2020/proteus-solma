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

package eu.proteus.solma.osvm.algorithm

import breeze.linalg.DenseVector
import breeze.numerics.signum
import eu.proteus.annotations.Proteus
import eu.proteus.solma.osvm.OSVM
import eu.proteus.solma.osvm.OSVM.{OSVMModel, UnlabeledVector}

@Proteus
class OSVMAlgorithm(instance: OSVM) extends BaseOSVMAlgorithm[UnlabeledVector, Double, OSVMModel]  {

  override def delta(
      dataPoint: UnlabeledVector,
      model: OSVMModel,
      label: Double,
      t: Long
  ): (DenseVector[Double], Double) = {

    val c = instance.getCParam()
    var sign = 0.0
    if (label * (dataPoint dot model._1 + model._2) < 1){
      sign = 1.0
    }
    val dirw = model._1 - c * label * dataPoint * sign
    val dirb = - label * sign
    (dirw * (1.0 / t), dirb * (1.0 / t))
  }

  override def predict(
      dataPoint: UnlabeledVector,
      model: OSVMModel): Double = {
    signum(dataPoint dot model._1 + model._2)
  }
}
