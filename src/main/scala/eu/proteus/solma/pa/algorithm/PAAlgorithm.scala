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

package eu.proteus.solma.pa.algorithm

import breeze.linalg.{DenseVector, max, min, norm}
import breeze.numerics.signum
import eu.proteus.annotations.Proteus
import eu.proteus.solma.pa.PA
import eu.proteus.solma.pa.PA.{PAModel, UnlabeledVector}

@Proteus
class PAAlgorithm(instance: PA) extends BasePAAlgorithm[UnlabeledVector, Double, PAModel]  {

  override def delta(
                      dataPoint: UnlabeledVector,
                      model: PAModel,
                      label: Double,
                      t: Long
                    ): (DenseVector[Double]) = {

    val c = instance.getCParam()
    val algo = instance.getAlgoParam()
    var dirw = DenseVector.zeros[Double](dataPoint.length)

    if (algo == "PA"){
      val loss = max(1.0 - label * dataPoint dot model, 0.0)
      val alphat = loss/norm(dataPoint, 2.0)
      dirw = alphat * label * dataPoint.toDenseVector
    } else if (algo == "PAII"){
      val loss = max(1.0 - label * dataPoint dot model, 0.0)
      val alphat = loss/(norm(dataPoint, 2.0) + 1/(2*c))
      dirw = alphat * label * dataPoint.toDenseVector
    } else if (algo == "PAI"){
      val loss = max(1.0 - label * dataPoint dot model, 0.0)
      val alpha = loss/norm(dataPoint, 2.0)
      val alphat = min(alpha,c)
      dirw = alphat * label * dataPoint.toDenseVector
    } else {
      println("please specify an algorithm by setting algo as PA, PAI or PAII")
    }
    dirw
  }

  override def predict(
                        dataPoint: UnlabeledVector,
                        model: PAModel): Double = {
    signum(dataPoint dot model)
  }
}
