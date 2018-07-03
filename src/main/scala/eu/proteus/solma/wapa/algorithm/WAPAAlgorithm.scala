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

package eu.proteus.solma.wapa.algorithm

import breeze.linalg.{DenseVector, max, min, norm}
import breeze.numerics.signum
import eu.proteus.annotations.Proteus
import eu.proteus.solma.wapa.WAPA
import eu.proteus.solma.wapa.WAPA.{WAPAModel, UnlabeledVector}

@Proteus
class WAPAAlgorithm(instance: WAPA) extends BaseWAPAAlgorithm[UnlabeledVector, Double, WAPAModel]  {

  override def delta(
                      dataPoint: UnlabeledVector,
                      model: WAPAModel,
                      label: Double,
                      t: Long
                    ): (DenseVector[Double]) = {

    val c = instance.getCParam()
    val rho = instance.getRhoParam()
    val algo = instance.getAlgoParam()
    var dirw = DenseVector.zeros[Double](dataPoint.length)

    if (algo == "WAPA"){
      val loss = max(1.0 - label * dataPoint dot model, 0.0)
      val alphat = loss/norm(dataPoint, 2.0)
      dirw = rho * alphat * label * dataPoint.toDenseVector
    } else if (algo == "WAPAII"){
      val loss = max(1.0 - label * dataPoint dot model, 0.0)
      val alphat = loss/(norm(dataPoint, 2.0) + 1/(2*c))
      dirw = rho * alphat * label * dataPoint.toDenseVector
    } else if (algo == "WAPAI"){
      val loss = max(1.0 - label * dataPoint dot model, 0.0)
      val alpha = loss/norm(dataPoint, 2.0)
      val alphat = min(alpha,c)
      dirw = rho * alphat * label * dataPoint.toDenseVector
    } else {
      println("please specify an algorithm by setting algo as WAPA, WAPAI or WAPAII")
    }
    dirw
  }

  override def predict(
                        dataPoint: UnlabeledVector,
                        model: WAPAModel): Double = {
    signum(dataPoint dot model)
  }
}
