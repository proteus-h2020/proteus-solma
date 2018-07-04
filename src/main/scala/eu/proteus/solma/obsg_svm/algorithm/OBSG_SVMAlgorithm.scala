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

package eu.proteus.solma.obsg_svm.algorithm

import breeze.linalg.DenseVector
import breeze.numerics.signum
import eu.proteus.annotations.Proteus
import eu.proteus.solma.obsg_svm.OBSG_SVM
import eu.proteus.solma.obsg_svm.OBSG_SVM.{OBSG_SVMModel, UnlabeledVector}

@Proteus
class OBSG_SVMAlgorithm(instance: OBSG_SVM) extends BaseOBSG_SVMAlgorithm[UnlabeledVector, Double, OBSG_SVMModel]  {

  override def delta(
                      dataPoint: UnlabeledVector,
                      model: OBSG_SVMModel,
                      label: Double,
                      t: Long
                    ): (DenseVector[Double], Double) = {

    var c = instance.getCParam()
    val xp = instance.getXpParam()

    var dirw = DenseVector.zeros[Double](dataPoint.length)
    var dirb : Double = 0.0

    //var sign = 0.0
    if (label * (dataPoint dot model._1 + model._2) < 1){
      //  sign = 1.0
       dirw = model._1 - c * label * dataPoint
       dirb = - label

      c = c + (1.0 / t) * xp dot (label * dataPoint)


      instance.setCParam(c)
      instance.setXpParam(dataPoint.toDenseVector)
    }




    (dirw * (1.0 / t), dirb * (1.0 / t))
  }

  override def predict(
                        dataPoint: UnlabeledVector,
                        model: OBSG_SVMModel): Double = {
    signum(dataPoint dot model._1 + model._2)
  }
}
