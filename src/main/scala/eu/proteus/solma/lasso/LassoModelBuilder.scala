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

import eu.proteus.solma.lasso.Lasso.{LassoModel, LassoParam}

class LassoModelBuilder (val baseModel: LassoModel) extends ModelBuilder[LassoParam, LassoModel] {
  def addParams(param1: LassoParam, param2: LassoParam): LassoParam =
    (param1._1 + param2._1, param1._2 + param2._2)

  override def buildModel(params: Iterable[(Int, LassoParam)],
                          featureCount: Int): LassoModel = {
    params.map(x => x._2).reduce(addParams(_, _))
  }

  def add(id: Int, incModel: LassoModel): LassoModel = {
    addParams(baseModel, incModel)
  }
}

/**
  * Generic model builder for Lasso cases.
  *
  * @tparam Param
  * Type of Parameter Server parameter.
  * @tparam Model
  * Type of the model.
  */
protected trait ModelBuilder[Param, Model] extends Serializable {

  /**
    * Creates a model out of single parameters.
    *
    * @param params
    * Parameters.
    * @param featureCount
    * Number of features.
    * @return
    * Model.
    */
  def buildModel(params: Iterable[(Int, Param)], featureCount: Int): Model

}
