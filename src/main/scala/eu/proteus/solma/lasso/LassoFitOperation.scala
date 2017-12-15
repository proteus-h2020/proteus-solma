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

import eu.proteus.solma.pipeline.StreamFitOperation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.slf4j.Logger

object LassoFitOperation{

  private val Log: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

}

/**
  * Fit operation for the Lasso.
  */
class LassoFitOperation[T] extends StreamFitOperation[Lasso, T]{

  import eu.proteus.solma.lasso.LassoFitOperation.Log

  /**
    * Extract the dataset statistics including the average and standard deviation.
    * @param instance The Lasso instance
    * @param fitParameters The fit paramenters
    * @param input The training data.
    */
  override def fit(instance: Lasso,
                   fitParameters: ParameterMap,
                   input: DataStream[T]): Unit = {


  }

}


