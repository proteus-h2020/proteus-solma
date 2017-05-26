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

package eu.proteus.solma.sax

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.pipeline.FitOperation
import org.apache.flink.util.Collector
import org.slf4j.Logger

object SAXFitOperation{

  /**
   * Class logger.
   */
  private val Log: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

}

/**
 * Fit operation for the SAX.
 */
class SAXFitOperation[T] extends FitOperation[SAX, T]{

  import eu.proteus.solma.sax.SAXFitOperation.Log

  /**
   * Extract the dataset statistics including the average and standard deviation.
   * @param instance The SAX instance
   * @param fitParameters The fit paramenters
   * @param input The training data.
   */
  override def fit(
    instance: SAX,
    fitParameters: ParameterMap,
    input: DataSet[T]): Unit = {

    val stats = input.map(_.toString.toDouble).reduceGroup[(Double, Double, Double)](
      (curr: Iterator[Double], collector: Collector[(Double, Double, Double)]) => {
        var acc = (0.0d, 0.0d, 0.0d)
        curr.foreach((x) => {
          acc = (acc._1 + (x * x), acc._2 + x, acc._3 + 1)
        })
        collector.collect(acc)
      }
    ).map((x: (Double, Double, Double)) => {
      val sqrAvg = x._1 / x._3
      val avg = x._2 / x._3
      val avgPerAvg = avg * avg
      val std = Math.sqrt(sqrAvg - avgPerAvg)
      (avg, std)
    }).collect().toList

    if(stats.size == 1){
      instance.trainingAvg = Some(stats.head._1)
      instance.trainingStd = Some(stats.head._2)
    }else{
      Log.error("Cannot extract stats from the given dataset")
      throw new RuntimeException("Cannot extract stats")
    }

  }

}

