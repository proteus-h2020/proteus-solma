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

import eu.proteus.solma.pipeline.StreamPredictor
import org.apache.flink.ml.common.Parameter
import org.apache.flink.ml.pipeline.Estimator

/**
 * Implementation of the SAX-VSM algorithm.
 *
 * This class contains the implementation of the dictionary required to store the document signature
 * per class. Notice that to use this with the provided SAX implementation it is required to
 * add the associated class identifier during the fit process. The fit process can be executed
 * as many times as required to include different classes into the dictionary. Once the dictionary
 * is built, the predict method will give information about the distance to the nearest class
 * in the dictionary for a given set of observations. Notice that in order to properly predict the
 * class and internal window will be used to match the length in words of the classes of the
 * dictionary.
 *
 * References:
 * Senin, Pavel, and Sergey Malinchik. "Sax-vsm: Interpretable time series classification using
 * SAX and vector space model." Data Mining (ICDM), 2013 IEEE 13th International Conference on.
 * IEEE, 2013.
 *
 */
object SAXDictionary{

  /**
   * Case object to define the number of words to wait for the class comparison.
   */
  case object NumberWords extends Parameter[Int] {

    /**
     * Default number of words constant.
     */
    private val DefaultNumberWords : Int = 1

    /**
     * Default value.
     */
    override val defaultValue : Option[Int] = Some(NumberWords.DefaultNumberWords)
  }

  implicit def fitImplementation[T] = {
    new SAXDictionaryFitOperation[T]
  }

  implicit def predictImplementation[K <: String] = {
    new SAXDictionaryPredictOperation[K]
  }

}


class SAXDictionary extends StreamPredictor[SAXDictionary] with Estimator[SAXDictionary]{

  /**
   * The dictionary.
   */
  private[sax] var dictionary : Option[BagDictionary] = None

  /**
   * Sets the number of words to use for the class comparison.
   * @param size The number of words.
   * @return A configured [[SAXDictionary]].
   */
  def setNumberWords(size: Int) : SAXDictionary = {
    this.parameters.add(SAXDictionary.NumberWords, size)
    this
  }

  /**
   * Get the number of words to use for the class comparison.
   * @return The number of words.
   */
  def getNumberWords() : Int = {
    this.parameters.get(SAXDictionary.NumberWords).get
  }

  /**
   * Get the recommended number of words to be used during the prediction phase.
   * @return An option with the recommended number of words.
   */
  def getRecommendedNumberWords() : Option[Int] = {
    if(this.dictionary.isDefined){
      Some(this.dictionary.get.maxWords())
    }else{
      None
    }
  }

  /**
   * Get the underlying dictionary.
   * @return The dictionary.
   */
  def getDictionary() : Option[BagDictionary] = {
    this.dictionary
  }

}
