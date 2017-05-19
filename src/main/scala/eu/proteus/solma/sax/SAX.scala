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

import eu.proteus.solma.pipeline.StreamTransformer
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.Parameter
import org.apache.flink.ml.pipeline.Estimator
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.slf4j.Logger

/**
 * Implementation of the SAX-VSM algorithm.
 *
 * This class contains the implementation to extract words from a signal given a number of elements
 * to be averaged using PAA, a size of the alphabet, and the length of the words to be generated.
 *
 * References:
 * Senin, Pavel, and Sergey Malinchik. "Sax-vsm: Interpretable time series classification using
 * SAX and vector space model." Data Mining (ICDM), 2013 IEEE 13th International Conference on.
 * IEEE, 2013.
 *
 */
object SAX {

  /**
   * Class logger.
   */
  private val Log: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

  /**
   * Case object to define the size of words to be used for the SAX.
   */
  case object WordSize extends Parameter[Int] {

    /**
     * Default word size constant.
     */
    private val DefaultWordSize : Int = 1

    /**
     * Default value.
     */
    override val defaultValue : Option[Int] = Some(WordSize.DefaultWordSize)
  }

  /**
   * Case object to define the size of the alphabet in the SAX abstraction.
   */
  case object AlphabetSize extends Parameter[Int] {

    /**
     * Default alphabet size.
     */
    private val DefaultAlphabetSize : Int = 2

    /**
     * Default value.
     */
    override val defaultValue: Option[Int] = Some(AlphabetSize.DefaultAlphabetSize)
  }

  /**
   * Case object that defines the size of the window for the PAA aggregation.
   */
  case object PAAFragmentSize extends Parameter[Int] {

    /**
     * Default fragment size.
     */
    private val DefaultFragmentSize : Int = 2

    /**
     * Default value.
     */
    override val defaultValue: Option[Int] = Some(PAAFragmentSize.DefaultFragmentSize)
  }

  implicit def fitImplementation[T] = {
    new SAXFitOperation[T]
  }

  implicit def transformImplementation[T <: Double] = {
    new SAXStreamTransformOperation[T]
  }

  /**
   * Transform the input datastream in a KeyedStream. The method will reject data that is not
   * associated with a key.
   * @param input The input datastream with tuples containing the payload and the key.
   * @tparam T The payload type.
   * @return A KeyedStream.
   */
  private[sax] def toKeyedStream[T](
    input: DataStream[(T, Int)]) : KeyedStream[(T, Int), Int] = {

    input match {
      case tuples : DataStream[(T, Int)] => {
        implicit val typeInfo = TypeInformation.of(classOf[(Any, Int)])
        tuples.asInstanceOf[DataStream[(T, Int)]].keyBy(t => t._2)
      }
      case _ => {
        throw new UnsupportedOperationException(
          "Cannot build a keyed stream from the given data type")
      }
    }

  }

}

/**
 * SAX transformer.
 */
class SAX extends StreamTransformer[SAX] with Estimator[SAX]{

  import eu.proteus.solma.sax.SAX.Log

  /**
   * Average of the training set.
   */
  private[sax] var trainingAvg : Option[Double] = None

  /**
   * Standard deviation of the training set.
   */
  private[sax] var trainingStd : Option[Double] = None

  /**
   * Sets the size of the words for SAX.
   * @param size The size of the words.
   * @return A configured [[SAX]].
   */
  def setWordSize(size: Int) : SAX = {
    this.parameters.add(SAX.WordSize, size)
    this
  }

  /**
   * Get the word size.
   * @return The size.
   */
  def getWordSize() : Int = {
    this.parameters.get(SAX.WordSize).get
  }

  /**
   * Set the PAA fragment size inside each window.
   * @param size The size of the fragment.
   * @return A configured [[SAX]].
   */
  def setPAAFragmentSize(size: Int) : SAX = {
    this.parameters.add(SAX.PAAFragmentSize, size)
    this
  }

  /**
   * Get the PAA fragment size.
   * @return The effective size.
   */
  def getPAAFragmentSize() : Int = {
    this.parameters.get(SAX.PAAFragmentSize).get
  }

  /**
   * Sets the size of the alphabet in SAX.
   * @param size The number of letters in the alphabet.
   * @return A configured [[SAX]].
   */
  def setAlphabetSize(size: Int) : SAX = {
    this.parameters.add(SAX.AlphabetSize, size)
    this
  }

  /**
   * Get the size of the alphabet.
   * @return
   */
  def getAlphabetSize() : Int = {
    this.parameters.get(SAX.AlphabetSize).get
  }

  /**
   * Get the cuts associated with the current alphabet size.
   * @return An array with the distribution cuts.
   */
  private[sax] def getAlphabetCuts() : Array[Double] = {
    Cuts.breakpoints(this.getAlphabetSize())
  }

  /**
   * Get the fitted parameters.
   * @return A tuple with the training average and training standard deviation.
   */
  def getFittedParameters() : Option[(Double, Double)] = {
    if(this.trainingAvg.isDefined && this.trainingStd.isDefined){
      Some((this.trainingAvg.get, this.trainingStd.get))
    }else{
      None
    }
  }

  /**
   * Load the internal parameters required by the SAX transformation. Use this method to avoid
   * training the same dataset multiple times.
   *
   * @param average The average of the values.
   * @param standardDeviation The standard deviation of the values.
   */
  def loadParameters(average: Double, standardDeviation: Double) : Unit = {
    this.trainingAvg = Some(average)
    this.trainingStd = Some(standardDeviation)
  }

  /**
   * Print the internal parameters to the logger output.
   */
  def printInternalParameters() : Unit = {
    Log.info("Word size: " + this.parameters.get(SAX.WordSize))
    Log.info("Alphabet size: " + this.parameters.get(SAX.AlphabetSize))
    Log.info("PAA fragment size: " + this.parameters.get(SAX.PAAFragmentSize))
    Log.info("Training Avg: " + this.trainingAvg)
    Log.info("Training Std: " + this.trainingStd)
  }

  /**
   * Apply helper.
   * @return A new [[SAX]].
   */
  def apply(): SAX = {
    new SAX()
  }

}

