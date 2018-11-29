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

import breeze.linalg.{DenseMatrix, DenseVector}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.StreamTransformer
import org.apache.flink.ml.common.Parameter
import org.slf4j.Logger

import scala.collection.mutable

@Proteus
class AsynSVI extends StreamTransformer[AsynSVI] {

  import eu.proteus.solma.asvi.AsynSVI._

  /** Get the WorkerParallism value as Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(AsynSVI.WorkerParallism).get
  }

  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism workerParallism as Int
    * @return
    */
  def setWorkerParallelism(workerParallism : Int): AsynSVI = {
    this.parameters.add(WorkerParallism, workerParallism)
    this
  }

  /**
    * Get the SeverParallism value as Int
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(PSParallelism).get
  }
  /**
    * Set the SeverParallism value as Int
    * @param psParallelism serverparallelism as Int
    * @return
    */
  def setPSParallelism(psParallelism : Int): AsynSVI = {
    parameters.add(PSParallelism, psParallelism)
    this
  }
  /**
    * Variable to stop the calculation in stream environment
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long) : AsynSVI = {
    parameters.add(IterationWaitTime, iterationWaitTime)
    this
  }
  /** Variable to stop the calculation in stream environment
    * get the waiting time
    * @return The waiting time as Long
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(IterationWaitTime).get
  }

  /**
    * set a pull limit
    * @param pullLimit
    */
  def setPullLimit(pullLimit : Int) : AsynSVI = {
    this.parameters.add(PullLimit, pullLimit)
    this
  }
  /**
    * Get the pull limit
    * @return The pull limit
    */
  def getPullLimit(): Int = {
    this.parameters.get(PullLimit).get
  }

  /**
    * set allowed lateness
    * @param allowedLateness
    */
  def setAllowedLateness(allowedLateness: Int) : AsynSVI = {
    this.parameters.add(AllowedLateness, allowedLateness)
    this
  }
  /**
    * Get the allowed lateness
    * @return The allowed lateness
    */
  def getAllowedLateness(): Int = {
    this.parameters.get(AllowedLateness).get
  }

   /**
    * set a pull
    * @param featuresCount
    */
  def setFeaturesCount(featuresCount: Int) : AsynSVI = {
    this.parameters.add(FeaturesCount, featuresCount)
    this
  }
  /**
    * Get the Windowsize
    * @return The windowsize
    */
  def getFeaturesCount(): Int = {
    this.parameters.get(FeaturesCount).get
  }

  def getNumOfTopics(): Int = {
    this.parameters.get(NumberOfTopics).get
  }

  def setNumOfTopics(k :Int): AsynSVI = {
    this.parameters.add(NumberOfTopics, k)
    this
  }

  def getInitialVocabulary: mutable.Map[String, Long] = {
    this.parameters.get(InitialVocabulary).get
  }

  def setInitialVocabulary(vocabulary: mutable.Map[String, Long]): AsynSVI = {
    this.parameters.add(InitialVocabulary, vocabulary)
    this
  }
}

object AsynSVI {

  type ASVIModel = DenseMatrix[Double]
  type DataType = DenseVector[String]
  type UnlabeledVector = (Long, DataType)

  /**
    * Apply helper.
    * @return A new [[AsynSVI]].
    */
  def apply(): AsynSVI = {
    new AsynSVI()
  }

   /**
    * Class logger.
    */
  private val Log: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

  case object IterationWaitTime extends Parameter[Long] {
    /**
      * Default time - needs to be set in any case to stop this calculation
      */
    private val DefaultIterationWaitTime: Long = 4000
    /**
      * Default value.
      */
    override val defaultValue: Option[Long] = Some(IterationWaitTime.DefaultIterationWaitTime)
  }

  case object NumberOfTopics extends Parameter[Int] {
    private val Default: Int = 20
    override val defaultValue: Option[Int] = Some(Default)
  }
  case object WorkerParallism extends Parameter[Int] {
    /**
      * Default parallelism
      */
    private val DefaultWorkerParallism: Int = 4
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(WorkerParallism.DefaultWorkerParallism)
  }
  case object PullLimit extends Parameter[Int] {
    /**
      * Default parallelism
      */
    private val DefaultPullLimit: Int = 20000
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(PullLimit.DefaultPullLimit)
  }
  case object PSParallelism extends Parameter[Int] {
    /**
      * Default server parallelism.
      */
    private val DefaultPSParallelism: Int = 4
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(PSParallelism.DefaultPSParallelism)
  }
  case object AllowedLateness extends Parameter[Int] {
    /**
      * Default Label for unseen data
      */
    private val AllowedLatenessDefault: Int = 1
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(AllowedLateness.AllowedLatenessDefault)
  }

  case object FeaturesCount extends Parameter[Int] {
    /**
      * Default output are the Weights
      */
    private val DefaultFeaturesCount: Int = -1
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(FeaturesCount.DefaultFeaturesCount)
  }

  case object InitialVocabulary extends Parameter[mutable.Map[String, Long]] {
    /**
      * Default output are the Weights
      */
    private val EmptyMap: mutable.Map[String, Long] = new mutable.HashMap[String, Long]()
    /**
      * Default value.
      */
    override val defaultValue: Option[mutable.Map[String, Long]] = Some(InitialVocabulary.EmptyMap)
  }

  implicit def transformStreamImplementation = {
    new AsynchronousStochasticVariationalInference
  }
}
