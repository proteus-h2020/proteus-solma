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

package eu.proteus.solma.pa

import breeze.linalg.{DenseMatrix, DenseVector, Vector}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.events.{StreamEvent, StreamEventLabel, StreamEventWithPos}
import eu.proteus.solma.pipeline.StreamTransformer
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.{Parameter, WithParameters}
import org.slf4j.Logger

import scala.reflect.ClassTag

@Proteus
class PA extends StreamTransformer[PA]{

  import eu.proteus.solma.pa.PA._

  /** Get the WorkerParallism value as Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(PA.WorkerParallism).get
  }

  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism workerParallism as Int
    * @return
    */
  def setWorkerParallelism(workerParallism : Int): PA = {
    this.parameters.add(WorkerParallism, workerParallism)
    this
  }

  /**
    * Set the C value a
    * @param c the c param of PA
    * @return
    */
  def setCParam(c : Double): PA = {
    this.parameters.add(CParam, c)
    this
  }

  /** Get the C param
    * @return The C param
    */
  def getCParam() : Double  = {
    this.parameters.get(PA.CParam).get
  }


  /**
    * Set the algorithms a
    * @param algo the algo param of PA
    * @return
    */
  def setAlgoParam(algo : String): PA = {
    this.parameters.add(AlgoParam, algo)
    this
  }

  /** Get the algo param
    * @return The algo param
    */
  def getAlgoParam() : String  = {
    this.parameters.get(PA.AlgoParam).get
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
  def setPSParallelism(psParallelism : Int): PA = {
    parameters.add(PSParallelism, psParallelism)
    this
  }
  /**
    * Variable to stop the calculation in stream environment
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long) : PA = {
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
  def setPullLimit(pullLimit : Int) : PA = {
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
  def setAllowedLateness(allowedLateness: Int) : PA = {
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
  def setFeaturesCount(featuresCount: Int) : PA = {
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

}

object PA extends WithParameters with Serializable {

  type PAStreamEvent = Either[(Long, StreamEvent), Label]
  type PAModel = (DenseVector[Double])  //changed deleted Double
  type UnlabeledVector = Vector[Double]
  type Label = (Long, Double)

  /**
    * Apply helper.
    * @return A new [[PA]].
    */
  def apply(): PA = {
    new PA()
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
  case object CParam extends Parameter[Double] {
    /**
      * Default Label for unseen data
      */
    private val DefaultC: Double = 0.5
    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(CParam.DefaultC)
  }
  case object AlgoParam extends Parameter[String] {
    /**
      * Default algorithms
      */
    private val DefaultAlgo: String = "PAI"
    /**
      * Default algorithms.
      */
    override val defaultValue: Option[String] = Some(AlgoParam.DefaultAlgo)
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

  implicit def transformStreamImplementation = {
    new PAPrequentialTraining
  }


}
