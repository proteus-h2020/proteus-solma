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

package eu.proteus.solma.obsg_svm

import breeze.linalg.{DenseMatrix, DenseVector, Vector}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.events.{StreamEvent, StreamEventLabel, StreamEventWithPos}
import eu.proteus.solma.pipeline.StreamTransformer
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.{Parameter, WithParameters}
import org.slf4j.Logger

import scala.reflect.ClassTag

@Proteus
class OBSG_SVM extends StreamTransformer[OBSG_SVM]{

  import eu.proteus.solma.obsg_svm.OBSG_SVM._

  /** Get the WorkerParallism value as Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(OBSG_SVM.WorkerParallism).get
  }

  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism workerParallism as Int
    * @return
    */
  def setWorkerParallelism(workerParallism : Int): OBSG_SVM = {
    this.parameters.add(WorkerParallism, workerParallism)
    this
  }

  /**
    * Set the C value a
    * @param c the c param of OBSG_SVM
    * @return
    */
  def setCParam(c : Double): OBSG_SVM = {
    this.parameters.add(CParam, c)
    this
  }

  /** Get the C param
    * @return The C param
    */
  def getCParam() : Double  = {
    this.parameters.get(OBSG_SVM.CParam).get
  }

  def setXpParam(xp : DenseVector[Double]): OBSG_SVM = {
    this.parameters.add(XpParam, xp)
    this
  }

  /** Get the Xp param
    * @return The Xp param
    */
  def getXpParam() : DenseVector[Double]  = {
    this.parameters.get(OBSG_SVM.XpParam).get
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
  def setPSParallelism(psParallelism : Int): OBSG_SVM = {
    parameters.add(PSParallelism, psParallelism)
    this
  }
  /**
    * Variable to stop the calculation in stream environment
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long) : OBSG_SVM = {
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
  def setPullLimit(pullLimit : Int) : OBSG_SVM = {
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
  def setAllowedLateness(allowedLateness: Int) : OBSG_SVM = {
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
  def setFeaturesCount(featuresCount: Int) : OBSG_SVM = {
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

object OBSG_SVM extends WithParameters with Serializable {

  type OBSG_SVMStreamEvent = Either[(Long, StreamEvent), Label]
  type OBSG_SVMModel = (DenseVector[Double], Double)
  type UnlabeledVector = Vector[Double]
  type Label = (Long, Double)

  /**
    * Apply helper.
    * @return A new [[OBSG_SVM]].
    */
  def apply(): OBSG_SVM = {
    new OBSG_SVM()
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
    new OBSG_SVMPrequentialTraining
  }

  case object XpParam extends Parameter[DenseVector[Double]] {
    /**
      * Default Label for unseen data
      */
    private val DefaultXp: DenseVector[Double] = DenseVector.zeros[Double](8)
    /**
      * Default value.
      */
    override val defaultValue: Option[DenseVector[Double]] = Some(XpParam.DefaultXp)
  }

}
