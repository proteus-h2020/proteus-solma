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
package eu.proteus.solma.iPCA

import breeze.linalg.{DenseMatrix, Vector => BreezeVector}
import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.streaming.api.scala._
import breeze.linalg._
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * A simple stream transformer that ingests a stream of samples and outputs
  * simple mean and variance of the input stream. It requires to know the
  * total number of features in advance, but it can work with features
  * coming at different "speed", i.e., asynchronously.
  * The output is the stream of the parallel moments, outputted as soon as
  * an instance is updated. Optionally, the output may be an aggregation
  * of all the parallel moments (with loss of parallelism).
  */
@Proteus
class CovariancefreeIncrementPCA extends StreamTransformer[CovariancefreeIncrementPCA] {
  import CovariancefreeIncrementPCA._

  def setEigenvectorsNumber( count: Int): CovariancefreeIncrementPCA = {
    parameters.add(EigenvectorsNumber, count)
    this
  }

  def enableAggregation(enabled: Boolean): CovariancefreeIncrementPCA = {
    parameters.add(Aggregation, enabled)
    this
  }
}

object CovariancefreeIncrementPCA {


  // ====================================== Parameters =============================================

  case object EigenvectorsNumber extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  case object Aggregation extends Parameter[Boolean]{
    override val defaultValue: Option[Boolean] = Some(false)
  }


  //=====================================Extra=================================================


  case class PCA(eigenvector:DenseMatrix[Double],eigenvalue:DenseVector[Double])
  // ==================================== Factory methods ==========================================

  def apply(): CovariancefreeIncrementPCA = {
    new CovariancefreeIncrementPCA()
  }

  // ==================================== Operations ==========================================

  implicit def fitNoOp[T : TypeInformation] = {
    new StreamFitOperation[CovariancefreeIncrementPCA, T]{
      override def fit(
                        instance: CovariancefreeIncrementPCA,
                        fitParameters: ParameterMap,
                        input: DataStream[T])
      : Unit = {}
    }
  }





  implicit def treansformCovariancefreeIncrementPCA = {
    new TransformDataStreamOperation[CovariancefreeIncrementPCA, DenseVector[Double], PCA] {
      override def transformDataStream(
                                        instance: CovariancefreeIncrementPCA,
                                        transformParameters: ParameterMap,
                                        input: DataStream[DenseVector[Double]])
      : DataStream[PCA] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[DenseVector[Double]](
          input, resultingParameters.get(PartitioningOperation))
        val eigenvectorsNumber = resultingParameters(EigenvectorsNumber)
        val aggregation=resultingParameters(Aggregation)
        var pid:Long=0
        statefulStream.mapWithState((in, state: Option[PCA]) => {
          val (elem, _) = in

            val new_PCA= updatePCA(eigenvectorsNumber,elem, state,pid<eigenvectorsNumber,pid)

            pid+=1
            (new_PCA, Some(new_PCA))


        }

        )

      }
    }
  }
  def updatePCA(k:Int,data:DenseVector[Double],state:Option[PCA],initialisation:Boolean,i:Long):PCA= {

    if (initialisation)
    {state match {
      case Some(curr) => { PCA(DenseMatrix.horzcat(curr.eigenvector,data.asDenseMatrix.t),curr.eigenvalue)}
      case None=>PCA(data.asDenseMatrix.t,DenseVector.zeros[Double](k))
                  }
    }
    else{val pca= state match  { case Some(curr)=>  PCA(curr.eigenvector,curr.eigenvalue)}
      val vnorm=sum(pca.eigenvector.map(i=>i*i),Axis._0).t.map(i=>math.sqrt(i))
      var residue = data
      val (w1,w2)=amnesic(i + 4)
      val result1=DenseMatrix.zeros[Double](data.length,k)
      for (i <-0 until k)
        {result1(::,i):=w1*pca.eigenvector(::,i) + (w2*(pca.eigenvector(::,i).t*residue))*(residue/vnorm(i))
          residue = residue - residue.t * ((result1(::,i)/norm(result1(::,i))):*(result1(::,i)/norm(result1(::,i))))
        }

      val result2=sum(result1.map(i=>i*i),Axis._0).t.map(i=>math.sqrt(i))

      PCA(result1,result2)
    }

  }
  def amnesic(i:Long):(Double, Double)= {
    val n1 = 20
    val n2 = 500
    val m = 1000
    val l= if (i < n1) {0 } else if ((i >= n1) && (i < n2)) {2 * (i - n1) / (n2 - n1)} else {2 + (i - n2) / m}
    ((i - 1 - l.toDouble) / i,(1 + l.toDouble) / i)
  }
}
