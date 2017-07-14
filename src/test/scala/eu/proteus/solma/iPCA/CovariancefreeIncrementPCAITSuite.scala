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
import scala.io
import breeze.linalg._
import eu.proteus.annotations.Proteus
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.streaming.api.scala._
import org.apache.flink.contrib.streaming.scala.utils._
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

@Proteus
class CovariancefreeIncrementPCAITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {


  behavior of "Flink's Covariance-free Increment Principal Component Analysis"
  import CovariancefreeIncrementPCA._
  it should "compute eigenvector and eigenvalue" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setMaxParallelism(1)
    val line=1000
    val data=Source.fromFile("./src/test/scala/eu/proteus/solma/iPCA/preprocessed2.csv")
      .getLines()
      .map(i=>DenseVector(i.split(",").map(_.trim.toDouble))).filter(i=>i(0)<=line).toArray

    val stream=env.fromCollection(data)
    val k=3
    val transformer = CovariancefreeIncrementPCA().setEigenvectorsNumber(k)

    val it: scala.Iterator[PCA]= transformer.transform(stream).collect()

    val dim=data(0).length
    val u=data.foldLeft(DenseMatrix.zeros[Double](dim,1))((a,b)=>DenseMatrix.horzcat(a,b.asDenseMatrix.t))
                                                                                                      (0 to -1,0 to -1)


    val cov = (u*u.t)/(dim.toDouble-1)
    val offlinePCA=eigSym(cov)
    val ss=offlinePCA.eigenvalues(0 until k)
    val vv=offlinePCA.eigenvectors(::,0 until k)
    val nUrVV=((u.t*vv)*vv.t).t

    for (i<-0 until line-1) it.next()
    val elem:PCA=it.next
    val v=elem.eigenvector(::,*).map(i=>i:/norm(i))
    val nUr=((u.t*v)*v.t).t


    val ep=5E+2
    math.sqrt(sum((u:-nUr):*(u:-nUr))) should be (math.sqrt(sum((nUrVV:-u):*(nUrVV:-u))) +- ep)

    env.execute("Covariance-free IPCA")
  }
}
