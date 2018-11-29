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

import breeze.linalg.{DenseVector, Matrix}
import breeze.numerics.{abs, digamma, exp, log}
import breeze.stats.distributions.Gamma
import breeze.linalg._

import scala.collection.mutable
import breeze.linalg.{max, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.mean
/**
  * Created by zhanwang on 14/02/18.
  */

class StreamVI(
    data_from: Boolean,
    data:Matrix[Double]=Matrix.zeros(1,1),
    var K:Int=20, dim:Int,
    var mean_t0: Double = 0.0,
    covar_t0: Matrix[Double]= Matrix.zeros(1,1),
    var v_0:Double=0.0,
    k_0:Double=0.0,
    var max_itr:Int=10,
    var alpha_0: Double=0.0,
    var nu:Double,
    covar_0:collection.mutable.Map[String,Long]) extends Serializable {


  """
    Arguments:
 |   K: Number of topics
 |   vocab: A set of words to recognize. When analyzing documents, any word
 |           not in this set will be ignored.
 |   dim: Total number of documents in the population. For a fixed corpus,
 |           this is the size of the corpus. In the truly online setting, this
 |           can be an estimate of the maximum number of documents that
 |           could ever be seen.
 |   alpha: Hyperparameter for prior on weight vectors theta
 |   eta: Hyperparameter for prior on topics beta
  """

  var phi= DenseVector[DenseMatrix[Double]]()
  //DenseMatrix.rand[Double](K,dim)
  var max_iter = max_itr
  var eta_0 = 0.0
  var vocab = mutable.Map[String,Long]()
  //var alpha_t0 = Matrix.ones(data.rows,K)
  val workerWindowSize = 20
  var alpha_t0 = DenseMatrix.ones[Double](workerWindowSize, K)
  //val d = Gamma(100.0, 1.0/100.0)
  val dataG= DenseVector(Gamma(100.0, 1.0/100.0).sample(K*dim).toArray)
  var eta_t0  = dataG.asDenseMatrix.reshape(K, dim)

  //var eta_t0 = Nd4j.create(dataG).reshape(K, dim)

  if(this.alpha_0 == None){
    this.alpha_0 = 0.01
  }
  if(this.v_0 == None){
    this.eta_0 = 0.01
  }
  else{
    this.eta_0 = v_0
  }
  if(data_from == true){
    for (word <- covar_0.keySet.iterator){
      val w = word.toLowerCase
      this.vocab += (w -> vocab.size.toLong)
    }
  }
  else{
    this.vocab = covar_0
  }
  if(this.max_itr == None){
    this.max_itr = 100
  }
  val max_ite_2 = max_itr
  if(this.nu == None){
    this.nu = 0.1
  }
  val nu_2 = nu

  def expectationPhi(
      x: DenseVector[String],
      i: Int,
      phi: DenseVector[DenseMatrix[Double]],
      online: Double
  ): Unit = {
    val N = x.length
    //var A = Array.fill(this.dim)(0.0)
    //Nd4j.setDataType(DataBuffer.Type.DOUBLE)

    var A = DenseVector.rand[Double](this.dim)
    for(d <- 0 until N){
      //        phi [D,K,dim]
      val DocVec = phi(d)
      val phi_d_k = DocVec(i,::).t
      val wordInDoc = wordsInDocument(phi_d_k, this.vocab, x(d), this.data_from)
      val vt = DenseVector(wordInDoc)
      A = A + vt
    }
    A += (online + this.eta_0)
    this.eta_t0(i,::) := A.t
  }
  def expectationLamdaPhiNexpj(
    jj: Int,
    x: DenseVector[String],
    n: Int,
    words: Array[Long]
  ): DenseVector[Double] = {


    //jj is the topic number, from 0 to k
    var phi = DenseVector.zeros[Double](this.dim)
    val t1 = this.alpha_t0(n, jj)
    //, Axis._0
    val t2 = breeze.linalg.sum(this.eta_t0(jj,::))

    val A = evPsi(t1) - ( evPsi(t2) )
    val wordids = words.map(_.toInt)
    //to do this.eta_t0.getRow(jj).getColumns did't work, for loop would be very ugly.
    //val t3 = A.add(ev_psi( this.eta_t0.getRow(jj).getColumns(wordids:_*).data().asDouble() ))
    var tt3 = new Array[Double](wordids.length)
    for(idx <- 0 until wordids.length){
      tt3(idx)=this.eta_t0(jj, wordids(idx) )
    }
    val tt2 = evPsi(tt3)
    val t3 = tt2 + A
    //phi(wordids:_*, t3)
    //phi(wordids:_*). = t3
    for(idx <- 0 until wordids.length){
      phi(wordids(idx)) = t3(idx)
    }

    //val expv = org.nd4j.linalg.ops.transforms.Transforms.exp(phi.getColumns(wordids:_*))
    //phi.putRow(wordid,expv)
    val tmp = DenseVector.zeros[Double](this.dim)
    for(idx <- 0 until wordids.length){
      tmp(wordids(idx))= scala.math.exp(phi(wordids(idx) ))
    }


    tmp

  }
  def updatePhiN(x: DenseVector[String], n: Int): DenseMatrix[Double] = {
    var phi = DenseMatrix.zeros[Double](this.K, this.dim)
    val (words,counts) = parseDocuments(x(n), this.vocab, this.data_from)


    for(jj <- 0 until this.K){
      //compute the expectation E_{q^{(t-1)}}[n_{l,j}(x_n,z_{n,-j},\beta)]
      val expe_lamda_phi_nexpj = expectationLamdaPhiNexpj(jj, x, n, words)
      // E_{q^{(t-1)}}[n_{l,j}(x_n,z_{n,-j},\beta)]
      phi(jj,::) := expe_lamda_phi_nexpj.t

    }
    val normalizer = breeze.linalg.sum(phi, Axis._0)

    phi(::, words(0).toInt)  :*= 1.0 / (normalizer( words(0).toInt))
    val tmp2 = phi(::, words(0).toInt)

    val psm =  tmp2 *:* (1.0 * counts(0) )//
    // counts(0).toInt
    val tsum =  psm
    //, Axis._1
    //to do : sum is to get rid of second axis?
     val r = tsum + this.alpha_0


    this.alpha_t0(n,::) := r.t
    phi

  }

  def updatePhi(x: DenseVector[String]): DenseVector[DenseMatrix[Double]] ={
    val D = x.length
    //      initial 3d array[D,K,dim]
    //val shape:Array[Int] = Array[Int](D,this.K, this.dim)
    val tmp = DenseMatrix.zeros[Double](this.K, this.dim)
    val phi= DenseVector.zeros[DenseMatrix[Double]](D)

    for(idx <- 0 until phi.length){
      phi(idx) = DenseMatrix.zeros(this.K, this.dim)
    }


    for(nn <- 0 until D){
      val row = updatePhiN(x, nn)
      phi(nn) = row
    }
    phi
  }


  def wordsInDocument(
      phi_dk: DenseVector[Double],
      voc_dic: collection.mutable.Map[String,Long],
      x:String, data_from: Boolean
  ): Array[Double] = {
    var A = Array.fill(phi_dk.length)(0.0)
    val (words,counts)=parseDocuments(x,voc_dic,data_from)
    counts(0) = (phi_dk(words(0).toInt) * counts(0)).toInt
    A(words(0).toInt) = counts(0)
    A
  }
  """
    Parse a document into a list of word ids and a list of counts,
    or parse a set of documents into two lists of lists of word ids
    and counts.
    Arguments:
    docs:  List of D documents. Each document must be represented as
           a single string. (Word order is unimportant.) Any
           words not in the vocabulary will be ignored.
    vocab: Dictionary mapping from words to integer ids.
    Returns a pair of lists of lists.
    The first, wordids, says what vocabulary tokens are present in
    each document. wordids[i][j] gives the jth unique token present in
    document i. (Don't count on these tokens being in any particular
    order.)
    The second, wordcts, says how many times each vocabulary token is
    present. wordcts[i][j] is the number of times that the token given
    by wordids[i][j] appears in document i.
  """
  def parseDocuments(docs: String, vocab: collection.mutable.Map[String,Long],
                     data_from: Boolean): (Array[Long],Array[Int])={
    val D = docs.length
    var wordids = new Array[Long](0)
    var wordcts = new Array[Int](0)
    var ddict = mutable.Map[Long,Int]()
    val low_docs = docs.toLowerCase.split(" ")
    for ( w <- low_docs.iterator){

      val word = w.trim
      if(vocab.keys.exists(_ ==word)){
        val wordtoken = vocab.find(_._1 == word).get._2

        if(!ddict.keys.exists(_==wordtoken)){
          ddict += (wordtoken.toLong -> 1)
        }else{
          ddict.update(wordtoken, ddict.find(_._1 == wordtoken).get._2 + 1)
        }

      }
    }
    wordids = ddict.keySet.toArray
    wordcts = ddict.values.toArray

    if(wordids.length==0){
      print("words.length==0!!")
      var wordids = new Array[Long](1)
      var wordcts = new Array[Int](1)
      wordids(0)=20
      wordcts(0)=1
      //return phi
      (wordids,wordcts)
    }

    (wordids,wordcts)
  }


  /**
    * Log Sum Exp with overflow protection using the identity:
    * For any a: $\log \sum_{n=1}^N \exp\{x_n\} = a + \log \sum_{n=1}^N \exp\{x_n - a\}$
    */
  def logSumExp(x: BDV[Double]): Double = {
    val a = max(x)
    a + log(sum(exp(x -:- a)))
  }

  /**
    * For theta ~ Dir(alpha), computes E[log(theta)] given alpha. Currently the implementation
    * uses [[breeze.numerics.digamma]] which is accurate but expensive.
    */
  def dirichletExpectation(alpha: BDV[Double]): BDV[Double] = {
    digamma(alpha) - digamma(sum(alpha))
  }


  /**
    * Computes [[dirichletExpectation()]] row-wise, assuming each row of alpha are
    * Dirichlet parameters.
    */
  def dirichletExpectation(alpha: BDM[Double]): BDM[Double] = {
    val rowSum = sum(alpha(breeze.linalg.*, ::))
    val digAlpha = digamma(alpha)
    val digRowSum = digamma(rowSum)
    val result = digAlpha(::, breeze.linalg.*) - digRowSum
    result
  }



  def evPsi(alpha: Array[Double]): DenseVector[Double]={
    val t = digamma(alpha)
    DenseVector(t)
  }

  def evPsi(alpha: Double): Double={
    digamma(alpha)
  }

  def updateLambda(
    x: DenseVector[String],
    phi: DenseVector[DenseMatrix[Double]],
    scale_of_step: Double
  ): Unit = {
    for (i <- 0 until K) {
      expectationPhi(x,i,phi,scale_of_step)
    }
  }

  def findLambda(
    x: DenseVector[String],
    scale_of_step: Double,
    etap: DenseMatrix[Double],
    with_test: Int = 1): DenseMatrix[Double] = {
    //# intilize phi

    this.alpha_t0 = DenseMatrix(Gamma(100.0, 1.0/100.0).sample(x.length * K).toArray).reshape(x.length, K)
    val eta = etap
    this.eta_t0 = eta.copy.reshape(K, dim)

    // compute local variable parameters
    var iteration = 0
    var t1=0.0
    do{
      iteration += 1
      //println( "iteration of : " + iteration )
      val alpha_old  =alpha_t0.copy
      this.phi = updatePhi(x)

      val absv = abs(this.alpha_t0 - alpha_old)
      t1 =  mean(absv)


    } while(iteration < max_iter || (t1 > this.nu))


    updateLambda(x, phi, scale_of_step)
    val eta_new = eta_t0.copy
    eta_new

  }


}
