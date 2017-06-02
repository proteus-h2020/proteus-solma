package eu.proteus.solma.sampling

import eu.proteus.annotations.Proteus
import eu.proteus.solma.pipeline.StreamEstimator.PartitioningOperation
import eu.proteus.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import eu.proteus.solma.utils.FlinkSolmaUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.math.Vector
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.XORShiftRandom

import scala.collection.mutable
import scala.math.min
import scala.reflect.ClassTag
import scala.util.Random




/* ---Inputs-------
---References------
* M. Al-Kateb, B. S. Lee and X. S. Wang, "Adaptive-Size Reservoir Sampling over Data Streams," 19th International Conference on Scientific and Statistical Database Management (SSDBM 2007),
 Banff, Alta., 2007, pp. 22-22.doi: 10.1109/SSDBM.2007.29
---Author-----------
* written by Wenjuan Wang, Department of Computing, Bournemouth University
* Email: wangw@bournemouth.ac.uk
*/
@Proteus
class AdaptiveReservoirSampling extends  StreamTransformer[AdaptiveReservoirSampling]{
  import AdaptiveReservoirSampling._
  def setAdaptiveReservoirSize(size: Int): AdaptiveReservoirSampling = {
    parameters.add(AdaptiveReservoirSize, size)

    this
  }


  def setAdaptiveReservoirNumberToChange(NumberToChange: Int): AdaptiveReservoirSampling = {
    parameters.add(AdaptiveReservoirNumberToChange, NumberToChange)

    this
  }

  def setAdaptiveReservoirThreshold(Threshold: Double): AdaptiveReservoirSampling = {
    parameters.add(AdaptiveReservoirThreshold, Threshold)

    this
  }
  def setAdaptiveReservoirNewSize(Ch: Int): AdaptiveReservoirSampling = {
    parameters.add(Change, Ch)

    this
  }


}



object AdaptiveReservoirSampling{

  // ====================================== Parameters =============================================

  case object AdaptiveReservoirSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(4)
  }
  case object AdaptiveReservoirNumberToChange extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(2)
  }
  case object AdaptiveReservoirThreshold extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.99)
  }
  case object Change extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(5)
  }

  //=====================================Extra=================================================
  def factorial(n: Int): Int = {   if (n == 0) 1 else n * factorial(n-1)  }
  def min_m (i: Int,ReservoirSize: Int,delta: Int,threshold: Double) : Int = {

    var m = delta
    var UC = 0.0
    var x = ReservoirSize
    var f1 = factorial(i)*factorial(m)*factorial(ReservoirSize+delta)*factorial(i+m-ReservoirSize-delta)
    var f2 = factorial(x)*factorial(i-x)*factorial(ReservoirSize+delta-x)*factorial(m-ReservoirSize-delta+x)*factorial(i+m)
    UC = UC + f1/f2
    while(UC <= threshold){
      m += 1
      UC = 0.0
      for (x <- math.max(0,ReservoirSize+delta-m) to ReservoirSize){
        f1 = factorial(i)*factorial(m)*factorial(ReservoirSize+delta)*factorial(i+m-ReservoirSize-delta)
        f2 = factorial(x)*factorial(i-x)*factorial(ReservoirSize+delta-x)*factorial(m-ReservoirSize-delta+x)*factorial(i+m)
        UC = UC + f1/f2
      }
    }
    m
  }

  // ==================================== Factory methods ==========================================

  def apply(): AdaptiveReservoirSampling = {
    new AdaptiveReservoirSampling()
  }


  // ==================================== Operations ==========================================


  implicit def fitNoOp[T] = {
    new StreamFitOperation[AdaptiveReservoirSampling, T]{
      override def fit(
                        instance: AdaptiveReservoirSampling,
                        fitParameters: ParameterMap,
                        input: DataStream[T])
      : Unit = {}
    }
  }


  implicit def treansformAdaptiveReservoirSampling[T <: Vector : TypeInformation : ClassTag] = {
    new TransformDataStreamOperation[AdaptiveReservoirSampling, T, mutable.ArrayBuffer[T]] {
      override def transformDataStream(
                                        instance: AdaptiveReservoirSampling,
                                        transformParameters: ParameterMap,
                                        input: DataStream[T])
      : DataStream[mutable.ArrayBuffer[T]] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[T](input, resultingParameters.get(PartitioningOperation))
        var k = resultingParameters(AdaptiveReservoirSize)
        val nboCh=resultingParameters(AdaptiveReservoirNumberToChange)
        val thresh=resultingParameters(AdaptiveReservoirThreshold)
        val changedReservoirSize = resultingParameters(Change)
        val gen = new XORShiftRandom()
        implicit val typeInfo = TypeInformation.of(classOf[(Long,Long,Long,mutable.ArrayBuffer[T],mutable.ArrayBuffer[T], mutable.ArrayBuffer[T])])
        implicit val retInfo = TypeInformation.of(classOf[mutable.ArrayBuffer[T]])
        var j : Int = 0
        var m : Int = 0
        var x : Int = 0
        var remainingSize = 0
        statefulStream.flatMapWithState((in, state: Option[(Long,Long,Long,mutable.ArrayBuffer[T],mutable.ArrayBuffer[T],mutable.ArrayBuffer[T])]) => {


          val (element,_)=in

          state match {
            case Some(curr) => {


              var (streamCounter1,streamCounter2,streamCounter3,adaptedReserS1,adaptedReserS2,adaptivereservoir) = curr

              var ret: Seq[mutable.ArrayBuffer[T]] = Seq()


              if (streamCounter1 < k) {

                adaptivereservoir(streamCounter1.toInt) =element

                streamCounter1 += 1
                streamCounter2+=1}


              else if (streamCounter1 == nboCh*k)
                {



                  if (streamCounter1==streamCounter2)
                  {
                    val  delta = changedReservoirSize - k
                    if (delta <= 0) {

                      adaptivereservoir =  Random.shuffle(adaptivereservoir.toList).take(changedReservoirSize).to[mutable.ArrayBuffer]
                      streamCounter1+=1
                      streamCounter2+=1

                    }else{
                      m = min_m(streamCounter1.toInt,k,delta,thresh)
                      if(m==delta){
                        x = changedReservoirSize - m
                      }else {
                        x = changedReservoirSize - m + gen.nextInt(m-delta)
                      }
                      remainingSize = changedReservoirSize - x
                      adaptedReserS1 = Random.shuffle(adaptivereservoir).take(x)
                      adaptedReserS2 = mutable.ArrayBuffer.fill[T](remainingSize)(null.asInstanceOf[T])
                      adaptedReserS2(0) = element
                      streamCounter2+=1
                    }
                  }else{
                    if (streamCounter2 < streamCounter1 + remainingSize){
                      adaptedReserS2(streamCounter3.toInt) = element
                      streamCounter3 += 1
                      streamCounter2+=1
                    } else {


                      j = gen.nextInt(m)

                      if (j < remainingSize)
                        adaptedReserS2(j)  = element
                      streamCounter2+=1
                    }
                    adaptivereservoir = adaptedReserS1 ++ adaptedReserS2
                    if (streamCounter2==streamCounter1+m){ streamCounter1+=m}
                  }
                  k = changedReservoirSize

                }else {
                  j = gen.nextInt(streamCounter1.toInt)
                  if (j < k)
                    if(adaptivereservoir.length<=j) {
                      adaptivereservoir=adaptivereservoir++mutable.ArrayBuffer.fill[T](j-adaptivereservoir.length+1)(null.asInstanceOf[T])

                      adaptivereservoir(j) = element
                    }




                  streamCounter1 += 1
                  streamCounter2 +=1
                }

              ret = Seq(adaptivereservoir.filter(i=>i != null))
              (ret, Some(streamCounter1,streamCounter2 ,streamCounter3,adaptedReserS1,adaptedReserS2, adaptivereservoir))


              }




            case None => {




              var adaptivereservoir = mutable.ArrayBuffer.fill[T](k)(null.asInstanceOf[T])
              var adaptedReserS1 = mutable.ArrayBuffer.fill[T](0)(null.asInstanceOf[T])
              var adaptedReserS2 = mutable.ArrayBuffer.fill[T](0)(null.asInstanceOf[T])
              adaptivereservoir(0)=element
              (Seq(adaptivereservoir.slice(0,1)),Some(1L,1L,1L,adaptedReserS1,adaptedReserS2,adaptivereservoir))



            }
          }

        }

        )
      }


    }
  }





}
