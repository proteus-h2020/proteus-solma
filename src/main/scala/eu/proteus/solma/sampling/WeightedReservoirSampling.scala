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





/*---Inputs-----------

---References--------
* P.S. Efraimidis and P.G. Spirakis. Weighted random sampling with a reservoir. Information Processing Letters, 97(5):181–185, 2006.
---Author-----------
* written by Wenjuan Wang, Department of Computing, Bournemouth University
* Email: wangw@bournemouth.ac.uk
*/
@Proteus
class WeightedReservoirSampling extends  StreamTransformer[WeightedReservoirSampling]{
  import WeightedReservoirSampling._
  def setWeightedReservoirSize(size: Int): WeightedReservoirSampling = {
    parameters.add(WeightedReservoirSize, size)

    this
  }
}



object WeightedReservoirSampling{

  // ====================================== Parameters =============================================

  case object WeightedReservoirSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(4)
  }


  // ==================================== Factory methods ==========================================

  def apply(): WeightedReservoirSampling = {
    new WeightedReservoirSampling()
  }


  // ==================================== Operations ==========================================


  implicit def fitNoOp[T] = {
    new StreamFitOperation[WeightedReservoirSampling, T]{
      override def fit(
                        instance: WeightedReservoirSampling,
                        fitParameters: ParameterMap,
                        input: DataStream[T])
      : Unit = {}
    }
  }


  implicit def treansformWeightedReservoirSampling[T <: Vector : TypeInformation : ClassTag] = {
    new TransformDataStreamOperation[WeightedReservoirSampling, T, mutable.ArrayBuffer[T]] {
      override def transformDataStream(
                                        instance: WeightedReservoirSampling,
                                        transformParameters: ParameterMap,
                                        input: DataStream[T])
      : DataStream[mutable.ArrayBuffer[T]]= {
        val resultingParameters = instance.parameters ++ transformParameters
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[T](input,resultingParameters.get(PartitioningOperation))
        val k = resultingParameters(WeightedReservoirSize)
        val gen = new XORShiftRandom()
        implicit val typeInfo = TypeInformation.of(classOf[(Long,mutable.ArrayBuffer[Double], mutable.ArrayBuffer[T])])
        implicit val retInfo = TypeInformation.of(classOf[mutable.ArrayBuffer[T]])
        statefulStream.flatMapWithState((in, state: Option[(Long,mutable.ArrayBuffer[Double], mutable.ArrayBuffer[T])]) => {



          val (element, _) = in
          state match {
            case Some(curr) => {
              val (streamCounter, weight, weightedreservoir) = curr

              var ret: Seq[mutable.ArrayBuffer[T]] = Seq()

              if (streamCounter < k) {

                weightedreservoir(streamCounter.toInt) =element
                ret = Seq(weightedreservoir.slice(0, streamCounter.toInt + 1))
                weight(streamCounter.toInt) = scala.math.pow(gen.nextDouble(), 1/element(0))

              } else {



                val key_current= scala.math.pow(gen.nextDouble(), 1/element(0))
               // val T =  weight.min
               // val T_index = weight.indexOf(T)
               val T =  weight.reduceLeft(min)
                val T_index = weight.view.zipWithIndex.minBy(_._1)._2
                if (key_current > T){

                  weightedreservoir(T_index)= element
                  ret = Seq(weightedreservoir)
                  weight(T_index)=key_current
                }



              }
              (ret, Some((streamCounter + 1,weight, weightedreservoir)))



            }

            case None => {
              val weightedreservoir = mutable.ArrayBuffer.fill[T](k)(null.asInstanceOf[T])
              weightedreservoir(0) = element
              val weight= mutable.ArrayBuffer.fill[Double](k)(null.asInstanceOf[Double])
              weight(0)=scala.math.pow(gen.nextDouble(), 1/element(0))
              (Seq(weightedreservoir.slice(0, 1)), Some((1L,weight, weightedreservoir)))

            }
          }




        }

        )
      }


    }
  }





}
