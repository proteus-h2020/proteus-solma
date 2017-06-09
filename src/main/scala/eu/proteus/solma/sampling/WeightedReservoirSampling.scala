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
import org.apache.flink.streaming.api.scala.createTypeInformation

import scala.collection.mutable
import scala.math.min
import scala.reflect.ClassTag


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

  //=====================================Extra=================================================

  case class inp[T](var streamCounter:Long,var weight:mutable.ArrayBuffer[Double],
                    var weightedreservoir:mutable.ArrayBuffer[T])

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
        implicit val typeInfo = createTypeInformation[inp[T]]
        implicit val retInfo = createTypeInformation[mutable.ArrayBuffer[T]]
        statefulStream.flatMapWithState((in, state: Option[inp[T]]) => {
          val (element, _) = in
          state match {
            case Some(curr) => {
              val weighinfo = curr
              var ret: Seq[mutable.ArrayBuffer[T]] = Seq()
              if (weighinfo.streamCounter < k) {
                weighinfo.weightedreservoir(weighinfo.streamCounter.toInt) =element
                ret = Seq(weighinfo.weightedreservoir.slice(0, weighinfo.streamCounter.toInt + 1))
                weighinfo.weight(weighinfo.streamCounter.toInt) = scala.math.pow(gen.nextDouble(), 1/element(0))

              } else {
                val key_current= scala.math.pow(gen.nextDouble(), 1/element(0))
               val T =  weighinfo.weight.reduceLeft(min)
                val T_index = weighinfo.weight.view.zipWithIndex.minBy(_._1)._2
                if (key_current > T){
                  weighinfo.weightedreservoir(T_index)= element
                  ret = Seq(weighinfo.weightedreservoir)
                  weighinfo.weight(T_index)=key_current
                }
              }
              weighinfo.streamCounter+=1
              (ret, Some(weighinfo))
            }
            case None => {
              val weightedreservoir = mutable.ArrayBuffer.fill[T](k)(null.asInstanceOf[T])
              weightedreservoir(0) = element
              val weight= mutable.ArrayBuffer.fill[Double](k)(null.asInstanceOf[Double])
              weight(0)=scala.math.pow(gen.nextDouble(), 1/element(0))
              (Seq(weightedreservoir.slice(0, 1)), Some(inp(1L,weight, weightedreservoir)))
            }
          }
        }
        )
      }
    }
  }
}
