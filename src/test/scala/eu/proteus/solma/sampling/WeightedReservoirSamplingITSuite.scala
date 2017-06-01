package eu.proteus.solma.sampling



import eu.proteus.annotations.Proteus
import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}
/**
  * Created by dung on 31/05/17.
  */
@Proteus
class WeightedReservoirSamplingITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase  {

  behavior of "Flink's Weighted Reservoir Sampling"



  it should "perform weighted reservoir sampling" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setMaxParallelism(1)

    val stream = env.fromCollection(WeightedReservoirSamplingITSuite.data)

    val transformer = WeightedReservoirSampling()
      .setWeightedReservoirSize(4)

    transformer.transform(stream).print()

    env.execute("weighted reservoir sampling")
  }

}
object WeightedReservoirSamplingITSuite{

  val data: Seq[Vector] = List(
    DenseVector( Array(0.2,1.0, 0.3, 5.0)),
    DenseVector(Array(0.1,1.2, 4.2, 5.2)),
    DenseVector(Array(0.3,2.0, 3.2, 5.2)),
    DenseVector(Array(0.5,1.2, 3.5, 7.6)),
    DenseVector(Array(0.8,1.1, 8.8, 5.2)),
    DenseVector(Array(0.8,1.3, 7.2, 5.2)),
    DenseVector(Array(0.4,5.2, 3.1, 5.6)),
    DenseVector(Array(0.5,8.2, 3.1, 5.5)))

}
