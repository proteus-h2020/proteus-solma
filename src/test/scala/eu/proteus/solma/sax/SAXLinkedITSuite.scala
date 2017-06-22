package eu.proteus.solma.sax

import java.io.File

import eu.proteus.solma.utils.FlinkTestBase
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.flink.api.scala.createTypeInformation

/**
 * Test suite for SAX + SAX Dictionary.
 */
class SAXLinkedITSuite extends FunSuite with Matchers with FlinkTestBase {

  val env = ExecutionEnvironment.getExecutionEnvironment

  val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

  /**
   * Train SAX using the same case as in SAXITSuite(Alphabet=2, PAA=2, WordSize=2)
   * The WordSize has been set to 1 for testing purposes.
   * @return A trained SAX.
   */
  def getTrainedSAX() : SAX = {
    val fittingData : Seq[Double] = List(2, 3, 4, 8, 8, 9, 8, 6, 6, 5, 5, 5, 6, 2, 1)
    val trainingDataSet : DataSet[Double] = this.env.fromCollection(fittingData)
    this.streamingEnv.setParallelism(4)
    this.streamingEnv.setMaxParallelism(4)
    val sax = new SAX().setPAAFragmentSize(2).setWordSize(1)
    sax.fit(trainingDataSet)
    sax.printInternalParameters()
    sax
  }

  /**
   * Train a SAX dictionary considering the usecase of SAX that will be used as input.
   * @return A trained SAX dictionary.
   */
  def getTrainedSAXDictionary() : SAXDictionary = {
    val trainingDataClass1 : Seq[String] = List("a", "b", "b")
    val dataset1 : DataSet[String] = env.fromCollection(trainingDataClass1)
    val toFit1 = dataset1.map((_, 1))
    val trainingDataClass2 : Seq[String] = List("b", "b")
    val dataset2 : DataSet[String] = env.fromCollection(trainingDataClass2)
    val toFit2 = dataset2.map((_, 1))
    val saxDictionary = new SAXDictionary().setNumberWords(2)
    saxDictionary.fit(toFit1)
    saxDictionary.fit(toFit2)
    saxDictionary
  }

  test("SAX + SAXDictionary, NumberWords=2"){
    val sax = this.getTrainedSAX()
    val saxDictionary = this.getTrainedSAXDictionary()

    val testingData : Seq[(Double, Int)] = List(
      (2, 0), (3, 0),
      (4, 0), (8, 0),
      (8, 0), (9, 0),
      (8, 0), (6, 0),
      (6, 0), (5, 0),
      (5, 0), (5, 0))
    val evalDataSet : DataStream[(Double, Int)] = streamingEnv.fromCollection(testingData)

    val saxTransformation = sax.transform(evalDataSet)
    val dictionaryMatching = saxDictionary.predict(saxTransformation)
    val r : Iterator[SAXPrediction] = dictionaryMatching.collect()
    val result = r.toList
    // Console.println("Result: " + result.mkString(", "))
    assertResult(3, "Invalid number of predictions")(result.size)

  }

}
