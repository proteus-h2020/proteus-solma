package eu.proteus.solma.sax

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
 * Tests for the SAX dictionary
 */
class SAXDictionaryITSuite extends FunSuite with Matchers with FlinkTestBase {

  test("Single class fit"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData : Seq[String] = List("aa", "bb", "cc", "aa", "bb", "cc")
    val dataset : DataSet[String] = env.fromCollection(trainingData)
    val toFit = dataset.map((_, 1))
    val saxDictionary = new SAXDictionary()
    saxDictionary.fit(toFit)
    val fitted = saxDictionary.getDictionary()
    assert(fitted.isDefined, "Expected fitted dictionary")
    assertResult(1, "Invalid number of classes")(fitted.get.bags.size())
  }

  test("Two classes fit"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData : Seq[String] = List("aa", "bb", "cc", "aa", "bb", "cc")
    val dataset : DataSet[String] = env.fromCollection(trainingData)
    val toFit1 = dataset.map((_, 1))
    val toFit2 = dataset.map((_, 2))
    val saxDictionary = new SAXDictionary()
    saxDictionary.fit(toFit1)
    saxDictionary.fit(toFit2)
    val fitted = saxDictionary.getDictionary()
    assert(fitted.isDefined, "Expected fitted dictionary")
    assertResult(2, "Invalid number of classes")(fitted.get.bags.size())
  }


}
