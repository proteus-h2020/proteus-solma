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

  test("Single class prediction"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData : Seq[String] = List("aa", "bb", "cc")
    val dataset : DataSet[String] = env.fromCollection(trainingData)
    val toFit = dataset.map((_, 1))
    val saxDictionary = new SAXDictionary()
    saxDictionary.fit(toFit)
    val fitted = saxDictionary.getDictionary()
    assert(fitted.isDefined, "Expected fitted dictionary")

    val evalData : Seq[String] = List(
      "aa", "bb", "cc",
      "aa", "bb", "dd",
      "aa", "dd", "dd",
      "dd", "dd", "dd"
    )
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamingEnv.setParallelism(4)
    streamingEnv.setMaxParallelism(4)
    val evalDataSet : DataStream[String] = streamingEnv.fromCollection(evalData)
    val evalDictionary = saxDictionary
      .setNumberWords(saxDictionary.getRecommendedNumberWords().getOrElse(3))

    val predictions = evalDictionary.predict[String, SAXPrediction](evalDataSet)
    val r : Iterator[SAXPrediction] = predictions.collect()
    val result = r.toList
    assertResult(4, "Invalid number of predictions")(result.size)
    assertResult(1.0, "Expecting perfect similarity")(result.head.similarity)
    assertResult(0.0, "Expecting 0 similarity")(result.last.similarity)
  }

  test("Two class prediction"){
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val trainingData1 : Seq[String] = List("aa", "bb", "cc")
    val dataset1 : DataSet[String] = env.fromCollection(trainingData1)
    val toFit1 = dataset1.map((_, 1))
    val trainingData2 : Seq[String] = List("dd", "ee", "ff")
    val dataset2 : DataSet[String] = env.fromCollection(trainingData2)
    val toFit2 = dataset2.map((_, 2))
    val saxDictionary = new SAXDictionary()
    saxDictionary.fit(toFit1)
    saxDictionary.fit(toFit2)
    val fitted = saxDictionary.getDictionary()
    assert(fitted.isDefined, "Expected fitted dictionary")

    val evalData : Seq[String] = List(
      "aa", "bb", "cc",
      "aa", "bb", "dd",
      "aa", "dd", "ee",
      "dd", "ee", "ff"
    )
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamingEnv.setParallelism(4)
    streamingEnv.setMaxParallelism(4)
    val evalDataSet : DataStream[String] = streamingEnv.fromCollection(evalData)
    val evalDictionary = saxDictionary.setNumberWords(3)

    val predictions = evalDictionary.predict[String, SAXPrediction](evalDataSet)
    val r : Iterator[SAXPrediction] = predictions.collect()
    val result = r.toList
    assertResult(4, "Invalid number of predictions")(result.size)
    assertResult(1.0, "Expecting perfect similarity")(result.head.similarity)
    assertResult("1", "Expecting match on class 1")(result.head.classId)
    assertResult(1.0, "Expecting perfect similarity")(result.last.similarity)
    assertResult("2", "Expecting match on class 2")(result.last.classId)

  }

}
