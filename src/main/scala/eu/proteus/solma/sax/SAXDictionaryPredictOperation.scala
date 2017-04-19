package eu.proteus.solma.sax

import java.util.concurrent.atomic.AtomicLong
import java.util.{HashMap => JHashMap}

import eu.proteus.solma.pipeline.PredictDataStreamOperation
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * Predict operation using the dictionary.
 */
class SAXDictionaryPredictOperation[T] extends PredictDataStreamOperation[SAXDictionary, T, SAXPrediction]{

  /** Calculates the predictions for all elements in the [[DataStream]] input
   *
   * @param instance          The Predictor instance that we will use to make the predictions
   * @param predictParameters The parameters for the prediction
   * @param input             The DataSet containing the unlabeled examples
   * @return
   */
  override def predictDataStream(
    instance: SAXDictionary,
    predictParameters: ParameterMap,
    input: DataStream[T]): DataStream[SAXPrediction] = {

    val windowSize = instance.getNumberWords()

    val predictWindowFunction = new AllWindowFunction[T, SAXPrediction, GlobalWindow] {
      override def apply(
        window: GlobalWindow,
        input: Iterable[T],
        out: Collector[SAXPrediction]): Unit = {

        val freq = new JHashMap[String, AtomicLong]
        input.foreach(w => {
          freq.putIfAbsent(w.toString, new AtomicLong(0))
          freq.get(w.toString).incrementAndGet()
        })

        val prediction = instance.dictionary.get.predict(freq)
        out.collect(prediction)
      }
    }

    input.countWindowAll(windowSize).apply(predictWindowFunction)

  }
}
