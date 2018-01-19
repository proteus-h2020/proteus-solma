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

package eu.proteus.solma.lasso

import org.apache.flink.ml.math.Breeze._
import eu.proteus.solma.lasso.Lasso.{LassoParam, OptionLabeledVector}
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import eu.proteus.solma.lasso.algorithm.{FlatnessMappingAlgorithm, LassoBasicAlgorithm}
import eu.proteus.solma.pipeline.TransformDataStreamOperation
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import scala.util.{Failure, Success, Try}

class FitTrigger[W <: GlobalWindow] extends Trigger[LassoStreamEvent, W] {
  override def onElement(element: LassoStreamEvent, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    element match {
      case Right(ev) => TriggerResult.FIRE
      case Left(ev) => TriggerResult.CONTINUE
    }
  }

  override def clear(w: W, triggerContext: TriggerContext): Unit = ???

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
}

class FitWindowFunction extends WindowFunction[LassoStreamEvent, Iterable[OptionLabeledVector], Long, GlobalWindow] {

  def apply(key: Long, window: GlobalWindow, input: Iterable[LassoStreamEvent],
            out: Collector[Iterable[OptionLabeledVector]]): Unit =
  {
    if (input.toVector.length > 1) {
      val inputStreamEvents = input.filter(x => x.isLeft).map(x => x.left.get)
      val labelStreamEvent = Try(input.filter(x => x.isRight).map(x => x.right.get).head)
      val poses = inputStreamEvents.toVector.map(x => x.pos._2)

      val interpolatedLabels = labelStreamEvent match {
        case Success(x) => {
          var labels = Vector[(Double, Double)]()

          for (i <- labelStreamEvent.get.labels.data.indices) {
            labels = (labelStreamEvent.get.poses(i), labelStreamEvent.get.labels(i)) +: labels
          }
          new FlatnessMappingAlgorithm(poses, labels).apply
        }
        case Failure(x) => Vector[Double]()
      }

      val processedEvents: Iterable[OptionLabeledVector] = inputStreamEvents.zipWithIndex.map(zipped =>
        labelStreamEvent match {
          case Success(x) => Left (((zipped._1.pos, zipped._1.data.asBreeze), interpolatedLabels(zipped._2)))
          case Failure(x) => Right((zipped._1.pos, zipped._1.data.asBreeze))
        }
      )

      out.collect(processedEvents)
    }
  }
}

class LassoDFStreamTransformOperation[T <: LassoStreamEvent](workerParallelism: Int, psParallelism: Int,
                                                             pullLimit: Int, featureCount: Int,
                                                             rangePartitioning: Boolean, iterationWaitTime: Long,
                                                             allowedLateness: Long)
    extends TransformDataStreamOperation[LassoDelayedFeedbacks, LassoStreamEvent, Either[((Long, Double), Double),
      (Int, LassoParam)]]{

    override def transformDataStream(instance: LassoDelayedFeedbacks,
                                     transformParameters: ParameterMap,
                                     rawInput: DataStream[LassoStreamEvent]):
    DataStream[Either[((Long, Double), Double), (Int, LassoParam)]] = {

      def selectKey(event: LassoStreamEvent): Long = {
        event match {
          case Left(ev) => ev.pos._1
          case Right(ev) => ev.label
        }
      }
      val processedInput = rawInput.keyBy(x => selectKey(x)).window(GlobalWindows.create())
        .allowedLateness(Time.minutes(allowedLateness))
        .trigger(new FitTrigger()).apply(new FitWindowFunction()).flatMap(x => x)

      val output = LassoParameterServer.transformLasso(None)(processedInput, workerParallelism, psParallelism,
        lassoMethod = LassoBasicAlgorithm.buildLasso(), pullLimit, featureCount, rangePartitioning, iterationWaitTime)
      output
    }

  }
