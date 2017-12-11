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

package eu.proteus.solma.utils

import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkTestUtils {

  case class SuccessException[T](content: T) extends Exception {
    override def toString: String = s"SuccessException($content)"
  }

  case class NoSuccessExceptionReceived() extends Exception

  def executeWithSuccessCheck[T](env: StreamExecutionEnvironment)(checker: T => Unit): Unit = {
    try {
      env.execute()
      throw NoSuccessExceptionReceived()
    } catch {
      case e: JobExecutionException =>
        val rootCause = Stream.iterate[Throwable](e)(_.getCause()).takeWhile(_ != null).last
        rootCause match {
          case successException: SuccessException[T] =>
            checker(successException.content)
          case otherCause =>
            throw e
        }
      case e: Throwable => throw e
    }
  }
}
