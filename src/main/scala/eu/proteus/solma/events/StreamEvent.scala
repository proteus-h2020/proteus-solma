/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.proteus.solma.events

import org.apache.flink.ml.math.Vector

/**
  * This is the base trait for modeling the stream events
  * describing ingested samples. Each sample contains a subset
  * of the features on which a ML model is trained.
  * slice the indexes of the features that describe the sample
  * data the features that describe the sample
  */
trait StreamEvent extends Serializable
{
  def slice: IndexedSeq[Int]
  def data: Vector
}
