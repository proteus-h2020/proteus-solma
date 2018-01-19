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

import breeze.linalg.{DenseMatrix, DenseVector}
import eu.proteus.solma.pipeline.{StreamPredictor, StreamTransformer}
import org.slf4j.Logger

object LassoDelayedFeedbacks {
  type UnlabeledBundleVectors = (List[Double], DenseMatrix[Double])
  type LabelBundleVectors = (List[Double], DenseVector[Double])
  type LabeledBundleVectors = (UnlabeledBundleVectors, LabelBundleVectors)
  type OptionLabeledBundleVectors = Either[LabeledBundleVectors, UnlabeledBundleVectors]

  private val Log: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

  def apply: LassoDelayedFeedbacks = {
    new LassoDelayedFeedbacks
  }

}

class LassoDelayedFeedbacks extends StreamTransformer[LassoDelayedFeedbacks]
  with StreamPredictor[LassoDelayedFeedbacks]
