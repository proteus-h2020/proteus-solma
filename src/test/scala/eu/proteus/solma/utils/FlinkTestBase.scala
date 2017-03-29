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

package eu.proteus.solma.utils

import eu.proteus.annotations.Proteus
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.util.TestStreamEnvironment
import org.apache.flink.test.util.TestBaseUtils
import org.scalatest.{BeforeAndAfter, Suite}

@Proteus
trait FlinkTestBase extends BeforeAndAfter {
  that: Suite =>

  var cluster: Option[LocalFlinkMiniCluster] = None
  val parallelism = 4

  before {
    val config = new Configuration
    val cl = TestBaseUtils.startCluster(1, parallelism, false, false, true)
    TestStreamEnvironment.setAsContext(cl, parallelism)
    cluster = Some(cl)
  }

  after {
    cluster.foreach(c => {
      TestStreamEnvironment.unsetAsContext()
      TestBaseUtils.stopCluster(c, TestBaseUtils.DEFAULT_TIMEOUT)
    })
  }

}
