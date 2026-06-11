/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.flink.kubernetes

import org.apache.streampark.flink.kubernetes.watcher.{ConsecutiveFailureCounter, FlinkJobStatusWatcher}

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class FlinkJobStatusWatcherFailureTest {

  @Test
  def marksFailureOnlyWhenThresholdIsReached(): Unit = {
    val counter = new ConsecutiveFailureCounter[String](3)

    assertFalse(counter.recordFailure("etl2sr"))
    assertFalse(counter.recordFailure("etl2sr"))
    assertTrue(counter.recordFailure("etl2sr"))
    assertTrue(counter.recordFailure("etl2sr"))
  }

  @Test
  def successfulPollClearsConsecutiveFailures(): Unit = {
    val counter = new ConsecutiveFailureCounter[String](3)

    assertFalse(counter.recordFailure("etl2sr"))
    assertFalse(counter.recordFailure("etl2sr"))
    counter.clear("etl2sr")

    assertFalse(counter.recordFailure("etl2sr"))
  }

  @Test
  def delaysFirstJobStatusPollByConfiguredInterval(): Unit = {
    val conf = JobStatusWatcherConfig(
      requestTimeoutSec = 120,
      requestIntervalSec = 5,
      silentStateJobKeepTrackingSec = 60)

    assertEquals(5, FlinkJobStatusWatcher.initialDelaySec(conf))
  }

}
