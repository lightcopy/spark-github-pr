/*
 * Copyright 2016 Lightcopy
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

package com.github.lightcopy.spark.pr

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

/**
 * [[PullRequestStreamSource]] is built on top of pull request relation and simply provides ability
 * to query GitHub multiple times without any filtering on offsets. Every time batch will be
 * created based on current fetched data from GitHub, no options are adjusted, e.g. batch size.
 * Offsets for streaming are incremented after each successful batch of pull requests, with internal
 * polling interval applied.
 */
class PullRequestStreamSource(
    @transient private val sqlContext: SQLContext,
    @transient private val baseRelation: PullRequestRelation,
    private val metadataPath: String)
  extends Source {

  private val logger = LoggerFactory.getLogger(getClass)

  // Warn user if polling delay is less than internal polling interval, in this case it should
  // default to internal metric
  val streamingPollingDelay = sqlContext.getConf("spark.sql.streaming.pollingDelay")
  if (PullRequestStreamSource.delayAsMs > JavaUtils.timeStringAsMs(streamingPollingDelay)) {
    logger.warn(s"Internal delay is greater than streaming polling delay " +
      s"$streamingPollingDelay, and will bounded by ${PullRequestStreamSource.delayAsMs} ms")
  }

  // time snapshot to keep track of internal delay
  private[pr] var timeSnapshot: Long = 0L
  // next batch offset, just increment it by 1 for each batch
  private[pr] var nextOffset: Option[Offset] = Some(LongOffset(0L))

  /** Return exactly the same schema of base relation for stream source */
  override def schema: StructType = baseRelation.schema

  /** Return none offset, current implementation relies on updates from GitHub */
  override def getOffset: Option[Offset] = synchronized {
    nextOffset match {
      case Some(offset: LongOffset) =>
        val startTime = System.nanoTime
        val diff = startTime - timeSnapshot
        if (diff >= PullRequestStreamSource.DELAY_NANOSECONDS) {
          nextOffset = Some(offset + 1L)
          timeSnapshot = startTime
        } else {
          logger.debug(s"Polling delay is not reached with diff = $diff, wait for the next check")
        }
      case other =>
        logger.warn(s"Cannot process offset $other, wait for the next check")
    }
    nextOffset
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    sqlContext.createDataFrame(baseRelation.buildScan(), schema)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = { }
}

private[pr] object PullRequestStreamSource {
  // delay of 59 seconds in nanoseconds, represents internal polling interval, after which pull
  // requests are available
  val DELAY_NANOSECONDS = 59000000000L

  /** Return delay as milliseconds to compare with streaming configuration */
  def delayAsMs: Long = DELAY_NANOSECONDS / 1e6.toInt
}
