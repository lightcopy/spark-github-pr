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

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
 * [[PullRequestStreamSource]] is built on top of pull request relation and simply provides ability
 * to query GitHub multiple times without any filtering on offsets. Every time batch will be
 * created based on current fetched data from GitHub, no options are adjusted, e.g. batch size.
 */
class PullRequestStreamSource(
    @transient private val sqlContext: SQLContext,
    @transient private val baseRelation: PullRequestRelation,
    private val metadataPath: String)
  extends Source {

  /** Return exactly the same schema of base relation for stream source */
  override def schema: StructType = baseRelation.schema

  /** Return none offset, current implementation relies on updates from GitHub */
  override def getOffset: Option[Offset] = Some(new LongOffset(0))

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    sqlContext.createDataFrame(baseRelation.buildScan, schema)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = { }
}
