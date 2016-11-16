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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset}

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class PullRequestStreamSourceSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    startSparkSession()
  }

  test("validate internal delay const") {
    PullRequestStreamSource.DELAY_NANOSECONDS should be (59 * 1e9)
  }

  test("validate internal delay const as ms") {
    PullRequestStreamSource.delayAsMs should be (PullRequestStreamSource.DELAY_NANOSECONDS / 1e6)
  }

  test("validate initial time snapshot") {
    val relation = new PullRequestRelation(spark.sqlContext, Map.empty)
    val source = new PullRequestStreamSource(spark.sqlContext, relation, "metadata")
    source.timeSnapshot should be (0L)
  }

  test("validate initial offset") {
    val relation = new PullRequestRelation(spark.sqlContext, Map.empty)
    val source = new PullRequestStreamSource(spark.sqlContext, relation, "metadata")
    source.nextOffset should be (Some(LongOffset(0L)))
  }

  test("check schema for stream source") {
    val relation = new PullRequestRelation(spark.sqlContext, Map.empty)
    val source = new PullRequestStreamSource(spark.sqlContext, relation, "metadata")
    source.schema should be (relation.schema)
  }

  test("get next offset for non-long offset") {
    val relation = new PullRequestRelation(spark.sqlContext, Map.empty)
    val source = new PullRequestStreamSource(spark.sqlContext, relation, "metadata")
    // offset is invalid - return itself
    source.nextOffset = Some(new Offset() {
      override def compareTo(other: Offset): Int = 0
    })
    source.getOffset should be (source.nextOffset)
  }

  test("get next offset for diff < internal delay") {
    val relation = new PullRequestRelation(spark.sqlContext, Map.empty)
    val source = new PullRequestStreamSource(spark.sqlContext, relation, "metadata")
    // should return unchanged offset, since internal delay is still applied
    source.nextOffset = Some(new LongOffset(1L))
    source.timeSnapshot = System.nanoTime
    source.getOffset should be (source.nextOffset)
  }

  test("get next offset for diff >= internal delay") {
    val relation = new PullRequestRelation(spark.sqlContext, Map.empty)
    val source = new PullRequestStreamSource(spark.sqlContext, relation, "metadata")
    // should return unchanged offset, since internal delay is still applied
    source.nextOffset = Some(new LongOffset(1L))
    source.timeSnapshot = 1L
    source.getOffset should be (Some(new LongOffset(2L)))
  }

  test("get batch for offset") {
    val relation = new PullRequestRelation(spark.sqlContext, Map.empty) {
      override def buildScan(): RDD[Row] = spark.sparkContext.emptyRDD[Row]
    }
    val source = new PullRequestStreamSource(spark.sqlContext, relation, "metadata")
    val df = source.getBatch(None, new LongOffset(1L))
    df.schema should be (source.schema)
    df.count should be (0)
  }
}
