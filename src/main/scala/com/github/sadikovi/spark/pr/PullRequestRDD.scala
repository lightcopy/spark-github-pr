/*
 * Copyright 2016 sadikovi
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

package com.github.sadikovi.spark.pr

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.json4s.jackson.{JsonMethods => Json}

/**
 * [[PullRequestInfo]] stores partial information about pull request that is used in partiitoning.
 */
private[pr] case class PullRequestInfo(
    id: Int,
    number: Int,
    url: String,
    updatedAt: String,
    token: Option[String]) {
  override def equals(other: Any): Boolean = other match {
    case that: PullRequestInfo =>
      this.id == that.id && this.number == that.number
    case _ => false
  }

  override def hashCode(): Int = (41 * id + number).toInt
}

/**
 * [[PullRequestPartition]] contains index of partition and provided pull request information,
 * e.g. number and url to access data.
 */
private[pr] class PullRequestPartition(
    val rddId: Long,
    val slice: Int,
    val info: Seq[PullRequestInfo])
  extends Partition with Serializable {

  def iterator: Iterator[PullRequestInfo] = info.toIterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: PullRequestPartition =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice
}

/**
 * [[PullRequestRDD]] computes each partition by executing REST request to fetch pull request data.
 * Currently every info instance is mapped to each partition, trying to maximize number of tasks,
 * as an opposite to minimizing scheduling latency.
 */
private[spark] class PullRequestRDD(
    sc: SparkContext,
    @transient private val data: Seq[PullRequestInfo],
    private val schema: StructType)
  extends RDD[Row](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    data.zipWithIndex.map { case (info, i) => new PullRequestPartition(id, i, Seq(info)) }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val buffer = new ArrayBuffer[Row]()
    for (info <- split.asInstanceOf[PullRequestPartition].iterator) {
      // perform request, convert result int row and append to buffer
      logInfo(s"Processing $info")
      val response = HttpUtils.pull(info.url, info.token).asString
      if (!response.isSuccess) {
        throw new RuntimeException(s"Request failed with code ${response.code}: ${response.body}")
      }

      val json = Json.parse(response.body).values.asInstanceOf[Map[String, Any]]
      val row = Utils.jsonToRow(schema, json)
      buffer.append(row)
    }
    new InterruptibleIterator(context, buffer.toIterator)
  }
}
