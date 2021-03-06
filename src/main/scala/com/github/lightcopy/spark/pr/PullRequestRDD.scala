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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.json4s.jackson.{JsonMethods => Json}

import scalaj.http.HttpResponse

/**
 * [[PullRequestInfo]] stores partial information about pull request that is used in partiitoning.
 */
private[pr] case class PullRequestInfo(
    id: Int,
    number: Int,
    url: String,
    updatedAt: String,
    token: Option[String],
    cachePath: Option[String]) {
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
    private val schema: StructType,
    private val numSlices: Int)
  extends RDD[Row](sc, Nil) {

  // Broadcast Hadoop configuration to access it on each executor. This is similar to HadoopRDD
  private val confBroadcast = sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))

  private def getConf: Configuration = confBroadcast.value.value

  override def getPartitions: Array[Partition] = {
    val slices = PullRequestRDD.slice(data.toArray, numSlices)
    slices.indices.map { i => new PullRequestPartition(id, i, slices(i)) }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val buffer = new ArrayBuffer[Row]()
    for (info <- split.asInstanceOf[PullRequestPartition].iterator) {
      // perform request, convert result int row and append to buffer
      // check if persisted cache is available for into path
      logInfo(s"Processing $info")
      val responseBody = info.cachePath match {
        case Some(resolvedPath) =>
          val path = new HadoopPath(info.cachePath.get)
          val fs = path.getFileSystem(getConf)
          if (fs.exists(path)) {
            // read cached data from path
            logInfo(s"Found $path, loading data from cache")
            Utils.readPersistedCache(fs, path)
          } else {
            // perform request and save cached data to the file provided
            logInfo(s"Could not find cache for $path, fetching remote data")
            val response = PullRequestRDD.findPullOrFail(info.url, info.token)
            Utils.writePersistedCache(fs, path, response.body)
            response.body
          }
        case None =>
          // just perform request, no cache is available on read/write
          val response = PullRequestRDD.findPullOrFail(info.url, info.token)
          response.body
      }

      val row = PullRequestRDD.processResponseBody(schema, responseBody)
      buffer.append(row)
    }
    new InterruptibleIterator(context, buffer.toIterator)
  }
}

private[spark] object PullRequestRDD {
  /** Slice array into sequence of partition elements */
  def slice[T: ClassTag](array: Array[T], numSlices: Int): Seq[Seq[T]] = {
    require(numSlices >= 1, s"Positive number of slices required, found $numSlices slices")
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    positions(array.length, numSlices).map { case (start, end) =>
      array.slice(start, end).toSeq }.toSeq
  }

  /** Return response and check http code */
  def findPullOrFail(url: String, token: Option[String]): HttpResponse[String] = {
    val response = HttpUtils.pull(url, token).asString
    if (!response.isSuccess) {
      throw new RuntimeException(s"Request failed with code ${response.code}: ${response.body}")
    }
    response
  }

  /** Process response body as json string and convert it into Spark SQL Row */
  def processResponseBody(schema: StructType, responseBody: String): Row = {
    val json = Json.parse(responseBody).values.asInstanceOf[Map[String, Any]]
    Utils.jsonToRow(schema, json)
  }
}
