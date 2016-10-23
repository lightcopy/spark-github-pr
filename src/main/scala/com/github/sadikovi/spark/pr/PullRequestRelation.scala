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

import java.util.concurrent.TimeUnit

import scala.math.BigInt
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}

import org.json4s.jackson.{JsonMethods => Json}

import org.slf4j.LoggerFactory

import scalaj.http.{Http, HttpRequest, HttpResponse}

/**
 * [[PullRequestRelation]] is a table to store GitHub pull requests information, currently
 * only half of fields is stored, including pull request statistics. Schema is fairly wide with
 * nested fields, but those nested columns also have only partial information compare to
 * all available data in response JSON.
 * Newly updated pull requests are fetched only, there is no sorting supported at the moment.
 */
class PullRequestRelation(
    @transient val sqlContext: SQLContext,
    private val parameters: Map[String, String])
  extends BaseRelation with TableScan {

  private val logger = LoggerFactory.getLogger(getClass())
  val minBatchSize = 1
  val defaultBatchSize = 200
  val maxBatchSize = 1000

  // User and repository to fetch, together they create user/repository pair
  private[spark] val user: String = parameters.get("user") match {
    case Some(username) if username.trim.nonEmpty => username.trim
    case other => sys.error(
      "Expected 'user' option, none/empty provided. " +
      "'user' option is either GitHub username or organization name")
  }

  private[spark] val repo: String = parameters.get("repo") match {
    case Some(repository) if repository.trim.nonEmpty => repository.trim
    case other => sys.error(
      "Expected 'repo' option, none/empty provided. " +
      "'repo' options is repository name without username prefix")
  }
  logger.info(s"$user/$repo repository is selected")

  // Size of pull requests batch to preload, this set is used to parallelize work across executors
  private[spark] val batchSize: Int = parameters.get("batch") match {
    case Some(size) => try {
      val resolvedSize = size.toInt
      require(resolvedSize >= minBatchSize && resolvedSize <= maxBatchSize,
        s"Batch size $size is out of bound, expected positive integer " +
        s"between $minBatchSize and $maxBatchSize, found $resolvedSize")
      resolvedSize
    } catch {
      case err: NumberFormatException =>
        throw new RuntimeException(
          s"Invalid batch size $size, should be positive integer, see cause for more info", err)
    }
    case None => defaultBatchSize
  }
  logger.info(s"Batch size $batchSize is selected")

  // authentication token
  private[spark] val authToken: Option[String] = parameters.get("token") match {
    case token @ Some(_) => token
    case None =>
      logger.warn("Token is not provided, rate limit is low for non-authenticated requests")
      None
  }

  // persistent cache folder, must be either shared directory on local file system, or HDFS
  private[spark] val cacheDirectory: String = Utils.checkPersistedCacheDir(
    parameters.get("cacheDir") match {
      case Some(directory) => directory
      case None => "file:/tmp/.spark-github-pr"
    },
    sqlContext.sparkContext.hadoopConfiguration)
  logger.info(s"Using directory $cacheDirectory for persisted cache on each executor")

  override def schema: StructType = {
    StructType(
      StructField("id", IntegerType, false) ::
      StructField("url", StringType, true) ::
      // patch and diff complimentary urls
      StructField("html_url", StringType, true) ::
      StructField("diff_url", StringType, true) ::
      StructField("patch_url", StringType, true) ::
      // pull request metadata
      StructField("number", IntegerType, false) ::
      StructField("state", StringType, true) ::
      StructField("title", StringType, true) ::
      StructField("body", StringType, true) ::
      // pull request dates in UTC, can be null
      StructField("created_at", TimestampType, true) ::
      StructField("updated_at", TimestampType, true) ::
      StructField("closed_at", TimestampType, true) ::
      StructField("merged_at", TimestampType, true) ::
      // repository that pull request is open against
      StructField("base", StructType(
        // branch name
        StructField("ref", StringType, true) ::
        StructField("sha", StringType, true) ::
        // target repository
        StructField("repo", StructType(
          StructField("id", IntegerType, false) ::
          StructField("name", StringType, true) ::
          StructField("full_name", StringType, true) ::
          StructField("description", StringType, true) ::
          StructField("private", BooleanType, false) ::
          StructField("url", StringType, true) ::
          StructField("html_url", StringType, true) ::
          Nil), true) :: Nil), true) ::
      // user who opened pull request
      StructField("user", StructType(
        StructField("login", StringType, true) ::
        StructField("id", IntegerType, false) ::
        StructField("url", StringType, true) ::
        StructField("html_url", StringType, true) :: Nil), true) ::
      // pull request statistics
      StructField("merged", BooleanType, false) ::
      StructField("mergeable", BooleanType, false) ::
      StructField("comments", IntegerType, false) ::
      StructField("commits", IntegerType, false) ::
      StructField("additions", IntegerType, false) ::
      StructField("deletions", IntegerType, false) ::
      StructField("changed_files", IntegerType, false) ::
      Nil
    )
  }

  override def buildScan(): RDD[Row] = {
    // Based on resolved username and repository prepare request to fetch all repositories for the
    // batch size, then partition pull requests across executors, so each url is resolved per task
    logger.info(s"List pull requests for $user/$repo")
    val prs = cache.get(CacheKey(user, repo, batchSize))
    new PullRequestRDD(sqlContext.sparkContext, prs, schema)
  }

  /** List pull requests for user/repo provided returning batch, token for increasing rate limit */
  private def listPullRequests(
      user: String,
      repo: String,
      batchSize: Int,
      token: Option[String]): Seq[PullRequestInfo] = {
    val response = HttpUtils.pulls(user, repo, batchSize, token).asString
    listFromResponse(response, token)
  }

  // open for testing
  private[spark] def listFromResponse(
      response: HttpResponse[String],
      token: Option[String]): Seq[PullRequestInfo] = {
    if (!response.isSuccess) {
      throw new RuntimeException(s"Request failed with code ${response.code}: ${response.body}")
    }

    val rawData = Json.parse(response.body).values
    rawData.asInstanceOf[Seq[Map[String, Any]]].map { data =>
      try {
        val id = Utils.valueForKey[BigInt](data, "id").toInt
        val number = Utils.valueForKey[BigInt](data, "number").toInt
        val url = Utils.valueForKey[String](data, "url")
        val updatedAt = Utils.valueForKey[String](data, "updated_at")
        val createdAt = Utils.valueForKey[String](data, "created_at")
        // if update date does not exist, use create date instead, url should always be defined
        PullRequestInfo(id, number, url, Option(updatedAt).getOrElse(createdAt), token, None)
      } catch {
        case NonFatal(err) =>
          throw new RuntimeException(
            s"Failed to convert to 'PullRequestInfo', data=$data", err)
      }
    }
  }

  private val pullRequestLoader = new CacheLoader[CacheKey, Seq[PullRequestInfo]]() {
    override def load(key: CacheKey): Seq[PullRequestInfo] = {
      logger.info(s"Cache miss for key $key, fetching data")
      listPullRequests(key.user, key.repo, key.batchSize, authToken)
    }
  }

  private val onRemovalAction = new RemovalListener[CacheKey, Seq[PullRequestInfo]] {
    override def onRemoval(rm: RemovalNotification[CacheKey, Seq[PullRequestInfo]]): Unit = {
      logger.info(s"Evicting key ${rm.getKey}")
    }
  }

  // Relation cache for listing pull requests, expires after 1 minute
  private[spark] val cache: LoadingCache[CacheKey, Seq[PullRequestInfo]] =
    CacheBuilder.newBuilder().
      maximumSize(100).
      expireAfterWrite(1, TimeUnit.MINUTES).
      removalListener(onRemovalAction).
      build(pullRequestLoader)
}
