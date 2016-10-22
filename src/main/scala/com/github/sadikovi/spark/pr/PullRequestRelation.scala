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

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}

import org.json4s.jackson.{JsonMethods => Json}

import org.slf4j.LoggerFactory

import scalaj.http.{Http, HttpRequest}

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
  private[spark] val token: Option[String] = parameters.get("token") match {
    case authToken @ Some(_) => authToken
    case None =>
      logger.warn("Token is not provided, rate limit is low for non-authenticated requests")
      None
  }

  override def schema: StructType = {
    StructType(
      StructField("id", IntegerType, false) ::
      StructField("url", StringType, false) ::
      // patch and diff complimentary urls
      StructField("html_url", StringType, true) ::
      StructField("diff_url", StringType, true) ::
      StructField("patch_url", StringType, true) ::
      // pull request metadata
      StructField("number", IntegerType, false) ::
      StructField("state", StringType, false) ::
      StructField("title", StringType, true) ::
      StructField("body", StringType, true) ::
      // pull request dates, can be null
      StructField("created_at", DateType, true) ::
      StructField("updated_at", DateType, true) ::
      StructField("closed_at", DateType, true) ::
      StructField("merged_at", DateType, true) ::
      // repository that pull request is open against
      StructField("base", StructType(
        // branch name
        StructField("ref", StringType, true) ::
        StructField("sha", StringType, true) ::
        // target repository
        StructField("repo", StructType(
          StructField("id", IntegerType, false) ::
          StructField("name", StringType, false) ::
          StructField("full_name", StringType, false) ::
          StructField("description", StringType, true) ::
          StructField("private", BooleanType, false) ::
          StructField("url", StringType, false) ::
          StructField("html_url", StringType, true) ::
          Nil), true) :: Nil), true) ::
      // user who opened pull request
      StructField("user", StructType(
        StructField("login", StringType, false) ::
        StructField("id", IntegerType, false) ::
        StructField("url", StringType, false) ::
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
    val response = PullRequestSource.pulls(user, repo, batchSize, token).asString
    if (!response.isSuccess) {
      throw new RuntimeException(s"Request failed with code ${response.code}: ${response.body}")
    }

    val rawData = Json.parse(response.body).values
    val prs: Seq[PullRequestInfo] = rawData.asInstanceOf[Seq[Map[String, Any]]].map { data =>
      try {
        val id = valueForKey[Int](data, "id")
        val number = valueForKey[Int](data, "number")
        val url = valueForKey[String](data, "url")
        val updatedAt = valueForKey[String](data, "updated_at")
        val createdAt = valueForKey[String](data, "created_at")
        // if update date does not exist, use create date instead, url should always be defined
        PullRequestInfo(id, number, url, Option(updatedAt).getOrElse(createdAt))
      } catch {
        case NonFatal(err) =>
          throw new RuntimeException(
            s"Failed to convert to 'PullRequestInfo', body=${response.body}", err)
      }
    }
    logger.debug(s"Processed ${prs.length} pull requests")

    new PullRequestRDD(sqlContext.sparkContext, prs)
  }

  /** Get value for key from map, will cast value to type T */
  private[spark] def valueForKey[T: ClassTag](data: Map[String, Any], key: String): T = {
    data.getOrElse(key, sys.error(s"Key $key does not exist")).asInstanceOf[T]
  }
}

/**
 * Generic utilities to work with pull requests and sending requests to GitHub.
 */
private[spark] object PullRequestSource {
  val baseURL = "https://api.github.com"

  /**
   * Make request to GitHub to return batch of pull requests.
   * Note that only first page is retrieved.
   * @param user GitHub username/organization
   * @param repo repository name
   * @param batch how many pull requests fetch per page
   * @param token optional authentication token to increase rate limit
   * @return HttpRequest
   */
  def pulls(
      user: String,
      repo: String,
      batch: Int,
      token: Option[String]): HttpRequest = {
    require(user.nonEmpty, "'user' parameter is empty")
    require(repo.nonEmpty, "'repo' parameter is empty")
    require(batch > 0, s"Non-positive batch size $batch")
    require(!user.contains("/"), "'user' parameter contains '/' which will alter URL")
    require(!repo.contains("/"), "'repo' parameter contains '/' which will alter URL")
    val url = s"$baseURL/repos/$user/$repo/pulls"
    val request = Http(url).method("GET").param("per_page", s"$batch")
    if (token.isDefined) request.header("Authorization", s"token ${token.get}") else request
  }

  /**
   * Fetch pull request for a specific url.
   * @param url fully-qualified URL, comes from JSON field "url"
   * @param token optional token to increase rate limit
   * @return HttpRequest
   */
  def pull(url: String, token: Option[String]): HttpRequest = {
    val request = Http(url).method("GET")
    if (token.isDefined) request.header("Authorization", s"token ${token.get}") else request
  }

  /**
   * Fetch pull request for user, repo and number.
   * @param user GitHub username/organization
   * @param repo repository name
   * @param number pull request number
   * @param token optional token to increase rate limit
   * @return HttpRequest
   */
  def pull(user: String, repo: String, number: Int, token: Option[String]): HttpRequest = {
    require(user.nonEmpty, "'user' parameter is empty")
    require(repo.nonEmpty, "'repo' parameter is empty")
    require(number >= 0, s"Negative pull request number $number")
    require(!user.contains("/"), "'user' parameter contains '/' which will alter URL")
    require(!repo.contains("/"), "'repo' parameter contains '/' which will alter URL")
    val url = s"$baseURL/repos/$user/$repo/pulls/$number"
    pull(url, token)
  }
}
