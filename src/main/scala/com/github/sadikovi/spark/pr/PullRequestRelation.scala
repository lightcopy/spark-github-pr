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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}

import org.slf4j.LoggerFactory

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
  private[pr] val user: String = parameters.get("user") match {
    case Some(username) if username.trim.nonEmpty => username.trim
    case other => sys.error(
      "Expected 'user' option, none/empty provided. " +
      "'user' option is either GitHub username or organization name")
  }

  private[pr] val repo: String = parameters.get("repo") match {
    case Some(repository) if repository.trim.nonEmpty => repository.trim
    case other => sys.error(
      "Expected 'repo' option, none/empty provided. " +
      "'repo' options is repository name without username prefix")
  }
  logger.info(s"$user/$repo repository is selected")

  // Size of pull requests batch to preload, this set is used to parallelize work across executors
  private[pr] val batchSize: Int = parameters.get("batch") match {
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
  private[pr] val token: Option[String] = parameters.get("token") match {
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
    null
  }
}
