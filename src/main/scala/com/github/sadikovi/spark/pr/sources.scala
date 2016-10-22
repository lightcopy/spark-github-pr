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

import java.text.SimpleDateFormat
import java.sql.Timestamp

import scala.math.BigInt
import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scalaj.http.{Http, HttpRequest}

/** Generic utilities to work with pull requests and sending requests to GitHub */
private[spark] object HttpUtils {
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

/** Generic utils functionality */
private[spark] object Utils {
  /**
   * Get value for key from map, will cast value to type T.
   * @param data key-value pairs of data
   * @param key requested key for provided data
   * @return value for key in data
   */
  def valueForKey[T: ClassTag](data: Map[String, Any], key: String): T = {
    data.getOrElse(key, sys.error(s"Key $key does not exist")).asInstanceOf[T]
  }

  /**
   * Convert JSON parsed data into Row.
   * Resulting row should follow schema defined in [[PullRequestRelation]].
   */
  def jsonToRow(fields: StructType, json: Map[String, Any]): Row = {
    require(json != null)
    val resolvedValues = fields.map { field =>
      val value = valueForKey[Any](json, field.name)
      field.dataType match {
        case IntegerType =>
          if (value.isInstanceOf[BigInt]) value.asInstanceOf[BigInt].intValue else value
        case LongType =>
          if (value.isInstanceOf[BigInt]) value.asInstanceOf[BigInt].longValue else value
        case BooleanType | DoubleType | StringType => value
        case TimestampType =>
          if (value != null) {
            val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").parse(value.toString)
            new Timestamp(format.getTime)
          } else {
            value
          }
        case struct @ StructType(_) => jsonToRow(struct, value.asInstanceOf[Map[String, Any]])
        case other => sys.error(s"Unsupported data type $other for JSON parsing")
      }
    }
    Row.fromSeq(resolvedValues)
  }
}
