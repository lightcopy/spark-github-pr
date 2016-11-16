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

import java.text.SimpleDateFormat
import java.sql.Timestamp

import scala.math.BigInt
import scala.reflect.ClassTag

import org.apache.commons.io.IOUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scalaj.http.{Http, HttpRequest}

/**
 * Cache key for user/repo retrieval.
 * 'authToken' and 'cacheDirectory' are mainly part of the key because of global cache, it is easier
 * to pass values through key. Note that comparison is only done on user - repo - batchSize, since
 * both auth token and directory do not force different cache value.
 */
private[spark] case class CacheKey(
    user: String,
    repo: String,
    batchSize: Int,
    authToken: Option[String],
    cacheDirectory: Option[String]) {

  override def hashCode(): Int = {
    batchSize + 31 * (user.hashCode + 31 * repo.hashCode)
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null || !obj.isInstanceOf[CacheKey]) return false
    val that = obj.asInstanceOf[CacheKey]
    this.user == that.user && this.repo == that.repo && this.batchSize == that.batchSize
  }

  override def toString(): String = {
    val authTokenStr = if (authToken.isDefined) "Some(*****)" else "None"
    s"${getClass.getSimpleName}(user=$user, repo=$repo, batchSize=$batchSize, " +
      s"authToken=$authTokenStr, cacheDirectory=$cacheDirectory)"
  }
}

/** Generic utilities to work with pull requests and sending requests to GitHub */
private[spark] object HttpUtils {
  val baseURL = "https://api.github.com"

  /**
   * Make request to GitHub to return batch of pull requests.
   * Note that only first page is retrieved.
   * @param user GitHub username/organization
   * @param repo repository name
   * @param pageSize how many pull requests fetch per page
   * @param pageNumber page number, 1-based
   * @param token optional authentication token to increase rate limit
   * @return HttpRequest
   */
  def pulls(
      user: String,
      repo: String,
      pageSize: Int,
      pageNumber: Int,
      token: Option[String]): HttpRequest = {
    require(user.nonEmpty, "'user' parameter is empty")
    require(repo.nonEmpty, "'repo' parameter is empty")
    require(pageSize > 0, s"Non-positive batch size $pageSize")
    require(pageNumber > 0, s"Non-positive page number $pageNumber")
    require(!user.contains("/"), "'user' parameter contains '/' which will alter URL")
    require(!repo.contains("/"), "'repo' parameter contains '/' which will alter URL")
    val url = s"$baseURL/repos/$user/$repo/pulls"
    val request = Http(url).method("GET").
      param("per_page", s"$pageSize").
      param("page", s"$pageNumber")
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
        case BooleanType | DoubleType | StringType =>
          if (!field.nullable && value == null) {
            throw new IllegalStateException(s"Non-nullable field ${field.name} has value $value")
          }
          value
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

  /** Filename for persisted cache */
  def persistedFilename(id: Int, date: String): String = {
    val dateKey = date.replaceAllLiterally(":", "=").replaceAllLiterally(" ", "=")
    s"pr-$id-$dateKey.cache"
  }

  /** Read content as UTF-8 String from provided file */
  def readPersistedCache(fs: FileSystem, path: HadoopPath): String = {
    val in = fs.open(path)
    try {
      IOUtils.toString(in, "UTF-8")
    } finally {
      in.close()
    }
  }

  /** Write content as UTF-8 String into provided file path, file is ovewritten on next attempt */
  def writePersistedCache(fs: FileSystem, path: HadoopPath, content: String): Unit = {
    val out = fs.create(path, true)
    try {
      IOUtils.write(content, out, "UTF-8")
    } finally {
      out.close()
    }
  }

  /** Check if directory exists or create it, verify that read/write access granted */
  private def checkPersistedCacheDir(fs: FileSystem, directory: HadoopPath): HadoopPath = {
    if (!fs.exists(directory)) {
      fs.mkdirs(directory, FsPermission.valueOf("drwxrwxrwx"))
    }
    val status = fs.getFileStatus(directory)
    require(status.isDirectory, s"$directory is not a directory")
    require(status.getPermission.getUserAction.implies(FsAction.READ_WRITE),
      s"Expected read/write access for $directory, found ${status.getPermission}")
    status.getPath
  }

  /** Wrapper to get verified path as String */
  def checkPersistedCacheDir(dir: String, conf: Configuration): String = {
    val path = new HadoopPath(dir)
    checkPersistedCacheDir(path.getFileSystem(conf), path).toString
  }

  /** List of attempts of either 'maxPageSize' size or 'batchSize % maxPageSize' size */
  def attempts(batchSize: Int, maxPageSize: Int): Seq[Int] = {
    require(batchSize > 0, s"Expected positive batch size, found $batchSize")
    require(maxPageSize > 0, s"Expected positive max page size, found $maxPageSize")
    val seq = (1 to batchSize / maxPageSize).map { x => maxPageSize } :+ (batchSize % maxPageSize)
    seq.filter(_ != 0)
  }
}
