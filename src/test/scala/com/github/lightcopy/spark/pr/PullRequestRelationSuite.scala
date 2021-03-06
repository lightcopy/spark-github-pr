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

import java.io.IOException
import java.math.BigInteger
import java.sql.Timestamp

import scala.io.Source
import scala.math.BigInt

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

import scalaj.http.HttpResponse

import com.github.lightcopy.testutil.{HttpTest, SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class PullRequestRelationSuite extends UnitTestSuite with SparkLocal with HttpTest {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("return default username when none is provided") {
    val sqlContext = spark.sqlContext
    val relation = new PullRequestRelation(sqlContext, Map.empty)
    relation.user should be (PullRequestRelation.DEFAULT_USER)
  }

  test("extract empty username") {
    val sqlContext = spark.sqlContext
    var err = intercept[RuntimeException] {
      new PullRequestRelation(sqlContext, Map("user" -> ""))
    }
    err.getMessage.contains("Expected non-empty 'user' option") should be (true)

    err = intercept[RuntimeException] {
      new PullRequestRelation(sqlContext, Map("user" -> "  "))
    }
    err.getMessage.contains("Expected non-empty 'user' option") should be (true)
  }

  test("return default repository when none is provided") {
    val sqlContext = spark.sqlContext
    val relation = new PullRequestRelation(sqlContext, Map("user" -> "user"))
    relation.repo should be (PullRequestRelation.DEFAULT_REPO)
  }

  test("extract empty repository") {
    val sqlContext = spark.sqlContext
    var err = intercept[RuntimeException] {
      new PullRequestRelation(sqlContext, Map("user" -> "user", "repo" -> ""))
    }
    err.getMessage.contains("Expected non-empty 'repo' option") should be (true)

    err = intercept[RuntimeException] {
      new PullRequestRelation(sqlContext, Map("user" -> "user", "repo" -> "  "))
    }
    err.getMessage.contains("Expected non-empty 'repo' option") should be (true)
  }

  test("extract username, repository") {
    val sqlContext = spark.sqlContext
    var relation = new PullRequestRelation(sqlContext, Map("user" -> "abc", "repo" -> "def"))
    relation.user should be ("abc")
    relation.repo should be ("def")
    relation = new PullRequestRelation(sqlContext, Map("user" -> "ABC", "repo" -> "DEF"))
    relation.user should be ("ABC")
    relation.repo should be ("DEF")
    relation = new PullRequestRelation(sqlContext, Map("user" -> " abc ", "repo" -> " def "))
    relation.user should be ("abc")
    relation.repo should be ("def")
  }

  test("batch size conversion fail") {
    val sqlContext = spark.sqlContext
    var err = intercept[RuntimeException] {
      new PullRequestRelation(sqlContext,
        Map("user" -> "user", "repo" -> "repo", "batch" -> ""))
    }
    err.getMessage.contains("Invalid batch size") should be (true)

    err = intercept[RuntimeException] {
      new PullRequestRelation(sqlContext,
        Map("user" -> "user", "repo" -> "repo", "batch" -> "abc"))
    }
    err.getMessage.contains("Invalid batch size") should be (true)

    err = intercept[RuntimeException] {
      new PullRequestRelation(sqlContext,
        Map("user" -> "user", "repo" -> "repo", "batch" -> "  "))
    }
    err.getMessage.contains("Invalid batch size") should be (true)
  }

  test("batch size out of bound") {
    val sqlContext = spark.sqlContext
    var err = intercept[IllegalArgumentException] {
      new PullRequestRelation(sqlContext,
        Map("user" -> "user", "repo" -> "repo", "batch" -> "0"))
    }
    err.getMessage.contains("Batch size 0 is out of bound") should be (true)

    err = intercept[IllegalArgumentException] {
      new PullRequestRelation(sqlContext,
        Map("user" -> "user", "repo" -> "repo", "batch" -> "1001"))
    }
    err.getMessage.contains("Batch size 1001 is out of bound") should be (true)
  }

  test("select default or valid batch size") {
    val sqlContext = spark.sqlContext
    var relation = new PullRequestRelation(sqlContext, Map("user" -> "user", "repo" -> "repo"))
    relation.batchSize should be (PullRequestRelation.DEFAULT_BATCH_SIZE)

    relation = new PullRequestRelation(sqlContext,
      Map("user" -> "user", "repo" -> "repo", "batch" -> "140"))
    relation.batchSize should be (140)
  }

  test("select token if available") {
    val sqlContext = spark.sqlContext
    var relation = new PullRequestRelation(sqlContext,
      Map("user" -> "user", "repo" -> "repo", "token" -> "abc"))
    relation.authToken should be (Some("abc"))

    relation = new PullRequestRelation(sqlContext,
      Map("user" -> "user", "repo" -> "repo"))
    relation.authToken should be (None)
  }

  test("verify cache directory") {
    val sqlContext = spark.sqlContext
    var relation = new PullRequestRelation(sqlContext,
      Map("user" -> "user", "repo" -> "repo", "cacheDir" -> "file:/tmp/.spark-github-pr"))
    relation.cacheDirectory should be ("file:/tmp/.spark-github-pr")
  }

  test("create and verify cache directory") {
    val sqlContext = spark.sqlContext
    withTempDir { dir =>
      var relation = new PullRequestRelation(sqlContext,
        Map("user" -> "user", "repo" -> "repo", "cacheDir" -> s"$dir/test"))
      relation.cacheDirectory should be (s"file:$dir/test")
    }
  }

  test("test DefaultSource") {
    val source = new DefaultSource()
    val relation = source.createRelation(spark.sqlContext, Map("user" -> "user", "repo" -> "repo"))
    relation.isInstanceOf[PullRequestRelation] should be (true)
  }

  test("check main schema items") {
    val sqlContext = spark.sqlContext
    val relation = new PullRequestRelation(sqlContext, Map("user" -> "user", "repo" -> "repo"))
    relation.schema("id").dataType should be (IntegerType)
    relation.schema("url").dataType should be (StringType)
    relation.schema("number").dataType should be (IntegerType)
    relation.schema("state").dataType should be (StringType)
    relation.schema("title").dataType should be (StringType)
    relation.schema("body").dataType should be (StringType)
    relation.schema("base").dataType.isInstanceOf[StructType] should be (true)
    relation.schema("user").dataType.isInstanceOf[StructType] should be (true)
    // statistics
    relation.schema("commits").dataType should be (IntegerType)
    relation.schema("additions").dataType should be (IntegerType)
    relation.schema("deletions").dataType should be (IntegerType)
    relation.schema("changed_files").dataType should be (IntegerType)
  }

  test("pull request source - pulls() validation") {
    intercept[IllegalArgumentException] {
      HttpUtils.pulls("", "repo", 10, 1, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("user", "", 10, 1, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("a/b/c", "repo", 10, 1, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("user", "/a/b/c", 10, 1, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("user", "repo", 0, 1, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("user", "repo", 10, 0, None)
    }
  }

  test("pull request source - pulls() without token") {
    val request = HttpUtils.pulls("user", "repo", 7, 2, None)
    request.method should be ("GET")
    request.url should be ("https://api.github.com/repos/user/repo/pulls")
    request.params should be (List(("per_page", "7"), ("page", "2")))
    request.headers.toMap.get("Authorization") should be (None)
  }

  test("pull request source - pulls() with token") {
    val request = HttpUtils.pulls("user", "repo", 15, 1, Some("abc123"))
    request.method should be ("GET")
    request.url should be ("https://api.github.com/repos/user/repo/pulls")
    request.params should be (List(("per_page", "15"), ("page", "1")))
    request.headers.toMap.get("Authorization") should be (Some("token abc123"))
  }

  test("pull request source - pull() validation") {
    intercept[IllegalArgumentException] {
      HttpUtils.pull("", "repo", 12345, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pull("user", "", 12345, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pull("a/b/c", "repo", 10, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pull("user", "/a/b/c", 10, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pull("user", "repo", -1, None)
    }
  }

  test("pull request source - pull() with token") {
    val request = HttpUtils.pull("user", "repo", 12345, Some("abc123"))
    request.method should be ("GET")
    request.url should be ("https://api.github.com/repos/user/repo/pulls/12345")
    request.params.isEmpty should be (true)
    request.headers.toMap.get("Authorization") should be (Some("token abc123"))
  }

  test("pull request source - pull() without token") {
    val request = HttpUtils.pull("user", "repo", 12345, None)
    request.method should be ("GET")
    request.url should be ("https://api.github.com/repos/user/repo/pulls/12345")
    request.params.isEmpty should be (true)
    request.headers.toMap.get("Authorization") should be (None)
  }

  test("value for key - key does not exist") {
    val map: Map[String, Any] = Map("key1" -> "abc", "key2" -> 12, "key3" -> true)
    val err = intercept[RuntimeException] {
      Utils.valueForKey[String](map, "key4")
    }
    err.getMessage should be ("Key key4 does not exist")
  }

  test("value for key - casting exception") {
    val map: Map[String, Any] = Map("key1" -> "abc", "key2" -> 12, "key3" -> true)
    intercept[ClassCastException] {
      Utils.valueForKey[Double](map, "key1")
    }

    intercept[ClassCastException] {
      Utils.valueForKey[Int](map, "key3")
    }
  }

  test("value for key - correct retrieval") {
    val map: Map[String, Any] = Map("key1" -> "abc", "key2" -> 12, "key3" -> true)
    Utils.valueForKey[String](map, "key1") should be ("abc")
    Utils.valueForKey[Int](map, "key2") should be (12)
    Utils.valueForKey[Boolean](map, "key3") should be (true)
  }

  test("json to row - primitive schema") {
    val struct = StructType(
      StructField("a", IntegerType) ::
      StructField("b", BooleanType) ::
      StructField("c", DoubleType) ::
      StructField("d", LongType) ::
      StructField("e", StringType) :: Nil)
    val json: Map[String, Any] = Map("a" -> 12, "b" -> false, "c" -> 1.2, "d" -> 10L, "e" -> "abc")
    val row = Utils.jsonToRow(struct, json)
    row should be (Row(12, false, 1.2, 10L, "abc"))
  }

  test("json to row - complex schema") {
    val struct = StructType(
      StructField("a", IntegerType) ::
      StructField("b", BooleanType) ::
      StructField("c", StructType(
        StructField("c.a", StringType) ::
        StructField("c.b", IntegerType) ::
        StructField("c.c", BooleanType) :: Nil)
      ) ::
      StructField("d", StringType) ::
      StructField("e", StructType(
        StructField("e.a", StringType) ::
        StructField("e.b", IntegerType) ::
        StructField("e.c", DoubleType) :: Nil)
      ) :: Nil)

    val json: Map[String, Any] = Map(
      "a" -> 12,
      "b" -> true,
      "c" -> Map("c.a" -> "a", "c.b" -> 1, "c.c" -> false),
      "d" -> "d",
      "e" -> Map("e.a" -> "e", "e.b" -> 2, "e.c" -> 0.3))

    val row = Utils.jsonToRow(struct, json)
    row should be (Row(12, true, Row("a", 1, false), "d", Row("e", 2, 0.3)))
  }

  test("json to row - process big int values") {
    val struct = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) :: Nil)
    val json: Map[String, Any] = Map(
      "a" -> new BigInt(BigInteger.TEN),
      "b" -> new BigInt(BigInteger.ONE))
    val row = Utils.jsonToRow(struct, json)
    row should be (Row(10, 1L))
  }

  test("json to row - only fetch required fields") {
    val struct = StructType(
      StructField("a", IntegerType) ::
      StructField("e", StringType) :: Nil)
    val json: Map[String, Any] = Map("a" -> 12, "b" -> false, "c" -> 1.2, "d" -> 10L, "e" -> "abc")
    val row = Utils.jsonToRow(struct, json)
    row should be (Row(12, "abc"))
  }

  test("json to row - fail if key does not exist") {
    val struct = StructType(StructField("a", IntegerType) :: Nil)
    val json: Map[String, Any] = Map("c" -> 12)
    val err = intercept[RuntimeException] {
      Utils.jsonToRow(struct, json)
    }
    err.getMessage.contains("Key a does not exist") should be (true)
  }

  test("json to row - fail if json is null") {
    val struct = StructType(StructField("a", IntegerType) :: Nil)
    intercept[IllegalArgumentException] {
      Utils.jsonToRow(struct, null)
    }
  }

  test("json to row - process timestamp type") {
    val struct = StructType(
      StructField("a", TimestampType) ::
      StructField("b", TimestampType) :: Nil)
    val json: Map[String, Any] = Map("a" -> "2016-10-19T12:30:51Z", "b" -> null)
    val row = Utils.jsonToRow(struct, json)
    row.size should be (2)
    row(0).asInstanceOf[Timestamp].getTime should be (1476880251000L)
    row(1).asInstanceOf[Timestamp] should be (null)
  }

  test("json to row - fail for unsupported type") {
    val struct = StructType(StructField("a", DateType) :: Nil)
    val json: Map[String, Any] = Map("a" -> "abc")
    val err = intercept[RuntimeException] {
      Utils.jsonToRow(struct, json)
    }
    err.getMessage.contains("Unsupported data type") should be (true)
  }

  test("pull request info - equality") {
    val info1 = PullRequestInfo(1, 100, "url", "date", None, None)
    val info2 = PullRequestInfo(2, 101, "url", "date", None, None)
    val info3 = PullRequestInfo(1, 102, "url", "date", None, None)
    val info4 = PullRequestInfo(1, 100, "url1", "date1", Some("token"), None)
    info1.equals(info2) should be (false)
    info1.equals(info3) should be (false)
    info1.equals(info4) should be (true)
    info1.equals(null) should be (false)
    info1.equals("str") should be (false)
  }

  test("pull request info - hash code") {
    val info1 = PullRequestInfo(1, 100, "url", "date", None, None)
    val info2 = PullRequestInfo(2, 101, "url", "date", None, None)
    val info3 = PullRequestInfo(1, 102, "url", "date", None, None)
    val info4 = PullRequestInfo(1, 100, "url1", "date1", Some("token"), None)
    assert(info1.hashCode != info2.hashCode)
    assert(info1.hashCode != info3.hashCode)
    assert(info1.hashCode == info4.hashCode)
  }

  test("pull request partition - equality") {
    val split1 = new PullRequestPartition(0, 1, null)
    val split2 = new PullRequestPartition(0, 1,
      Seq(PullRequestInfo(1, 100, "url", "date", None, None)))
    val split3 = new PullRequestPartition(0, 2, Seq.empty)
    split1.equals(split2) should be (true)
    split1.equals(split3) should be (false)
    split1.equals(null) should be (false)
    split1.equals("str") should be (false)
  }

  test("pull request partition - hash code") {
    val split1 = new PullRequestPartition(0, 1, null)
    val split2 = new PullRequestPartition(0, 1,
      Seq(PullRequestInfo(1, 100, "url", "date", None, None)))
    val split3 = new PullRequestPartition(0, 2, Seq.empty)
    assert(split1.hashCode == split2.hashCode)
    assert(split1.hashCode != split3.hashCode)
  }

  test("pull request partition slice") {
    val split = new PullRequestPartition(0, 1, null)
    split.index should be (split.slice)
  }

  test("pull request partition iterator") {
    val split = new PullRequestPartition(0, 1,
      Seq(PullRequestInfo(1, 100, "url", "date", None, None)))
    split.iterator.size should be (1)
    val info = split.iterator.next
    info.id should be (1)
    info.number should be (100)
  }

  test("pull request rdd - partitions") {
    val schema = StructType(StructField("a", StringType) :: Nil)
    val data = Seq(
      PullRequestInfo(1, 101, "url", "updatedAt", None, None),
      PullRequestInfo(3, 103, "url", "updatedAt", None, None),
      PullRequestInfo(2, 102, "url", "updatedAt", None, None))
    val rdd = new PullRequestRDD(spark.sparkContext, data, schema, data.length)
    val splits = rdd.getPartitions.map(_.asInstanceOf[PullRequestPartition])
    splits.length should be (data.length)
    splits(0).index should be (0)
    splits(1).index should be (1)
    splits(2).index should be (2)
    splits(0).info.length should be (1)
    splits(1).info.length should be (1)
    splits(2).info.length should be (1)
  }

  test("pull request rdd - process response body") {
    val schema = StructType(
      StructField("a", StringType) ::
      StructField("b", BooleanType) ::
      StructField("c", IntegerType) :: Nil)
    val body = """{
      | "a": "str",
      | "b": true,
      | "c": 123}""".stripMargin
    val row = PullRequestRDD.processResponseBody(schema, body)
    row should be (Row("str", true, 123))
  }

  test("pull request rdd - findPullOrFail, error 500") {
    makeRequest { case (request, response) =>
      response.setContentType("application/json; charset=utf-8")
      response.setStatus(500)
      response.getWriter.print("{\"message\": \"Server error\"}")
    } { url =>
      val err = intercept[RuntimeException] {
        PullRequestRDD.findPullOrFail(url, None)
      }
      err.getMessage.contains("Request failed with code 500")
    }
  }

  test("pull request rdd - findPullOrFail, error 400") {
    makeRequest { case (request, response) =>
      response.setContentType("application/json; charset=utf-8")
      response.setStatus(400)
      response.getWriter.print("{\"message\": \"Request error\"}")
    } { url =>
      val err = intercept[RuntimeException] {
        PullRequestRDD.findPullOrFail(url, None)
      }
      err.getMessage.contains("Request failed with code 400")
    }
  }

  test("pull request rdd - findPullOrFail, success 200") {
    makeRequest { case (request, response) =>
      response.setContentType("application/json; charset=utf-8")
      response.setStatus(200)
      response.getWriter.print("{\"message\": \"All good\"}")
    } { url =>
      val response = PullRequestRDD.findPullOrFail(url, None)
      response.body should be ("{\"message\": \"All good\"}")
    }
  }

  test("pull request rdd - compute, no cache") {
    makeRequest { case (request, response) =>
      response.setContentType("application/json; charset=utf-8")
      response.setStatus(200)
      response.getWriter.print("{\"a\": \"str\", \"b\": 1}")
    } { url =>
      val schema = StructType(StructField("a", StringType) :: StructField("b", IntegerType):: Nil)
      val data = Seq(
        PullRequestInfo(1, 100, url, "2000-01-01T00:00:00Z", None, None),
        PullRequestInfo(2, 200, url, "2000-01-01T00:00:00Z", None, None))
      val rdd = new PullRequestRDD(spark.sparkContext, data, schema, 2)
      rdd.collect should be (Array(Row("str", 1), Row("str", 1)))
    }
  }

  test("pull request rdd - compute, read remote, write cache") {
    withTempDir { dir =>
      makeRequest { case (request, response) =>
        response.setContentType("application/json; charset=utf-8")
        response.setStatus(200)
        response.getWriter.print("{\"a\": \"str\", \"b\": 1}")
      } { url =>
        val schema = StructType(StructField("a", StringType) :: StructField("b", IntegerType):: Nil)
        val data = Seq(
          PullRequestInfo(1, 100, url, "2000-01-01T00:00:00Z", None,
            Some(dir.suffix("/1").toString)),
          PullRequestInfo(2, 200, url, "2000-01-01T00:00:00Z", None,
            Some(dir.suffix("/2").toString))
        )
        val rdd = new PullRequestRDD(spark.sparkContext, data, schema, 2)
        rdd.collect should be (Array(Row("str", 1), Row("str", 1)))
        // check cache

        val json1 = Source.fromFile(dir.suffix("/1").toString).getLines.next
        val json2 = Source.fromFile(dir.suffix("/2").toString).getLines.next
        json1 should be ("{\"a\": \"str\", \"b\": 1}")
        json2 should be ("{\"a\": \"str\", \"b\": 1}")
      }
    }
  }

  test("pull request rdd - compute, read cache") {
    withTempDir { dir =>
      val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
      Utils.writePersistedCache(fs, dir.suffix("/1"), "{\"a\": \"str1\", \"b\": 1}")
      Utils.writePersistedCache(fs, dir.suffix("/2"), "{\"a\": \"str2\", \"b\": 2}")

      val schema = StructType(StructField("a", StringType) :: StructField("b", IntegerType):: Nil)
      val data = Seq(
        PullRequestInfo(1, 100, "localhost:0", "2000-01-01T00:00:00Z", None,
          Some(dir.suffix("/1").toString)),
        PullRequestInfo(2, 200, "localhost:0", "2000-01-01T00:00:00Z", None,
          Some(dir.suffix("/2").toString))
      )
      val rdd = new PullRequestRDD(spark.sparkContext, data, schema, 2)
      rdd.collect should be (Array(Row("str1", 1), Row("str2", 2)))
    }
  }

  test("pull request rdd - invalid number of slices") {
    intercept[IllegalArgumentException] {
      PullRequestRDD.slice(Array.empty[Int], 0)
    }
  }

  test("pull request rdd - slice seq") {
    val res = PullRequestRDD.slice(Array(1, 2, 3, 4, 5, 6), 3)
    res should be (Seq(Seq(1, 2), Seq(3, 4), Seq(5, 6)))
  }

  test("pull request rdd - slice seq 2") {
    val res = PullRequestRDD.slice(Array(1, 2, 3, 4, 5, 6), 7)
    res should be (Seq(Seq(), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5), Seq(6)))
  }

  test("list from response - response 5xx failure") {
    val err = intercept[RuntimeException] {
      PullRequestRelation.listFromResponse(HttpResponse("Error", 500, Map.empty), None)
    }
    err.getMessage.contains("Request failed with code 500") should be (true)
  }

  test("list from response - response 4xx failure") {
    val err = intercept[RuntimeException] {
      PullRequestRelation.listFromResponse(HttpResponse("Error", 404, Map.empty), None)
    }
    err.getMessage.contains("Request failed with code 404") should be (true)
  }

  test("list from response - response 3xx failure") {
    // fail on redirects as they are not supported for now
    val err = intercept[RuntimeException] {
      PullRequestRelation.listFromResponse(HttpResponse("Error", 302, Map.empty), None)
    }
    err.getMessage.contains("Request failed with code 302") should be (true)
  }

  test("list from response - key does not exist") {
    val err = intercept[RuntimeException] {
      PullRequestRelation.listFromResponse(HttpResponse("[{\"a\": true}]", 200, Map.empty), None)
    }
    err.getMessage should be ("Failed to convert to 'PullRequestInfo', data=Map(a -> true)")
    err.getCause.getMessage should be ("Key id does not exist")
  }

  test("list from response - conversion fails") {
    val err = intercept[RuntimeException] {
      PullRequestRelation.listFromResponse(HttpResponse("[{\"id\": true}]", 200, Map.empty), None)
    }
    err.getMessage should be ("Failed to convert to 'PullRequestInfo', data=Map(id -> true)")
    err.getCause.getMessage.contains("cannot be cast to") should be (true)
  }

  test("list from response - parse into pull request info") {
    val body = """[
      |{
      |  "id": 1,
      |  "number": 100,
      |  "url": "url",
      |  "updated_at": null,
      |  "created_at": "2010-10-23T12:52:31Z"
      |}]""".stripMargin
    val seq = PullRequestRelation.listFromResponse(HttpResponse(body, 200, Map.empty), None)
    seq.length should be (1)
    seq.head.id should be (1)
    seq.head.number should be (100)
    seq.head.url should be ("url")
    seq.head.updatedAt should be ("2010-10-23T12:52:31Z")
  }

  test("list from response - parse with cache directory") {
    val body = """[
      |{
      |  "id": 1,
      |  "number": 123,
      |  "url": "url",
      |  "updated_at": "2010-11-01T09:38:15Z",
      |  "created_at": "2010-10-23T12:52:31Z"
      |}]""".stripMargin
    val seq = PullRequestRelation.listFromResponse(
      HttpResponse(body, 200, Map.empty), None, Some("/folder"))
    seq.length should be (1)
    seq.head.id should be (1)
    seq.head.number should be (123)
    seq.head.url should be ("url")
    seq.head.updatedAt should be ("2010-11-01T09:38:15Z")
    seq.head.cachePath should be (Some(s"/folder/pr-1-2010-11-01T09=38=15Z.cache"))
  }

  test("utils - persisted filename") {
    val filename = Utils.persistedFilename(123, "2000-01-01T21:45:32Z")
    filename should be ("pr-123-2000-01-01T21=45=32Z.cache")
  }

  // Potentially dangerous action, because previous content might be newer than current
  // TODO: investigate this case and maybe add check on hashsum or something like that
  test("utils - overwrite content of the file that already exists") {
    withTempDir { dir =>
      val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val path = dir.suffix(HadoopPath.SEPARATOR + "test")
      Utils.writePersistedCache(fs, path, "hello")
      Utils.writePersistedCache(fs, path, "hello2")
      val result = Utils.readPersistedCache(fs, path)
      result should be ("hello2")
    }
  }

  test("utils - read/write persisted cache") {
    withTempDir { dir =>
      val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val path = dir.suffix(HadoopPath.SEPARATOR + "test")
      Utils.writePersistedCache(fs, path, "hello")
      val result = Utils.readPersistedCache(fs, path)
      result should be ("hello")
    }
  }

  test("utils - read non-existent file") {
    withTempDir { dir =>
      val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val path = dir.suffix(HadoopPath.SEPARATOR + "test")
      intercept[IOException] {
        Utils.readPersistedCache(fs, path)
      }
    }
  }

  test("utils - check persisted cache directory") {
    withTempDir { dir =>
      val path = dir.toString
      val fqn = Utils.checkPersistedCacheDir(path, spark.sparkContext.hadoopConfiguration)
      fqn should be (s"file:$path")
    }
  }

  test("utils - check persisted cache non-existent directory") {
    withTempDir { dir =>
      val path = dir.toString / "test"
      val fqn = Utils.checkPersistedCacheDir(path, spark.sparkContext.hadoopConfiguration)
      fqn should be (s"file:$path")
    }
  }

  test("utils - fail to check persisted cache for file") {
    withTempDir { dir =>
      val path = dir.toString / "test"
      val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
      fs.createNewFile(new HadoopPath(path))
      val err = intercept[IllegalArgumentException] {
        Utils.checkPersistedCacheDir(path, spark.sparkContext.hadoopConfiguration)
      }
      err.getMessage.contains("not a directory") should be (true)
    }
  }

  test("utils - fail with wrong permissions on cache directory") {
    withTempDir { dir =>
      val path = dir.toString / "test"
      val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
      fs.mkdirs(new HadoopPath(path), FsPermission.valueOf("dr--r--r--"))
      val err = intercept[IllegalArgumentException] {
        Utils.checkPersistedCacheDir(path, spark.sparkContext.hadoopConfiguration)
      }
      err.getMessage.contains("Expected read/write access") should be (true)
    }
  }

  test("utils - attempts, invalid batch size") {
    val err = intercept[IllegalArgumentException] {
      Utils.attempts(0, 1)
    }
    err.getMessage.contains("Expected positive batch size") should be (true)
  }

  test("utils - attempts, invalid max page size") {
    val err = intercept[IllegalArgumentException] {
      Utils.attempts(1, 0)
    }
    err.getMessage.contains("Expected positive max page size") should be (true)
  }

  test("utils - attempts") {
    Utils.attempts(12, 100) should be (Seq(12))
    Utils.attempts(100, 100) should be (Seq(100))
    Utils.attempts(101, 100) should be (Seq(100, 1))
    Utils.attempts(200, 100) should be (Seq(100, 100))
    Utils.attempts(245, 100) should be (Seq(100, 100, 45))
  }

  test("cachekey - init") {
    val key = CacheKey("user", "repo", 128, Some("token"), Some("directory"))
    key.user should be ("user")
    key.repo should be ("repo")
    key.batchSize should be (128)
    key.authToken should be (Some("token"))
    key.cacheDirectory should be (Some("directory"))
  }

  test("cachekey - equals, null") {
    val key = CacheKey("user", "repo", 128, None, None)
    key.equals(null) should be (false)
  }

  test("cachekey - equals, arbitrary object") {
    val key = CacheKey("user", "repo", 128, None, None)
    key.equals(Option(null)) should be (false)
    key.equals(Seq("a", "b", "c")) should be (false)
  }

  test("cachekey - equals, different user") {
    val key1 = CacheKey("user1", "repo", 128, None, None)
    val key2 = CacheKey("user2", "repo", 128, None, None)
    key1.equals(key2) should be (false)
  }

  test("cachekey - equals, different repo") {
    val key1 = CacheKey("user", "repo1", 128, None, None)
    val key2 = CacheKey("user", "repo2", 128, None, None)
    key1.equals(key2) should be (false)
  }

  test("cachekey - equals, different batch size") {
    val key1 = CacheKey("user", "repo", 128, None, None)
    val key2 = CacheKey("user", "repo", 129, None, None)
    key1.equals(key2) should be (false)
  }

  test("cachekey - equals, different token") {
    val key1 = CacheKey("user", "repo", 128, None, None)
    val key2 = CacheKey("user", "repo", 128, Some("token"), None)
    key1.equals(key2) should be (true)
  }

  test("cachekey - equals, different cache directory") {
    val key1 = CacheKey("user", "repo", 128, None, None)
    val key2 = CacheKey("user", "repo", 128, None, Some("directory"))
    key1.equals(key2) should be (true)
  }

  test("cachekey - hashcode, comparison between two similar instances") {
    val key1 = CacheKey("user", "repo", 128, None, None)
    val key2 = CacheKey("user", "repo", 128, None, None)
    key1.hashCode should be (key2.hashCode)
  }

  test("cachekey - hashcode, comparison between two different instances 1") {
    val key1 = CacheKey("user", "repo", 128, None, Some("directory"))
    val key2 = CacheKey("user", "repo", 128, Some("token"), None)
    key1.hashCode should be (key2.hashCode)
  }

  test("cachekey - hashcode, comparison between two different instances 2") {
    def notEqual(key1: CacheKey, key2: CacheKey): Unit = assert(key1.hashCode != key2.hashCode)
    // different combinations of keys that should result in different hash codes
    notEqual(CacheKey("user", "repo", 128, None, None), CacheKey("user", "repo", 129, None, None))
    notEqual(CacheKey("user1", "repo", 128, None, None), CacheKey("user2", "repo", 128, None, None))
    notEqual(CacheKey("user", "repo1", 128, None, None), CacheKey("user", "repo2", 128, None, None))
    notEqual(CacheKey("a", "b", 128, None, None), CacheKey("b", "a", 128, None, None))
    notEqual(CacheKey("a", "b", 0, None, None), CacheKey("b", "a", 0, None, None))
    notEqual(CacheKey("a", "b", 1, Some("token"), None), CacheKey("a", "b", 2, Some("token"), None))
  }

  test("cachekey - toString, token provided") {
    val key = CacheKey("a", "b", 5, Some("token"), None)
    key.toString should be (
      "CacheKey(user=a, repo=b, batchSize=5, authToken=Some(*****), cacheDirectory=None)")
  }

  test("cachekey - toString, no token provided") {
    val key = CacheKey("a", "b", 5, None, None)
    key.toString should be (
      "CacheKey(user=a, repo=b, batchSize=5, authToken=None, cacheDirectory=None)")
  }

  test("cachekey, toString, cacheDirectory provided") {
    val key = CacheKey("a", "b", 5, None, Some("file:/tmp"))
    key.toString should be (
      "CacheKey(user=a, repo=b, batchSize=5, authToken=None, cacheDirectory=Some(file:/tmp))")
  }

  test("issue #8 - non-nullable field in schema is null") {
    val schema = StructType(
      StructField("a", StringType, true) ::
      StructField("b", BooleanType, false) :: Nil)
    val response = "{\"a\" : null, \"b\" : null}"
    val err = intercept[IllegalStateException] {
      PullRequestRDD.processResponseBody(schema, response)
    }
    err.getMessage should be ("Non-nullable field b has value null")
  }

  test("issue #8 - nullable field in schema is null") {
    val schema = StructType(StructField("a", StringType, true) :: Nil)
    val response = "{\"a\" : null}"
    val row = PullRequestRDD.processResponseBody(schema, response)
    row should be (Row(null))
  }

  test("issue #8 - non-nullable field in schema is not null") {
    val schema = StructType(StructField("a", BooleanType, true) :: Nil)
    val response = "{\"a\" : true}"
    val row = PullRequestRDD.processResponseBody(schema, response)
    row should be (Row(true))
  }

  test("issue #8 - mergeable field is null") {
    // test actual (simplified) response and check mergeable field
    val sqlContext = spark.sqlContext
    val relation = new PullRequestRelation(sqlContext, Map.empty)
    val response = """
    {
      "url": "url",
      "id": 93545317,
      "html_url": "url",
      "diff_url": "url.diff",
      "patch_url": "url.patch",
      "number": 15880,
      "state": "open",
      "title": "title",
      "user": {"login": "login", "id": 3182036, "url": "url", "html_url": "url"},
      "body": "body",
      "created_at": "2016-11-14T10:55:04Z",
      "updated_at": "2016-11-16T06:20:00Z",
      "closed_at": null,
      "merged_at": null,
      "base": {
        "ref": "master",
        "sha": "f95b124c68ccc2e318f6ac30685aa47770eea8f3",
        "repo": {
          "id": 17165658,
          "name": "spark",
          "full_name": "apache/spark",
          "private": false,
          "url": "url",
          "html_url": "url",
          "description": "description"
        }
      },
      "merged": false,
      "mergeable": null,
      "comments": 15,
      "commits": 3,
      "additions": 38,
      "deletions": 7,
      "changed_files": 7
    }
    """
    val row = PullRequestRDD.processResponseBody(relation.schema, response)
    row(15).asInstanceOf[Boolean] should be (false)
    // 'mergeable' should be null
    row.isNullAt(16) should be (true)
    row(17).asInstanceOf[Int] should be (15)
  }
}
