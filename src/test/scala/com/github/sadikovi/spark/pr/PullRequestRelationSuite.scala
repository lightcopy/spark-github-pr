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

import java.math.BigInteger
import java.sql.Timestamp

import scala.math.BigInt

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.github.sadikovi.testutil.{SparkLocal, UnitTestSuite}

class PullRequestRelationSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkContext()
  }

  override def afterAll {
    stopSparkContext()
  }

  test("fail to extract username") {
    val err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map.empty)
    }
    err.getMessage.contains("Expected 'user' option") should be (true)
  }

  test("extract empty username") {
    var err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map("user" -> ""))
    }
    err.getMessage.contains("Expected 'user' option") should be (true)

    err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map("user" -> "  "))
    }
    err.getMessage.contains("Expected 'user' option") should be (true)
  }

  test("fail to extract repository") {
    val err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map("user" -> "user"))
    }
    err.getMessage.contains("Expected 'repo' option") should be (true)
  }

  test("extract empty repository") {
    var err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map("user" -> "user", "repo" -> ""))
    }
    err.getMessage.contains("Expected 'repo' option") should be (true)

    err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map("user" -> "user", "repo" -> "  "))
    }
    err.getMessage.contains("Expected 'repo' option") should be (true)
  }

  test("extract username, repository") {
    var relation = new PullRequestRelation(null, Map("user" -> "abc", "repo" -> "def"))
    relation.user should be ("abc")
    relation.repo should be ("def")
    relation = new PullRequestRelation(null, Map("user" -> "ABC", "repo" -> "DEF"))
    relation.user should be ("ABC")
    relation.repo should be ("DEF")
    relation = new PullRequestRelation(null, Map("user" -> " abc ", "repo" -> " def "))
    relation.user should be ("abc")
    relation.repo should be ("def")
  }

  test("batch size conversion fail") {
    var err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map("user" -> "user", "repo" -> "repo", "batch" -> ""))
    }
    err.getMessage.contains("Invalid batch size") should be (true)

    err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map("user" -> "user", "repo" -> "repo", "batch" -> "abc"))
    }
    err.getMessage.contains("Invalid batch size") should be (true)

    err = intercept[RuntimeException] {
      new PullRequestRelation(null, Map("user" -> "user", "repo" -> "repo", "batch" -> "  "))
    }
    err.getMessage.contains("Invalid batch size") should be (true)
  }

  test("batch size out of bound") {
    var err = intercept[IllegalArgumentException] {
      new PullRequestRelation(null, Map("user" -> "user", "repo" -> "repo", "batch" -> "0"))
    }
    err.getMessage.contains("Batch size 0 is out of bound") should be (true)

    err = intercept[IllegalArgumentException] {
      new PullRequestRelation(null, Map("user" -> "user", "repo" -> "repo", "batch" -> "1001"))
    }
    err.getMessage.contains("Batch size 1001 is out of bound") should be (true)
  }

  test("select default or valid batch size") {
    var relation = new PullRequestRelation(null, Map("user" -> "user", "repo" -> "repo"))
    relation.batchSize should be (relation.defaultBatchSize)

    relation = new PullRequestRelation(null,
      Map("user" -> "user", "repo" -> "repo", "batch" -> "140"))
    relation.batchSize should be (140)
  }

  test("select token if available") {
    var relation = new PullRequestRelation(null,
      Map("user" -> "user", "repo" -> "repo", "token" -> "abc"))
    relation.token should be (Some("abc"))

    relation = new PullRequestRelation(null,
      Map("user" -> "user", "repo" -> "repo"))
    relation.token should be (None)
  }

  test("check main schema items") {
    val relation = new PullRequestRelation(null, Map("user" -> "user", "repo" -> "repo"))
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
      HttpUtils.pulls("", "repo", 10, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("user", "", 10, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("a/b/c", "repo", 10, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("user", "/a/b/c", 10, None)
    }

    intercept[IllegalArgumentException] {
      HttpUtils.pulls("user", "repo", 0, None)
    }
  }

  test("pull request source - pulls() without token") {
    val request = HttpUtils.pulls("user", "repo", 7, None)
    request.method should be ("GET")
    request.url should be ("https://api.github.com/repos/user/repo/pulls")
    request.params should be (List(("per_page", "7")))
    request.headers.toMap.get("Authorization") should be (None)
  }

  test("pull request source - pulls() with token") {
    val request = HttpUtils.pulls("user", "repo", 15, Some("abc123"))
    request.method should be ("GET")
    request.url should be ("https://api.github.com/repos/user/repo/pulls")
    request.params should be (List(("per_page", "15")))
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
    val info1 = PullRequestInfo(1, 100, "url", "date", None)
    val info2 = PullRequestInfo(2, 101, "url", "date", None)
    val info3 = PullRequestInfo(1, 102, "url", "date", None)
    val info4 = PullRequestInfo(1, 100, "url1", "date1", Some("token"))
    info1.equals(info2) should be (false)
    info1.equals(info3) should be (false)
    info1.equals(info4) should be (true)
  }

  test("pull request info - hash code") {
    val info1 = PullRequestInfo(1, 100, "url", "date", None)
    val info2 = PullRequestInfo(2, 101, "url", "date", None)
    val info3 = PullRequestInfo(1, 102, "url", "date", None)
    val info4 = PullRequestInfo(1, 100, "url1", "date1", Some("token"))
    assert(info1.hashCode != info2.hashCode)
    assert(info1.hashCode != info3.hashCode)
    assert(info1.hashCode == info4.hashCode)
  }

  test("pull request partition - equality") {
    val split1 = new PullRequestPartition(0, 1, null)
    val split2 = new PullRequestPartition(0, 1, Seq(PullRequestInfo(1, 100, "url", "date", None)))
    val split3 = new PullRequestPartition(0, 2, Seq.empty)
    split1.equals(split2) should be (true)
    split1.equals(split3) should be (false)
  }

  test("pull request partition - hash code") {
    val split1 = new PullRequestPartition(0, 1, null)
    val split2 = new PullRequestPartition(0, 1, Seq(PullRequestInfo(1, 100, "url", "date", None)))
    val split3 = new PullRequestPartition(0, 2, Seq.empty)
    assert(split1.hashCode == split2.hashCode)
    assert(split1.hashCode != split3.hashCode)
  }
}
