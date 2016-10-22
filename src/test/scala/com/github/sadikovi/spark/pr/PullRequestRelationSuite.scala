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
}
