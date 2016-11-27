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

import org.apache.spark.sql.types.{LongType, StructField, StructType}

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class DefaultSourceSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("create pull request relation") {
    val source = new DefaultSource()
    val relation = source.createRelation(spark.sqlContext, Map.empty)
    relation.isInstanceOf[PullRequestRelation] should be (true)
  }

  test("fail if user-defined schema is provided for streaming schema") {
    val source = new DefaultSource()
    val err = intercept[IllegalArgumentException] {
      source.sourceSchema(spark.sqlContext, Some(StructType(StructField("a", LongType) :: Nil)),
        "provider", Map.empty)
    }
    err.getMessage.contains("User-defined schema is not supported") should be (true)
  }

  test("return correct schema for streaming") {
    // schema should be the same as for pull request relation
    val source = new DefaultSource()
    val relation = source.createRelation(spark.sqlContext, Map.empty)
    val (provider, schema) = source.sourceSchema(spark.sqlContext, None, "provider", Map.empty)
    provider should be ("provider")
    schema should be (relation.schema)
  }

  test("fail if user-defined schema is provided for streaming source") {
    val source = new DefaultSource()
    val err = intercept[IllegalArgumentException] {
      source.createSource(spark.sqlContext, "metadataPath",
        Some(StructType(StructField("a", LongType) :: Nil)), "provider", Map.empty)
    }
    err.getMessage.contains("User-defined schema is not supported") should be (true)
  }

  test("create pull request stream source") {
    val source = new DefaultSource()
    val stream = source.createSource(spark.sqlContext, "metadata", None, "provider", Map.empty)
    stream.isInstanceOf[PullRequestStreamSource] should be (true)
  }
}
