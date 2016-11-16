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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

/**
 * Default source provider for GitHub PR datasource.
 * Schema inferrence is not required and user-defined schema is not supported for now.
 * Source also provides stream source.
 */
class DefaultSource extends RelationProvider with StreamSourceProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    new PullRequestRelation(sqlContext, parameters)
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, s"User-defined schema is not supported for provider $providerName")
    // validate all options by creating pull request relation
    val relation = createRelation(sqlContext, parameters)
    // seems that provider name is not required, still pass it for safety reasons
    (providerName, relation.schema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    require(schema.isEmpty, s"User-defined schema is not supported for provider $providerName")
    new PullRequestStreamSource(sqlContext, new PullRequestRelation(sqlContext, parameters),
      metadataPath)
  }
}
