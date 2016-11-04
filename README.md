# spark-github-pr
Spark SQL datasource for GitHub PR API.

[![Build Status](https://travis-ci.org/lightcopy/spark-github-pr.svg?branch=master)](https://travis-ci.org/lightcopy/spark-github-pr)
[![Coverage Status](https://coveralls.io/repos/github/lightcopy/spark-github-pr/badge.svg?branch=master)](https://coveralls.io/github/lightcopy/spark-github-pr?branch=master)

## Overview
Package allows to query GitHub API v3 to fetch pull request information. Launches first requests on
driver to list available pull requests, and creates tasks with pull requests details to execute. PRs
are cached in `cacheDir` value to save rate limit. It is recommended to use token to remove 60
requests/hour constraint.
Most of JSON keys are supported, see schema [here](./src/main/scala/com/github/lightcopy/spark/pr/PullRequestRelation.scala#L106),
here is an example output for subset of columns you might see:
```sh
scala> df.select("number", "title", "state", "base.repo.full_name", "user.login",
  "commits", "additions", "deletions")

+------+--------------------+-----+------------+------------+-------+---------+---------+
|number|               title|state|   full_name|       login|commits|additions|deletions|
+------+--------------------+-----+------------+------------+-------+---------+---------+
| 15599|[SPARK-18022][SQL...| open|apache/spark|      srowen|      1|        1|        1|
| 15598|[SPARK-18027][YAR...| open|apache/spark|      srowen|      1|        2|        0|
| 15597|[SPARK-18063][SQL...| open|apache/spark| jiangxb1987|      2|       16|        6|
| 15596|[SQL] Remove shuf...| open|apache/spark|      viirya|      1|       13|       12|
+------+--------------------+-----+------------+------------+-------+---------+---------+
```

## Requirements
| Spark version | spark-github-pr latest version |
|---------------|--------------------------------|
| 1.6.x | [1.1.0](http://spark-packages.org/package/lightcopy/spark-github-pr) |
| 2.x.x | [1.1.0](http://spark-packages.org/package/lightcopy/spark-github-pr) |

## Linking
The spark-github-pr package can be added to Spark by using the `--packages` command line option.
For example, run this to include it when starting the spark shell:
```shell
 $SPARK_HOME/bin/spark-shell --packages lightcopy:spark-github-pr:1.1.0-s_2.10
```
Change to `lightcopy:spark-github-pr:1.1.0-s_2.11` for Scala 2.11.x

### Options
Currently supported options:

| Name | Since | Example | Description |
|------|:-----:|:-------:|-------------|
| `user` | `1.0.0` | _apache_ | GitHub username or organization, default is `apache`
| `repo` | `1.0.0` | _spark_ | GitHub repository name for provided user, default is `spark`
| `batch` | `1.0.0` | _100_ | number of pull requests to fetch, default is 25, must be >= 1 and <= 1000
| `token` | `1.0.0` | _auth_token_ | authentication token to increase rate limit from 60 to 5000, see [GitHub Auth for more info](https://developer.github.com/v3/#oauth2-token-sent-in-a-header)
| `cacheDir` | `1.0.0` | _file:/tmp/.spark-github-pr_ | directory to store cached pull requests information, currently required to be shared folder on local file system or directory on HDFS.

## Example

### Scala API
```scala
val df = sqlContext.read.format("com.github.lightcopy.spark.pr").
  option("user", "apache").option("repo", "spark").load().
  select("number", "title", "state", "base.repo.full_name", "user.login", "commits")

// You can also specify batch size for number of pull requests to fetch
val df = sqlContext.read.format("com.github.lightcopy.spark.pr").
  option("user", "apache").option("repo", "spark").option("batch", "52").load()
```

### Python API
```python
df = sqlContext.read.format("com.github.lightcopy.spark.pr").
  option("user", "apache").option("repo", "spark")load()

res = df.where("commits > 10")
```

### SQL API
```sql
CREATE TEMPORARY TABLE prs
USING com.github.lightcopy.spark.pr
OPTIONS (user "apache", repo "spark");

SELECT number, title FROM prs LIMIT 10;
```

## Building From Source
This library is built using `sbt`, to build a JAR file simply run `sbt package` from project root.

## Testing
Run `sbt test` from project root. CI runs for Spark 2.0 only.
