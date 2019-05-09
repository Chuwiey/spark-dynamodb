package com.github.traviscrawford.spark.dynamodb

import org.apache.spark.sql._
import org.apache.spark.sql.types._

class DynamoDBRelationIntegrationSpec() extends BaseIntegrationSpec {

  private val EndpointKey = "endpoint"
  private val TestUsersTableSchema = StructType(Seq(
    StructField(CreatedAtKey, LongType),
    StructField(UserIdKey, LongType),
    StructField(UserIdRange, StringType),
    StructField(UsernameKey, StringType)))

  "A DynamoDBRelation" should "infer the correct schema" in {
    val usersDF = spark.read
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    usersDF.schema shouldEqual TestUsersTableSchema
  }

  it should "get attributes in the inferred schema" in {
    val usersDF = spark.read
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("rate_limit_per_segment", "1")
      .dynamodb(TestUsersTableName)

    usersDF.collect() should contain theSameElementsAs
      Seq(Row(11, 1, "aa", "a"), Row(22, 2, "bb", "b"), Row(33, 3, "cc", "c"))
  }

  it should "get attributes in the user-provided schema" in {
    val usersDF = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    usersDF.collect() should contain theSameElementsAs
      Seq(Row(11, 1, "aa", "a"), Row(22, 2, "bb", "b"), Row(33, 3, "cc", "c"))
  }

  it should "support queries with just partition_key" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option("table", "vectors_videoblocks")
      .option("segments", 1)
      .option("key_condition_expression", "user_id = :uid")
      .option("partition_key", ":uid=1")
      .option("calltype", "query")
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.collect() should contain theSameElementsAs Seq(Row(11, 1, "aa", "a"))
  }

  it should "support queries with partition_key and sort_key" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option("table", "vectors_videoblocks")
      .option("segments", 1)
      .option("key_condition_expression", "user_id = :uid and user_range = :urng")
      .option("partition_key", ":uid=1")
      .option("sort_key", ":urng=aa")
      .option("calltype", "query")
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.collect() should contain theSameElementsAs Seq(Row(11, 1, "aa", "a"))
  }

  it should "support queries with partition_key and sort_key on an index" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option("table", "vectors_videoblocks")
      .option("segments", 1)
      .option("key_condition_expression", "user_range = :urng and user_id = :uid")
      .option("partition_key", ":urng=aa")
      .option("sort_key", ":uid=1")
      .option("calltype", "index")
      .option("index_name", TestUsersIndexName)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.collect() should contain theSameElementsAs Seq(Row(11, 1, "aa", "a"))
  }

  it should "support EqualTo filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username = 'a'").collect() should
      contain theSameElementsAs Seq(Row(11, 1, "aa", "a"))
  }

  it should "support GreaterThan filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username > 'b'").collect() should
      contain theSameElementsAs Seq(Row(33, 3, "cc", "c"))
  }

  it should "support LessThan filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username < 'b'").collect() should
      contain theSameElementsAs Seq(Row(11, 1, "aa", "a"))
  }

  it should "apply server side filter_expressions" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("filter_expression", "username <> b")
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username <> 'c'").collect() should
      contain theSameElementsAs Seq(Row(11, 1, "aa", "a"))
  }

  it should "apply server side filter_expressions equals" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("filter_expression", "username = a")
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users").collect() should
      contain theSameElementsAs Seq(Row(11, 1, "aa", "a"))
  }
}
