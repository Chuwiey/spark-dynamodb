package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Table}
import com.amazonaws.services.dynamodbv2.document.spec.{QuerySpec, ScanSpec}
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

private[dynamodb] trait BaseScanner {
  private val log = LoggerFactory.getLogger(this.getClass)

  def getTable(
    tableName: String,
    maybeCredentials: Option[String],
    awsAccessKey: Option[String],
    awsSecretKey: Option[String],
    maybeRegion: Option[String],
    maybeEndpoint: Option[String])
  : Table = {

    val builder = AmazonDynamoDBClientBuilder.standard()

    maybeCredentials match {
      case Some(credentialsClassName) =>
        log.info(s"Using AWSCredentialsProvider $credentialsClassName")
        if (credentialsClassName == "AWSStaticCredentialsProvider") {
            val creds = new BasicAWSCredentials(awsAccessKey.get, awsSecretKey.get)
            val ascp = new AWSStaticCredentialsProvider(creds)
            builder.withCredentials(ascp)
        } else {
            val credentials = Class.forName(credentialsClassName)
                .newInstance().asInstanceOf[AWSCredentialsProvider]
            builder.withCredentials(credentials)
        }
      case None => // pass
    }

    maybeRegion.foreach(builder.withRegion)
    maybeEndpoint.foreach(endpoint => {
      val endpointConfiguration = new EndpointConfiguration(endpoint, "us-west-2")
      builder.withEndpointConfiguration(endpointConfiguration) // for tests
    })
    val client = builder.build()
    new DynamoDB(client).getTable(tableName)
  }

  def getTable(config: Config): Table = {
    getTable(
      tableName = config.table,
      config.maybeCredentials,
      config.awsAccessKey,
      config.awsSecretKey,
      config.maybeRegion,
      config.maybeEndpoint)
  }

  def getScanSpec(config: Config): ScanSpec = {
    val scanSpec = new ScanSpec()
      .withMaxPageSize(config.pageSize)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .withTotalSegments(config.totalSegments)
      .withSegment(config.segment)

    // Parse any projection expressions passed in
    val exprSpecBuilder = config.maybeRequiredColumns
      .map(requiredColumns => new ExpressionSpecBuilder().addProjections(requiredColumns: _*))
    val parsedProjectionExpr = exprSpecBuilder.map(xSpecBuilder => xSpecBuilder.buildForScan())
    val projectionNameMap = parsedProjectionExpr.flatMap(projExpr => Option(projExpr.getNameMap))
      .map(_.asScala.toMap).getOrElse(Map.empty)
    val projectionValueMap = parsedProjectionExpr.flatMap(projExpr => Option(projExpr.getValueMap))
      .map(_.asScala.toMap).getOrElse(Map.empty)
    parsedProjectionExpr.foreach(expr =>
      scanSpec.withProjectionExpression(expr.getProjectionExpression))

    // Parse any filter expression passed in as an option
    val parsedFilterExpr = config.maybeFilterExpression
      .map(filterExpression => ParsedFilterExpression(filterExpression))
    val filterNameMap = parsedFilterExpr.map(_.expressionNames).getOrElse(Map.empty)
    val filterValueMap = parsedFilterExpr.map(_.expressionValues).getOrElse(Map.empty)
    parsedFilterExpr.foreach(expr =>
      scanSpec.withFilterExpression(expr.expression))

    // Combine parsed name and value maps from the projections and filter expressions
    val nameMap = projectionNameMap ++ filterNameMap
    Option(nameMap).filter(_.nonEmpty).foreach(nMap => scanSpec.withNameMap(nMap.asJava))
    val valueMap = projectionValueMap ++ filterValueMap
    Option(valueMap).filter(_.nonEmpty).foreach(vMap => scanSpec.withValueMap(vMap.asJava))

    scanSpec
  }

  def getQuerySpec(config: Config): QuerySpec = {
    val querySpec = new QuerySpec()
      .withMaxPageSize(config.pageSize)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    // Queries might have required partition/hash keys
    val partitionKey = parseStringToMap(config.maybePartitionKey)
    val sortKey = parseStringToMap(config.maybeSortKey)

    // Parse any projection expressions passed in
    val exprSpecBuilder = config.maybeRequiredColumns
      .map(requiredColumns => new ExpressionSpecBuilder().addProjections(requiredColumns: _*))
    val parsedProjectionExpr = exprSpecBuilder.map(xSpecBuilder => xSpecBuilder.buildForScan())
    val projectionNameMap = parsedProjectionExpr.flatMap(projExpr => Option(projExpr.getNameMap))
      .map(_.asScala.toMap).getOrElse(Map.empty)
    val projectionValueMap = parsedProjectionExpr.flatMap(projExpr => Option(projExpr.getValueMap))
      .map(_.asScala.toMap).getOrElse(Map.empty)
    parsedProjectionExpr.foreach(expr =>
      querySpec.withProjectionExpression(expr.getProjectionExpression))

    // Parse any filter expression passed in as an option
    val parsedFilterExpr = config.maybeFilterExpression
      .map(filterExpression => ParsedFilterExpression(filterExpression))
    val filterNameMap = parsedFilterExpr.map(_.expressionNames).getOrElse(Map.empty)
    val filterValueMap = parsedFilterExpr.map(_.expressionValues).getOrElse(Map.empty)
    parsedFilterExpr.foreach(expr =>
      querySpec.withFilterExpression(expr.expression))

    // Parse/pass through any keyConditionExpression
    if (config.maybeKeyConditionExpression.isDefined) {
      querySpec.withKeyConditionExpression(config.maybeKeyConditionExpression.get)
    }

    // Combine parsed name and value maps from the projections and filter expressions
    val nameMap = projectionNameMap ++ filterNameMap
    Option(nameMap).filter(_.nonEmpty).foreach(nMap => querySpec.withNameMap(nMap.asJava))
    val valueMap = projectionValueMap ++ filterValueMap ++ partitionKey ++ sortKey
    Option(valueMap).filter(_.nonEmpty).foreach(vMap => querySpec.withValueMap(vMap.asJava))

    querySpec
  }

    def parseStringToMap(s: Option[String]): Map[String, AnyRef] = {
      s.map(_.split("=")).map(arr => arr(0) -> parseValue(arr(1)).asInstanceOf[AnyRef]).toMap
    }

    def parseValue(value: String): Any = {
      try {
        value.toInt
      } catch {
        case _ => value
      }
    }
}


private[dynamodb] case class Config(
  table: String,
  callType: String,
  segment: Int,
  totalSegments: Int,
  pageSize: Int,
  maybeSchema: Option[StructType] = None,
  maybeRequiredColumns: Option[Array[String]] = None,
  maybeFilterExpression: Option[String] = None,
  maybeRateLimit: Option[Int] = None,
  maybeCredentials: Option[String] = None,
  awsAccessKey: Option[String] = None,
  awsSecretKey: Option[String] = None,
  maybeRegion: Option[String] = None,
  maybeEndpoint: Option[String] = None,
  maybePartitionKey: Option[String] = None,
  maybeSortKey: Option[String] = None,
  maybeKeyConditionExpression: Option[String] = None,
  maybeIndexName: Option[String] = None
)

