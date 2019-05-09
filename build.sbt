name := "spark-dynamodb"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.298",
  "com.twitter" % "util-app_2.11" % "18.3.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0" % "provided",
  "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test",
  "com.google.guava" % "guava" % "14.0.1"
)
