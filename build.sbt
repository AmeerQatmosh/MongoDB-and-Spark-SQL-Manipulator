ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "MongoDB and Spark SQL Manipulator",
    idePackagePrefix := Some("org.json.tweets")
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.2",
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.7.1",
  "org.mongodb" % "mongo-java-driver" % "3.12.11",
  "org.mongodb" % "bson" % "4.7.1"
)
