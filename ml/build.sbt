name := "MlExample"

version := "1.0"

scalaVersion := "2.11.8"

lazy val common = ProjectRef(file("../common"), "common")
lazy val root = project in file(".") dependsOn common

libraryDependencies ++= Seq(
  "com.h2database" % "h2" % "1.4.191",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "org.postgresql" % "postgresql" % "9.4-1206-jdbc41",
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)
