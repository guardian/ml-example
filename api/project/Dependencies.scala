import sbt._

object Version {
  val hadoop    = "2.6.0"
  val logback   = "1.1.2"
  val mockito   = "1.10.19"
  val scala     = "2.11.8"
  val scalaTest = "2.2.4"
  val slf4j     = "1.7.6"
  val spark     = "2.0.0"
  val kryo      = "4.0.0"
}

object Library {
  val logbackClassic = "ch.qos.logback"       %  "logback-classic" % Version.logback
  val mockitoAll     = "org.mockito"          %  "mockito-all"     % Version.mockito
  val scalaTest      = "org.scalatest"        %% "scalatest"       % Version.scalaTest
  val slf4jApi       = "org.slf4j"            %  "slf4j-api"       % Version.slf4j
  val sparkSQL       = "org.apache.spark"     %% "spark-sql"       % Version.spark
  val sparkML        = "org.apache.spark"     %% "spark-mllib"     % Version.spark
  val kryo           = "com.esotericsoftware" %  "kryo"            % Version.kryo
}

object Dependencies {
  import Library._

  val sparkAkkaHadoop = Seq(
    sparkSQL,
    sparkML,
    logbackClassic % "test",
    scalaTest      % "test",
    mockitoAll     % "test",
    kryo
  )
}

