package com.gu.mlExample.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}

import scala.reflect.runtime.universe._
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

trait Logging {
  val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)
}

trait LoggingSettings {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  private lazy val classSimpleName: String = this.getClass.getSimpleName

  def attemptRunWithLogging(f: () => Unit): Try[Unit] = {

    val effort = Try { f() }

    effort recover {
      case NonFatal(error) => {
        Logger.getRootLogger.error(s"error running ophan job for $classSimpleName", error)
        Failure(new Exception(s"Exception running $classSimpleName", error))
      }
    }
  }
}

case class SparkSource(tableName: String, location: String)

trait SparkUtils extends LoggingSettings {

  def run(spark: SparkSession)(f: () => Unit): Unit = {
    val attemptedRun: Try[Unit] = attemptRunWithLogging(f)

    spark.stop

    attemptedRun recover { case error => throw error }
  }

  def asClass[T <: Product : TypeTag](spark: SparkSession, table: DataFrame): RDD[T] = {

    import spark.implicits._

    val columns: Seq[Column] = typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => new Column(m.name.toString)
    }.toSeq

    table.select(columns: _*).as[T].rdd
  }

  def updateRawPartition(spark: SparkSession, source: SparkSource, date: String): Unit  = {
    val newPartitionQuery = s"alter table ${source.tableName} add if not exists partition (date='$date') location '${source.location}date=$date'"
    spark.sql(newPartitionQuery)
  }

  def refreshSchema[T <: Product : TypeTag](spark: SparkSession, source: SparkSource): Unit = {

    spark.sql(s"DROP TABLE IF EXISTS ${source.tableName}")

    val schema: StructType = Encoders.product[T].schema
    val catalog = spark.catalog
    catalog.createExternalTable(source.tableName, "ORC", schema, Map("path" -> source.location))
    ()
  }

}