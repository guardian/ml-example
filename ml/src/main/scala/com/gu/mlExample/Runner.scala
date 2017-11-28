package com.gu.mlExample

import java.time.{LocalDate, Period}


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import com.gu.mlExample.utils.SparkUtils
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try


case class PageviewData(
  pageViewId: String,
  browserId: Option[String] = None,
  countryCode: Option[String] = None,
  attentionTime: Option[Double] = None,
  averageDaysBetweenRecentVisits: Option[Double] = None
)

case class AcquisitionsData(
  pageViewId: String,
  amount: Option[Double] = None,
  currency: Option[String] = None,
  browserId: Option[String] = None
)

case class DatasetData(
  countryCode: Option[String] = None,
  attentionTime: Option[Double] = None,
  averageDaysBetweenRecentVisits: Option[Double] = None,
  label: Boolean
)


object Runner extends SparkUtils {

  lazy val logger = LoggerFactory.getLogger(getClass)

  def readPageViewFromClean(date: LocalDate)(implicit spark: SparkSession): RDD[(String, PageviewData)] = {

    val yesterday = date.minus(Period.ofDays(1))

    spark.table("clean.pageview")
      .select("page_view_id", "browser_id", "country_code", "attention_time", "avg_days_between_recent_visits")
      .filter(s"received_date >= '$yesterday'")
      .filter(s"received_date <= '$date'")
      .rdd
      .map { row =>
        val a = PageviewData(
          pageViewId = row.getString(0),
          browserId = Try(row.getString(1)).toOption,
          countryCode = Try(row.getString(2)).toOption,
          attentionTime = Try(row.getDouble(3)).toOption,
          averageDaysBetweenRecentVisits = Try(row.getDouble(4)).toOption)
        (a.pageViewId, a)
      }
  }

  def readAcquisitionFromClean(date: LocalDate)(implicit spark: SparkSession): RDD[(String, AcquisitionsData)] = {

    val yesterday = date.minus(Period.ofDays(1))

    spark.table("clean.acquisitions")
      .select("page_view_id", "amount", "currency", "browser_id")
      .filter(s"received_date >= '$yesterday'")
      .filter(s"received_date <= '$date'")
      .rdd
      .map { row =>
        val a = AcquisitionsData(
          pageViewId = row.getString(0),
          amount = Try(row.getDouble(1)).toOption,
          currency = Try(row.getString(2)).toOption,
          browserId = Try(row.getString(3)).toOption)
        (a.pageViewId, a)
      }
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val from = LocalDate.parse(args(0))
    val to: LocalDate = args.lift(1).map(LocalDate.parse).getOrElse(from.plusDays(1)) // default to `from + 1` i.e. run only for `from`

    val conf = new SparkConf()
      .setAppName("Acquisitions")
      .set("hive.exec.dynamic.partition", "true") // allow dynamic partitioning (default=false prior to Hive 0.9.0)
      .set("hive.exec.dynamic.partition.mode", "nonstrict") // setting required because the only partition is dynamic

    implicit val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()


    run(spark) { () =>
      DateUtil.getDatesBetween(from, to).foreach { date =>
        val acquisitionsTable = readAcquisitionFromClean(date)
        val pageViewTable = readPageViewFromClean(date)

        val datasetData = pageViewTable.leftOuterJoin(acquisitionsTable).values.map(
          tuple => DatasetData(
            countryCode = tuple._1.countryCode,
            attentionTime = tuple._1.attentionTime,
            averageDaysBetweenRecentVisits = tuple._1.averageDaysBetweenRecentVisits,
            label = tuple._2.isDefined))

        val datasetDataIter2 = datasetData.map( data => (if(data.label) 1.0 else 0.0, Vectors.dense(data.attentionTime.getOrElse(0.0), data.averageDaysBetweenRecentVisits.getOrElse(0))))

        val trainingData = spark.sqlContext.createDataFrame(datasetDataIter2).toDF("label", "features");


        // Create a LogisticRegression instance. This instance is an Estimator.
        val lr = new LogisticRegression()
        // Print out the parameters, documentation, and any default values.
        println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

        // We may set parameters using setter methods.
        lr.setMaxIter(10)
          .setRegParam(0.01)

        // Learn a LogisticRegression model. This uses the parameters stored in lr.
        val model1 = lr.fit(trainingData)
        // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
        // we can view the parameters it used during fit().
        // This prints the parameter (name: value) pairs, where names are unique IDs for this
        // LogisticRegression instance.
        println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

        // We may alternatively specify parameters using a ParamMap,
        // which supports several methods for specifying parameters.
        val paramMap = ParamMap(lr.maxIter -> 20)
          .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
          .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

        // One can also combine ParamMaps.
        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
        val paramMapCombined = paramMap ++ paramMap2

        // Now learn a new model using the paramMapCombined parameters.
        // paramMapCombined overrides all parameters set earlier via lr.set* methods.
        val model2 = lr.fit(trainingData, paramMapCombined)
        println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

        // Prepare test data.
        //val test = spark.createDataFrame(Seq(
        //  (1.0, Vectors.dense(1.5, 1.3)),
        //  (0.0, Vectors.dense(2.0, -0.1)),
        //  (1.0, Vectors.dense(2.2, -1.5))
        // )).toDF("label", "features")

        // Make predictions on test data using the Transformer.transform() method.
        // LogisticRegression.transform will only use the 'features' column.
        // Note that model2.transform() outputs a 'myProbability' column instead of the usual
        // 'probability' column since we renamed the lr.probabilityCol parameter previously.
//        model2.transform(test)
//          .select("features", "label", "myProbability", "prediction")
//          .collect()
//          .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
//            println(s"($features, $label) -> prob=$prob, prediction=$prediction")
//          }

        model1.write.overwrite().save(s"s3://santiago-hackday-2017-prediction/models/model1")

      }
    }
  }
}