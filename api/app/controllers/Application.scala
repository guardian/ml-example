package controllers

import play.api.libs.json.Json
import play.api.mvc.{Action, Controller, Result}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.{Vector, Vectors}

class Application extends Controller {


	// Force loading of models
	SparkCommons

	def index = Action {
		Ok("ML Example API 💡")
	}

	def classify(attentionTime: Double, averageDaysBetweenRecentVisits: Double) = Action { implicit request =>
    val result = getClassification(attentionTime, averageDaysBetweenRecentVisits)
      Ok(Json.toJson(result.map(_._2).getOrElse(-1.0)))
	}

	def healthcheck() = Action {
		Ok("")
	}

	private def getClassification(attentionTime: Double, averageDaysBetweenRecentVisits: Double): Option[(Vector, Double)] = {


		val spark = SparkCommons.session


    val test = spark.createDataFrame(Seq((1.0, Vectors.dense(attentionTime, averageDaysBetweenRecentVisits)))).toDF("label", "features")

    SparkCommons
      .logModel
      .transform(test)
      .select("features", "label", "probability", "prediction")
      .collect()
      .headOption map {
        case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
          (prob, prediction)
      }
  }
}

object SparkCommons {
	val conf = new SparkConf(false)
		.setMaster("local[*]")
		.set("spark.logConf", "true")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer.mb", "24")

	val session = SparkSession
		.builder()
		.appName("MLExampleWebApp")
		.config(conf)
		.getOrCreate()

	val sqlContext = session.sqlContext
	val logModel = {
    try {
      LogisticRegressionModel.load("file:///Users/santiago_fernandez/Projects/ml-example/api/conf/model")
    } catch {
      case e =>
        e.printStackTrace()
        null
    }
  }

}

