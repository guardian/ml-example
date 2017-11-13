package com.gu.mlExample

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.{SparkConf}


object Runner {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("CountingSheep")

  val sparkSession = SparkSession.builder().config(conf).getOrCreate()

  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")

  val pipeline = new Pipeline().setStages(Array(tokenizer))

  val inputData = sparkSession.createDataFrame(Seq(
    (0L, "a b c d e spark", 1.0),
    (1L, "b d", 0.0),
    (2L, "spark f g h", 1.0),
    (3L, "hadoop mapreduce", 0.0),
    (4L, "b spark who", 1.0),
    (5L, "g d a y", 0.0),
    (6L, "spark fly", 1.0),
    (7L, "was mapreduce", 0.0),
    (8L, "e spark program", 1.0),
    (9L, "a e c l", 0.0),
    (10L, "spark compile", 1.0),
    (11L, "hadoop software", 0.0)
  )).toDF("id", "text", "label")

  val model = pipeline.fit(inputData);

  val realData = sparkSession.createDataFrame(Seq((23, "example of hello world"))).toDF("id", "text")

  val transformedData = model.transform(realData)

  transformedData.select("tokens").show()

}
