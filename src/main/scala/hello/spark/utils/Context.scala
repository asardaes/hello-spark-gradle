package hello.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  // since this is lazy, .setMaster can be called in test, but left alone for spark-submit
  lazy val sparkConf: SparkConf = new SparkConf()
    .setAppName("Hello Spark")

  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}
