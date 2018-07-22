package hello.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkConf: SparkConf = new SparkConf()
    .setAppName("Hello Spark")
    .setMaster("local[*]")
    .set("spark.cores.max", "4")

  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}
