package hello.spark.sql

import hello.spark.utils.Context

object SparkSQLTutorialTest extends Context {
  def main (args: Array[String]) {
    sparkConf.setMaster("local[*]")
    sparkSession.sparkContext.setLogLevel("WARN")
    sparkSession.sparkContext.addFile("spark-warehouse/question_tags_10K.csv")
    sparkSession.sparkContext.addFile("spark-warehouse/questions_10K.csv")
    SparkSQLTutorial.run(sparkSession)
    sparkSession.stop()
  }
}
