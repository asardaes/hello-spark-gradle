package hello.spark.dataframe

import hello.spark.utils.Context

object DataFrameTutorialTest extends Context {
  def main (args: Array[String]) {
    sparkConf.setMaster("local[*]")
    sparkSession.sparkContext.setLogLevel("WARN")
    sparkSession.sparkContext.addFile("spark-warehouse/question_tags_10K.csv")
    sparkSession.sparkContext.addFile("spark-warehouse/questions_10K.csv")
    DataFrameTutorial.run(sparkSession)
    sparkSession.stop()
  }
}
