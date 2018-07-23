package hello.spark.dataframe

import hello.spark.utils.Context

object DataFrameStatisticsTest extends App with Context {
  sparkConf.setMaster("local[*]")
  sparkSession.sparkContext.setLogLevel("WARN")
  sparkSession.sparkContext.addFile("spark-warehouse/question_tags_10K.csv")
  sparkSession.sparkContext.addFile("spark-warehouse/questions_10K.csv")
  DataFrameStatistics.run(sparkSession)
  sparkSession.stop()
}