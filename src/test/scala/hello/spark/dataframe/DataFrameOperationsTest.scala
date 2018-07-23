package hello.spark.dataframe

import hello.spark.utils.Context

object DataFrameOperationsTest extends App with Context {
  sparkConf.setMaster("local[*]")
  sparkSession.sparkContext.setLogLevel("WARN")
  sparkSession.sparkContext.addFile("spark-warehouse/question_tags_10K.csv")
  sparkSession.sparkContext.addFile("spark-warehouse/questions_10K.csv")
  DataFrameOperations.run(sparkSession)
  sparkSession.stop()
}