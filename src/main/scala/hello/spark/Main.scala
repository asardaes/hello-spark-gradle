package hello.spark

import hello.spark.dataframe.{DataFrameOperations, DataFrameStatistics, DataFrameTutorial}
import hello.spark.sql.SparkSQLTutorial
import hello.spark.utils.Context

object Main extends App with Context {
  sparkSession.sparkContext.setLogLevel("WARN")
  DataFrameTutorial.run(sparkSession)
  SparkSQLTutorial.run(sparkSession)
  DataFrameStatistics.run(sparkSession)
  DataFrameOperations.run(sparkSession)
  sparkSession.stop()
}
