package hello.spark

import hello.spark.dataframe.{DataFrameStatistics, DataFrameTutorial}
import hello.spark.sql.SparkSQLTutorial
import hello.spark.utils.Context

object Main extends App with Context {
  sparkSession.sparkContext.setLogLevel("WARN")
  DataFrameTutorial.run(sparkSession)
  SparkSQLTutorial.run(sparkSession)
  DataFrameStatistics.run(sparkSession)
  sparkSession.stop()
}
