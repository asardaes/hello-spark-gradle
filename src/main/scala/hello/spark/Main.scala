package hello.spark

import hello.spark.dataframe.DataFrameTutorial
import hello.spark.utils.Context

object Main extends App with Context {
  sparkSession.sparkContext.setLogLevel("WARN")
  DataFrameTutorial.run(sparkSession)
}
