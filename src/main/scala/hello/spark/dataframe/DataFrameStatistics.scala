package hello.spark.dataframe

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object DataFrameStatistics {
  def run(sparkSession: SparkSession): Unit = {
    val dfTags = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(SparkFiles.get("question_tags_10K.csv"))
      .toDF("id", "tag")

    val dfQuestionsCSV = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat","yyyy-MM-dd HH:mm:ss")
      .csv(SparkFiles.get("questions_10K.csv"))
      .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

    val dfQuestions = dfQuestionsCSV.select(
      dfQuestionsCSV.col("id").cast("integer"),
      dfQuestionsCSV.col("creation_date").cast("timestamp"),
      dfQuestionsCSV.col("closed_date").cast("timestamp"),
      dfQuestionsCSV.col("deletion_date").cast("date"),
      dfQuestionsCSV.col("score").cast("integer"),
      dfQuestionsCSV.col("owner_userid").cast("integer"),
      dfQuestionsCSV.col("answer_count").cast("integer")
    )

    import org.apache.spark.sql.functions._
    dfQuestions
      .select(avg("score"))
      .show()

    dfQuestions
      .filter("id > 400 AND id < 450")
      .filter("owner_userid is not null")
      .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
      .groupBy(dfQuestions.col("owner_userid"))
      .agg(avg("score"), max("answer_count"))
      .show()

    dfTags.describe().show()

    println(s"correlation between column score and answer_count = " +
      s"${dfQuestions.stat.corr("score", "answer_count")}")

    dfQuestions
      .filter("owner_userid > 0 AND owner_userid < 20")
      .stat
      .crosstab("score", "owner_userid")
      .show(10)

    dfQuestions
      .filter("owner_userid > 0")
      .filter("answer_count IN (5, 10, 20)")
      .stat
      .sampleBy("answer_count", Map(5 -> 0.5, 10 -> 0.1, 20 -> 1.0), 7L)
      .groupBy("answer_count")
      .count()
      .show()

    val quantiles = dfQuestions
      .stat
      .approxQuantile("score", Array(0, 0.5, 1), 0.25)
    println(s"Qauntiles segments = ${quantiles.toSeq}")
  }
}
