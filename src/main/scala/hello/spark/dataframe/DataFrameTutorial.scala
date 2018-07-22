package hello.spark.dataframe

import hello.spark.utils.Context

object DataFrameTutorial extends App with Context {
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)
  dfTags.filter("tag == 'php'").show(10)
  println(s"Number of php tags = ${ dfTags.filter("tag == 'php'").count() }")

  dfTags
    .groupBy("tag")
    .count()
    .filter("count > 5")
    .orderBy("tag")
    .show(10)

  dfTags
    .select("tag")
    .distinct()
    .show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  dfQuestionsCSV.printSchema()

  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  dfQuestions.printSchema()

  val dfQuestionsSubset = dfQuestions
      .filter("score > 400 AND score < 410")
      .toDF()

  dfQuestionsSubset
    .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))
    .select("owner_userid", "tag", "creation_date", "score")
    .show(10)

  sparkSession.stop()
}
