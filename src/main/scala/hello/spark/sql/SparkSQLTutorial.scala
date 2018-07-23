package hello.spark.sql

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object SparkSQLTutorial {
  def run(sparkSession: SparkSession): Unit = {
    val dfTags = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(SparkFiles.get("question_tags_10K.csv"))
      .toDF("id", "tag")

    dfTags.createOrReplaceTempView("so_tags")
    sparkSession.catalog.listTables().show()
    sparkSession.sql("SHOW tables").show()

    sparkSession
      .sql("SELECT id, tag FROM so_tags LIMIT 10")
      .show()

    sparkSession
      .sql(
        """
          |SELECT
          |  COUNT(*) AS php_count
          |FROM
          |  so_tags
          |WHERE
          |  tag = 'php'
        """.stripMargin
      )
      .show()

    val dfQuestionsCSV = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
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

    val dfQuestionsSubset = dfQuestions
      .filter("score > 400 AND score < 410")
      .toDF()

    dfQuestionsSubset.createOrReplaceTempView("so_questions")

    sparkSession
      .sql(
        """
          |SELECT t.*, q.*
          |FROM so_questions q
          |INNER JOIN so_tags t
          |ON t.id = q.id
        """.stripMargin
      )
      .show(10)

    def prefixStackOverflow(s: String): String = s"so_$s"
    sparkSession
      .udf
      .register("prefix_so", prefixStackOverflow _)

    sparkSession
      .sql("""SELECT id, prefix_so(tag) FROM so_tags""")
      .show(10)
  }
}
