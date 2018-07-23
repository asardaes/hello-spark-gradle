package hello.spark.dataframe

import org.apache.spark.SparkFiles
import org.apache.spark.sql.{Dataset, SparkSession}

// see https://stackoverflow.com/a/39659028/5793905
private case class Tag(id: Int, tag: String) {
  override def toString: String = s"id = $id, tag = $tag"
}

private case class Question(owner_userid: Int, tag: String, creationDate: java.sql.Timestamp, score: Int) {
  override def toString: String = s"owner userid = $owner_userid, " +
    s"tag = $tag, " +
    s"creation date = $creationDate, " +
    s"score = $score"
}

object DataFrameOperations {
  private def toQuestion(row: org.apache.spark.sql.Row): Question = {
    // to normalize owner_userid data
    val IntOf: String => Option[Int] = {
      case s if s == "NA" => None
      case s => Some(s.toInt)
    }

    import java.time._
    val DateOf: String => java.sql.Timestamp = s => java.sql.Timestamp.valueOf(ZonedDateTime.parse(s).toLocalDateTime)

    Question (
      owner_userid = IntOf(row.getString(0)).getOrElse(-1),
      tag = row.getString(1),
      creationDate = DateOf(row.getString(2)),
      score = row.getString(3).toInt
    )
  }

  def run(sparkSession: SparkSession): Unit = {
    val dfTags = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(SparkFiles.get("question_tags_10K.csv"))
      .toDF("id", "tag")

    val dfQuestions = sparkSession
      .read
      .option("header", value = false)
      .option("inferSchema", value = true)
      .option("dateFormat","yyyy-MM-dd HH:mm:ss")
      .csv(SparkFiles.get("questions_10K.csv"))
      .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")
      .filter("score > 400 and score < 410")
      .join(dfTags, "id")
      .select("owner_userid", "tag", "creation_date", "score")
      .toDF()

    import sparkSession.implicits._

    val dfTagsOfTag: Dataset[Tag] = dfTags.as[Tag]
    dfTagsOfTag
      .take(10)
      .foreach(println)

    println()

    dfQuestions
      .map(toQuestion)
      .take(10)
      .foreach(println)

    val seqTags = Seq(
      1 -> "so_java",
      1 -> "so_jsp",
      2 -> "so_erlang",
      3 -> "so_scala",
      3 -> "so_akka"
    )

    val dfMoreTags = seqTags.toDF("id", "tag")
    dfTags
      .union(dfMoreTags)
      .filter("id IN (1,3)")
      .show(10)

    import org.apache.spark.sql.functions._
    val dfSplitColumn = dfMoreTags
      .withColumn("tmp", split($"tag", "_"))
      .select(
        $"id",
        $"tag",
        $"tmp".getItem(0).as("so_prefix"),
        $"tmp".getItem(1).as("so_tag")
      )
      .drop("tmp")
    dfSplitColumn.show(10)
  }
}
