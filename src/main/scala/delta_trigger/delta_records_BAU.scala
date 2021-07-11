package delta_trigger

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_date}
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object delta_records_BAU {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Pick Delta Records").config("spark.master","local").getOrCreate()

    val dataSchema = StructType(Array(
      StructField("customer_id", StringType),
      StructField("customer_name", StringType),
      StructField("customer_gender", StringType),
      StructField("customer_age", IntegerType),
      StructField("trigger_flag", StringType),
      StructField("curr_rating_value", StringType),
      StructField("curr_rating_timestamp", TimestampType),
      StructField("curr_rating_ref_i", StringType),
      StructField("curr_rating_srce_c", StringType),
      StructField("prev_rating_value",StringType),
      StructField("prev_rating_timestamp", TimestampType),
      StructField("prev_rating_ref_i", StringType),
      StructField("prev_rating_srce_c", StringType),
      StructField("load_timestamp", TimestampType),
      StructField("year", IntegerType),
      StructField("month", IntegerType),
      StructField("day", IntegerType)
    ))

    val day0 = "2021-01-01"
    val prev_day = "2021-01-07"
    val curr_day = "2021-01-08"

    val ratings = spark.read.format("csv").schema(dataSchema).options(Map("mode"->"failFast", "nullValue"->""))
      .load("src/main/resources/Data/ValidFormat/Consolidated_Rating.csv")

    val prev_day_DF = ratings.filter(to_date(col("load_timestamp")) === lit(prev_day))
      .filter("curr_rating_value IN ('VERY HIGH','LOW','MEDIUM','HIGH')")

    val curr_day_DF = ratings.filter(to_date(col("load_timestamp")) === lit(curr_day))
      .filter("curr_rating_value IN ('VERY HIGH','LOW','MEDIUM','HIGH')")

    import spark.implicits._
    val delta_joined_DF = curr_day_DF.as("a")
      .join(prev_day_DF.as("b"), $"a.customer_id" === $"b.customer_id", "left")

    val delta_DF = delta_joined_DF.where($"a.curr_rating_timestamp" > $"b.curr_rating_timestamp")
      .unionAll(delta_joined_DF.where($"b.customer_id".isNull)).select($"a.*")

    val start = LocalDate.parse(day0)
    val end = LocalDate.parse(curr_day)
    val between = ChronoUnit.DAYS.between(start,end).toInt

    delta_DF.show()
    //delta_DF.write.mode(SaveMode.Overwrite).save(s"src/main/resources/Output_Data/Delta_customers/Day$between.parquet")
  }
}
