package delta_trigger

import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object delta_records_day0 {
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

    val ratings = spark.read.format("csv").schema(dataSchema).options(Map("mode"->"failFast", "nullValue"->""))
      .load("src/main/resources/Data/ValidFormat/Consolidated_Rating.csv")

    val day0 = "2021-01-01"

    ratings.filter(to_date(col("load_timestamp")) === lit(day0))
      .filter("curr_rating_value IN ('VERY HIGH','LOW','MEDIUM','HIGH')")
      .write.mode(SaveMode.Overwrite).save("src/main/resources/Output_Data/Delta_customers/Day0.parquet")

  }
}
