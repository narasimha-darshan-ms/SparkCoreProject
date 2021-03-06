package delta_trigger

import conf.data_schema.ratings_schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, to_date, when}
import org.apache.spark.sql.types.{IntegerType, TimestampType}

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object delta_records_BAU {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Pick Delta Records").config("spark.master","local").getOrCreate()

    val day0 = "2021-01-01"
    val prev_day = "2021-01-07"
    val curr_day = "2021-01-08"

    val ratings = spark.read.format("csv").schema(ratings_schema).options(Map("mode"->"failFast", "nullValue"->""))
      .load("src/main/resources/Data/ValidFormat/Consolidated_Rating.csv")

    val prev_day_DF = ratings.filter(to_date(col("load_timestamp")) === lit(prev_day))
      .filter("curr_rating_value IN ('VERY HIGH','LOW','MEDIUM','HIGH')")

    val curr_day_DF = ratings.filter(to_date(col("load_timestamp")) === lit(curr_day))
      .filter("curr_rating_value IN ('VERY HIGH','LOW','MEDIUM','HIGH')")

    import spark.implicits._
    val delta_joined_DF = curr_day_DF.as("a")
      .join(prev_day_DF.as("b"), $"a.customer_id" === $"b.customer_id", "left")

    val delta_DF = delta_joined_DF.where($"a.curr_rating_timestamp" > $"b.curr_rating_timestamp")
      .unionAll(delta_joined_DF.where($"b.customer_id".isNull))
      .select($"a.customer_id",$"a.customer_name",$"a.customer_gender",$"a.customer_age",$"a.trigger_flag",
        $"a.curr_rating_value",$"a.curr_rating_timestamp",$"a.curr_rating_ref_i",$"a.curr_rating_srce_c",
        $"a.prev_rating_value",$"a.prev_rating_timestamp",$"a.prev_rating_ref_i",$"a.prev_rating_srce_c")
      .withColumn("Customer_type", when($"a.prev_rating_ref_i".isNull, "New Customer")
        .otherwise("Existing Customer"))
      .withColumn("load_timestamp",lit(curr_day+" 12:00:00").cast(TimestampType))
      .withColumn("year", lit(curr_day.substring(0,4)).cast(IntegerType))
      .withColumn("month", lit(curr_day.substring(5,7)).cast(IntegerType))
      .withColumn("day", lit(curr_day.substring(8,10)).cast(IntegerType))

    val start = LocalDate.parse(day0)
    val end = LocalDate.parse(curr_day)
    val between = ChronoUnit.DAYS.between(start,end).toInt

    delta_DF.write.mode(SaveMode.Overwrite)
      .save(s"src/main/resources/Output_Data/Delta_customers/Day$between.parquet")

    spark.stop()
  }
}
