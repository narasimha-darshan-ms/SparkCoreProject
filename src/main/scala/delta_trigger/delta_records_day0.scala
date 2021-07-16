package delta_trigger

import conf.data_schema.ratings_schema
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object delta_records_day0 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Pick Delta Records").config("spark.master","local").getOrCreate()

    val ratingsDF = spark.read.format("csv").schema(ratings_schema).options(Map("mode"->"failFast", "nullValue"->""))
      .load("src/main/resources/Data/ValidFormat/Consolidated_Rating.csv")

    val day0 = "2021-01-01"

    val valid_ratingsDF = ratingsDF.filter(to_date(col("load_timestamp")) === lit(day0))
      .filter("curr_rating_value IN ('VERY HIGH','LOW','MEDIUM','HIGH')")

    import spark.implicits._
    val Final_ratings_DF = valid_ratingsDF.as("a")
      .select($"a.customer_id",$"a.customer_name",$"a.customer_gender",$"a.customer_age",$"a.trigger_flag",
        $"a.curr_rating_value",$"a.curr_rating_timestamp",$"a.curr_rating_ref_i",$"a.curr_rating_srce_c",
        $"a.prev_rating_value",$"a.prev_rating_timestamp",$"a.prev_rating_ref_i",$"a.prev_rating_srce_c")
      .withColumn("Customer_type", when($"a.prev_rating_ref_i".isNull, "New Customer")
        .otherwise("Existing Customer"))
      .withColumn("load_timestamp",lit(day0 + " 12:00:00").cast(TimestampType))
      .withColumn("year", lit(day0.substring(0,4)).cast(IntegerType))
      .withColumn("month", lit(day0.substring(5,7)).cast(IntegerType))
      .withColumn("day", lit(day0.substring(8,10)).cast(IntegerType))

      Final_ratings_DF.write.mode(SaveMode.Overwrite)
        .save("src/main/resources/Output_Data/Delta_customers/Day0.parquet")

    spark.stop()
  }
}
