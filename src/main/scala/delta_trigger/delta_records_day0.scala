package delta_trigger

import conf.data_schema.ratings_schema
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object delta_records_day0 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Pick Delta Records").config("spark.master","local").getOrCreate()

    val ratings = spark.read.format("csv").schema(ratings_schema).options(Map("mode"->"failFast", "nullValue"->""))
      .load("src/main/resources/Data/ValidFormat/Consolidated_Rating.csv")

    val day0 = "2021-01-01"

    ratings.filter(to_date(col("load_timestamp")) === lit(day0))
      .filter("curr_rating_value IN ('VERY HIGH','LOW','MEDIUM','HIGH')")
      .write.mode(SaveMode.Overwrite).save("src/main/resources/Output_Data/Delta_customers/Day0.parquet")

  }
}
