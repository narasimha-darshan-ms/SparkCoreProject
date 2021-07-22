package delta_trigger

import conf.data_schema.{delta_factors_schema, delta_records_schema}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object combined_details {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Combined details").config("spark.master","local").getOrCreate()

    val day0 = "2021-01-01"
    val curr_day = "2021-01-08"

    val between = ChronoUnit.DAYS.between(LocalDate.parse(day0), LocalDate.parse(curr_day)).toInt

    val delta_records_DF = spark.read.schema(delta_records_schema)
      .load(s"src/main/resources/Output_Data/Delta_customers/Day$between.parquet/*.snappy.parquet")
      .filter(to_date(col("load_timestamp")) === lit(curr_day))

    val combined_Factors_DF = spark.read.schema(delta_factors_schema)
      .load(s"src/main/resources/Output_Data/Factors/Day*.parquet/*.snappy.parquet")

    import spark.implicits._

    val curr_details_DF = delta_records_DF.as("delta")
      .join(combined_Factors_DF.as("facts")
        ,$"delta.curr_rating_ref_i" === $"facts.curr_rating_ref_i")
      .groupBy($"delta.customer_id",$"delta.customer_name",$"delta.customer_gender",$"delta.customer_age",
        $"delta.trigger_flag",$"delta.curr_rating_value",$"delta.curr_rating_timestamp",$"delta.curr_rating_ref_i",
        $"delta.curr_rating_srce_c",$"delta.prev_rating_value",$"delta.prev_rating_timestamp",
        $"delta.prev_rating_ref_i",$"delta.prev_rating_srce_c",$"delta.Customer_type").agg(
      collect_list(
        struct($"facts.customer_id".as("CaseCustomer"),
        $"facts.evaluation_type_code".as("CaseTypeCode"),
        $"facts.evaluation_code".as("CaseCode"),
        $"facts.evaluation_value".as("CaseValue"))).alias("Current_cases_details"))
      .select($"delta.customer_id",'Current_cases_details)

    val prev_details_DF = delta_records_DF.as("delta")
      .join(combined_Factors_DF.as("prev_facts"),$"delta.prev_rating_ref_i" === $"prev_facts.curr_rating_ref_i")
      .groupBy($"delta.customer_id",$"delta.customer_name",$"delta.customer_gender",$"delta.customer_age",
        $"delta.trigger_flag",$"delta.curr_rating_value",$"delta.curr_rating_timestamp",$"delta.curr_rating_ref_i",
        $"delta.curr_rating_srce_c",$"delta.prev_rating_value",$"delta.prev_rating_timestamp",
        $"delta.prev_rating_ref_i",$"delta.prev_rating_srce_c",$"delta.Customer_type").agg(
      collect_list(
        struct($"prev_facts.customer_id".as("PreviousCaseCustomer"),
          $"prev_facts.evaluation_type_code".as("PreviousCaseTypeCode"),
          $"prev_facts.evaluation_code".as("PreviousCaseCode"),
          $"prev_facts.evaluation_value".as("PreviousCaseValue"))).alias("Previous_cases_details"))
      .select($"delta.customer_id",'Previous_cases_details)

    val details_DF = delta_records_DF.as("delta").join(curr_details_DF.as("curr"),
      $"curr.customer_id" === $"delta.customer_id","left").join(prev_details_DF.as("prev"),
      $"prev.customer_id" === $"delta.customer_id","left")
      .select($"delta.customer_id",$"delta.customer_name",$"delta.customer_gender",$"delta.customer_age",
        $"delta.trigger_flag",$"delta.curr_rating_value",$"delta.curr_rating_timestamp",$"delta.curr_rating_ref_i",
        $"delta.curr_rating_srce_c",$"delta.prev_rating_value",$"delta.prev_rating_timestamp",
        $"delta.prev_rating_ref_i",$"delta.prev_rating_srce_c",$"delta.Customer_type",$"curr.Current_cases_details",
        $"prev.Previous_cases_details")
      .withColumn("load_timestamp",lit(curr_day+" 12:00:00").cast(TimestampType))
      .withColumn("year", lit(curr_day.substring(0,4)).cast(IntegerType))
      .withColumn("month", lit(curr_day.substring(5,7)).cast(IntegerType))
      .withColumn("day", lit(curr_day.substring(8,10)).cast(IntegerType))

     details_DF.write.mode(SaveMode.Overwrite)
      .save(s"src/main/resources/Output_Data/Combined/Day$between.parquet")

    spark.stop()
  }
}
