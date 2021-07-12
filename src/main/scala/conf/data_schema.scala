package conf

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object data_schema {

  val ratings_schema: StructType = StructType(Array(
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

  val factors_schema: StructType = StructType(Array(
    StructField("customer_id", StringType),
    StructField("ure_rating_ref_i", StringType),
    StructField("evaluation_type_code", StringType),
    StructField("evaluation_code", StringType),
    StructField("evaluation_value", StringType),
    StructField("load_timestamp",TimestampType),
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("day", IntegerType)
  ))

  val flags_schema: StructType = StructType(Array(
    StructField("curr_rating_ref_i", StringType),
    StructField("score",DoubleType),
    StructField("traffic_violation_score", IntegerType),
    StructField("traffic_violation_flag", StringType),
    StructField("license_flag", StringType),
    StructField("illegal_parking_flag", StringType),
    StructField("age_f", StringType),
    StructField("load_timestamp", TimestampType),
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("day", IntegerType)
  ))
}
