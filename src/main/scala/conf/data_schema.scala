package conf

import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

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
    StructField("day", IntegerType))
  )

  val delta_records_schema: StructType = StructType(Array(
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
    StructField("Customer_type", StringType),
    StructField("load_timestamp", TimestampType),
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("day", IntegerType))
  )

  val factors_schema: StructType = StructType(Array(
    StructField("customer_id", StringType),
    StructField("ure_rating_ref_i", StringType),
    StructField("evaluation_type_code", StringType),
    StructField("evaluation_code", StringType),
    StructField("evaluation_value", StringType),
    StructField("load_timestamp",TimestampType),
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("day", IntegerType))
  )

  val delta_factors_schema: StructType = StructType(Array(
    StructField("customer_id", StringType),
    StructField("curr_rating_ref_i", StringType),
    StructField("evaluation_type_code", StringType),
    StructField("evaluation_code", StringType),
    StructField("evaluation_value", StringType),
    StructField("load_timestamp",TimestampType),
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("day", IntegerType))
  )

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
    StructField("day", IntegerType))
  )

  val final_details_schema: StructType = StructType(Array(
    StructField("customer_id",StringType),
    StructField("customer_name",StringType),
    StructField("customer_gender",StringType),
    StructField("customer_age",IntegerType),
    StructField("trigger_flag",StringType),
    StructField("curr_rating_value",StringType),
    StructField("curr_rating_timestamp",TimestampType),
    StructField("curr_rating_ref_i",StringType),
    StructField("curr_rating_srce_c",StringType),
    StructField("prev_rating_value",StringType),
    StructField("prev_rating_timestamp",TimestampType),
    StructField("prev_rating_ref_i",StringType),
    StructField("prev_rating_srce_c",StringType),
    StructField("Customer_type",StringType),
    StructField("Current_cases_details",ArrayType(
      StructType(Array(
        StructField("CaseCustomer",StringType),
        StructField("CaseTypeCode",StringType),
        StructField("CaseCode",StringType),
        StructField("CaseValue",StringType)))
    )),
    StructField("Previous_cases_details",ArrayType(
      StructType(Array(
        StructField("PreviousCaseCustomer",StringType),
        StructField("PreviousCaseTypeCode",StringType),
        StructField("PreviousCaseCode",StringType),
        StructField("PreviousCaseValue",StringType)))
    )),
    StructField("load_timestamp",TimestampType),
    StructField("year",IntegerType),
    StructField("month",IntegerType),
    StructField("day",IntegerType))
  )
}
