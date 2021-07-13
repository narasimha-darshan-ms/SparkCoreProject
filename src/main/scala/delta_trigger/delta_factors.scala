package delta_trigger

import conf.data_schema.{factors_schema, flags_schema, ratings_schema}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, map, to_date, when}
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import java.time.LocalDate
import java.time.temporal.ChronoUnit

object delta_factors {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Combined Factors").config("spark.master","local").getOrCreate()

    val day0 = "2021-01-01"
    val curr_day = "2021-01-01"

    val between = ChronoUnit.DAYS.between(LocalDate.parse(day0), LocalDate.parse(curr_day)).toInt

    /* Loading required dataframes */
    val delta_records_DF = spark.read.schema(ratings_schema)
      .load(s"src/main/resources/Output_Data/Delta_customers/Day$between.parquet/*.snappy.parquet")
      .filter(to_date(col("load_timestamp")) === lit(curr_day))

    val factors_DF = spark.read.format("csv").schema(factors_schema)
      .options(Map("mode"->"failFast", "nullValue"->"")).load("src/main/resources/Data/ValidFormat/factors.csv")
      .filter(to_date(col("load_timestamp")) === lit(curr_day))

    val flags_DF = spark.read.format("csv").schema(flags_schema)
      .options(Map("mode"->"failFast", "nullValue"->"")).load("src/main/resources/Data/ValidFormat/Flags.csv")
      .filter(to_date(col("load_timestamp")) === lit(curr_day))

    import spark.implicits._
    val join1DF = delta_records_DF.as("delta")
      .join(factors_DF.as("factors"),$"delta.curr_rating_ref_i" === $"factors.ure_rating_ref_i")
      .select($"factors.customer_id",$"factors.ure_rating_ref_i",$"factors.evaluation_type_code",
        $"factors.evaluation_code",$"factors.evaluation_value")
      .withColumnRenamed("ure_rating_ref_i","curr_rating_ref_i")

    val join2DF = delta_records_DF.as("delta")
      .join(flags_DF.as("flags"),$"delta.curr_rating_ref_i" === $"flags.curr_rating_ref_i", "left")
      .withColumn("traffic_violation", lit("TRAFFIC VIOLATION FLAG"))
      .withColumn("license", lit("LICENSE FLAG"))
      .withColumn("illegal_parking", lit("ILLEGAL PARKING FLAG"))
      .withColumn("age", lit("AGE FLAG"))
      .withColumn("map", map('traffic_violation, $"flags.traffic_violation_flag",
        'license ,$"flags.license_flag",
        'illegal_parking ,$"flags.illegal_parking_flag",
        'age ,$"flags.age_f"))
      .select($"delta.customer_id",$"flags.curr_rating_ref_i",$"flags.traffic_violation_score",
        explode('map).as(Seq("evaluation_type_code","evaluation_code")))
      .withColumn("evaluation_value",
        when('evaluation_type_code === "TRAFFIC VIOLATION FLAG", 'traffic_violation_score)
          .otherwise(null))
      .drop('traffic_violation_score)

    val combinedDF = join1DF.unionAll(join2DF)
      .withColumn("load_timestamp",lit(curr_day+" 12:00:00").cast(TimestampType))
      .withColumn("year", lit(curr_day.substring(0,4)).cast(IntegerType))
      .withColumn("month", lit(curr_day.substring(5,7)).cast(IntegerType))
      .withColumn("day", lit(curr_day.substring(8,10)).cast(IntegerType))

    combinedDF.write.mode(SaveMode.Overwrite)
      .save(s"src/main/resources/Output_Data/Factors/Day$between.parquet")
  }
}
