import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_date, datediff, year, month, weekofyear, 
    dayofmonth, last_day, least, greatest, coalesce, lit, when
)
from pyspark.sql.types import BooleanType
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', "s3_bucket",
        "redshift_db", "redshift_schema", "jdbc_url", "awsuser", "aws_password", "aws_iam_role"])

s3_output_path = f"s3://{args['s3_bucket']}/curated/"
jdbc_url = args["jdbc_url"]
awsuser = args["awsuser"]
aws_password = args["aws_password"]
aws_iam_role = args["aws_iam_role"]

logger.info(f"data will be saved to s3 bucket: {s3_output_path}")
logger.info(f"Using JDBC URL: {jdbc_url}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

logger.info("Loading data from Glue Data Catalog...")

def load_table_from_catalog(table_name):
    return glueContext.create_dynamic_frame.from_catalog(
        database="rental-marketplace",
        table_name=table_name
    ).toDF()
    
raw_bookings_df = load_table_from_catalog("bookings_parquet")
raw_apartments_df = load_table_from_catalog("apartments_parquet")
raw_apartment_attr = load_table_from_catalog("apartments_attributes_parquet")
raw_user_view = load_table_from_catalog("user_viewing_parquet")

# Transform apartments data
logger.info("Transforming apartments data...")
apartments_curated_df = raw_apartments_df.join(raw_apartment_attr, "id", "left") \
    .select(
        col("id").alias("apartment_id"),
        coalesce(col("title"), lit("Unknown")).alias("title"),
        col("source"),
        coalesce(col("price"), lit(0)).alias("price"),
        coalesce(col("currency"), lit("USD")).alias("currency"),
        to_date(col("listing_created_on"), "dd/MM/yyyy").alias("listing_date"),
        col("is_active").cast(BooleanType()),
        col("last_modified_timestamp"),
        coalesce(col("category"), lit("Other")).alias("category"),
        col("body"),
        col("amenities"),
        coalesce(col("bathrooms"), lit(1)).alias("bathrooms"),
        coalesce(col("bedrooms"), lit(1)).alias("bedrooms"),
        coalesce(col("fee"), lit(0)).alias("fee"),
        col("has_photo").cast(BooleanType()),
        col("pets_allowed").cast(BooleanType()),
        col("price_display"),
        col("price_type"),
        coalesce(col("square_feet"), lit(0)).alias("square_feet"),
        col("address"),
        coalesce(col("cityname"), lit("Unknown")).alias("city"),
        col("state"),
        col("latitude"),
        col("longitude")
    ) \
    .withColumn("year", year(col("listing_date"))) \
    .withColumn("month", month(col("listing_date"))) \
    .withColumn("week", weekofyear(col("listing_date")))

# Transform bookings data
logger.info("Transforming bookings data...")
bookings_curated_df = raw_bookings_df \
    .withColumn("checkin_date", to_date(col("checkin_date"), "dd/MM/yyyy")) \
    .withColumn("checkout_date", to_date(col("checkout_date"), "dd/MM/yyyy")) \
    .filter(col("checkin_date").isNotNull() & col("checkout_date").isNotNull()) \
    .withColumn("checkin", least(col("checkin_date"), col("checkout_date"))) \
    .withColumn("checkout", greatest(col("checkin_date"), col("checkout_date"))) \
    .withColumn("booking_duration", datediff(col("checkout"), col("checkin"))) \
    .withColumn("days_in_month", dayofmonth(last_day(col("checkin")))) \
    .select(
        col("booking_id"),
        col("user_id"),
        col("apartment_id"),
        to_date(col("booking_date"), "dd/MM/yyyy").alias("booking_date"),
        col("checkin"),
        col("checkout"),
        col("booking_duration"),
        col("days_in_month"),
        coalesce(col("total_price"), lit(0)).alias("total_price"),
        coalesce(col("currency"), lit("USD")).alias("currency"),
        coalesce(col("booking_status"), lit("Unknown")).alias("status")
    )

# Transform user viewings data
logger.info("Transforming user viewings data...")
user_engagement_curated_df = raw_user_view \
    .withColumn("is_wishlisted", 
               when(col("is_wishlisted").cast("int") == 1, True)
               .otherwise(False)) \
    .select(
        col("user_id"),
        col("apartment_id"),
        col("viewed_at"),
        col("is_wishlisted").cast(BooleanType()),
        col("call_to_action")
    )

redshift_options = {
    "url": jdbc_url,
    "user": awsuser,
    "password": aws_password,
    "redshiftTmpDir": s3_output_path,
    "aws_iam_role": aws_iam_role
}

def write_to_redshift(df, table_name):
    try:
        logger.info(f"Writing {table_name} to Redshift...")
        redshift_options["dbtable"] = f"{args['redshift_schema']}.{table_name}"
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
        glueContext.write_dynamic_frame.from_options(dyf, connection_type="redshift", connection_options=redshift_options)
        logger.info(f"✅ Successfully written {table_name} to Redshift.")
    except Exception as e:
        logger.error(f"❌ Error writing {table_name} to Redshift: {str(e)}", exc_info=True)
        raise
    
write_to_redshift(apartments_curated_df, "dim_apartments")
write_to_redshift(bookings_curated_df, "fact_bookings")
write_to_redshift(user_engagement_curated_df, "dim_users")

job.commit()
logger.info("ETL job completed successfully")