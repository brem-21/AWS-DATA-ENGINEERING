import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', "s3_bucket",
        "redshift_db", "redshift_schema", "jdbc_url", "awsuser", "aws_password", "aws_iam_role"])

s3_output_path = f"s3://{args['s3_bucket']}/raw/"
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

tables = ["apartments_parquet","apartments_attributes_parquet", "bookings_parquet", "user_viewing_parquet"]
logger.info(f"Tables to process: {tables}")

def load_table_from_catalog(table_name):
    return glueContext.create_dynamic_frame.from_catalog(
        database="rental-marketplace",
        table_name=table_name
        ).toDF()
        
raw_bookings_df = load_table_from_catalog("bookings_parquet")
raw_apartments_df = load_table_from_catalog("apartments_parquet")
raw_apartment_attr = load_table_from_catalog("apartments_attributes_parquet")
raw_user_view = load_table_from_catalog("user_viewing_parquet")

redshift_options = {
    "url":  jdbc_url,
    "user":awsuser,
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
    
    
write_to_redshift(raw_bookings_df, "raw_bookings")
write_to_redshift(raw_apartments_df, "raw_apartments")
write_to_redshift(raw_apartment_attr, "raw_apartment_attr")
write_to_redshift(raw_user_view, "raw_user_viewing")

job.commit()