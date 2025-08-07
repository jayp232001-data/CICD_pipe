
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, count, row_number, to_date, when, regexp_replace, concat_ws, lit, lower, abs

)
from pyspark.sql.window import Window

# Initialize Glue job (1st)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read from AWS Glue Data Catalog
df = glueContext.create_dynamic_frame.from_catalog(
    database="test_db",
    table_name="lottery_sales",
    transformation_ctx="datasource"
).toDF()


# Step 8: Drop unwanted columns (exact matches)
columns_to_drop = [
    "retailer location address 2",
    "retailer location zip code +4",
    "calendar month",
    "calendar year",
    "calendar month name and number",
    "retailer number and location name",
    "retailer location state",
    "owning entity/chain head number and name"
]
df = df.drop(*columns_to_drop)

# Step 11: Write to S3
output_path = "s3://jay-patil-transformed-bucket/transformed_data/"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Step 12: Commit job
job.commit()
