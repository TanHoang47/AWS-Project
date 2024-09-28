import sys
import boto3
import os
import shutil
import urllib.parse
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_replace, when, avg, first

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'object_key'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['bucket_name']
object_key = urllib.parse.unquote_plus(args['object_key'])

# Read CSV file directly from S3 into a DataFrame
zomato_df = spark.read.csv(f"s3://{bucket_name}/{object_key}", header=True, inferSchema=True)

# Drop Unnecessary Columns
zomato_df = zomato_df.drop('url', 'phone', 'address', 'reviews_list', 'menu_item')

# Formatting: Remove commas and convert to float
zomato_df = zomato_df.withColumn('approx_cost(for two people)', 
                                 regexp_replace(col('approx_cost(for two people)'), ',', '').cast('float'))

# Convert 'rate' column to float
zomato_df = zomato_df.withColumn('rate', 
                                 when(col('rate').contains('/5'), 
                                      col('rate').substr(1, 3).cast('float')).otherwise(None))

# Replace specified values with null
zomato_df = zomato_df.replace(['', 'None', 'NA', 'NaN', '-999'], None)

# Convert 'online_order' and 'book_table' to binary format
zomato_df = zomato_df.withColumn('online_order', when(col('online_order') == 'Yes', 1).otherwise(0))
zomato_df = zomato_df.withColumn('book_table', when(col('book_table') == 'Yes', 1).otherwise(0))

# Handling Duplicate Rows
zomato_df = zomato_df.dropDuplicates()

# Handling Missing Data by Dropping Unnecessary Column
zomato_df = zomato_df.drop('dish_liked')

# Calculate mean and mode values for missing data handling
approx_cost_mean = zomato_df.select(avg('approx_cost(for two people)')).first()[0]
rest_type_mode = zomato_df.groupBy('rest_type').count().orderBy('count', ascending=False).first()[0]
cuisines_mode = zomato_df.groupBy('cuisines').count().orderBy('count', ascending=False).first()[0]
location_mode = zomato_df.groupBy('location').count().orderBy('count', ascending=False).first()[0]

# Fill missing values with calculated values
zomato_df = zomato_df.na.fill({'approx_cost(for two people)': approx_cost_mean,
                               'rest_type': rest_type_mode,
                               'cuisines': cuisines_mode,
                               'location': location_mode})

# Convert DataFrame back to DynamicFrame for writing
zomato_dyf = DynamicFrame.fromDF(zomato_df, glueContext, "zomato_dyf")

output_path = 's3://hoanglht2-trusted-bucket/'

# Write the transformed DynamicFrame back to S3
glueContext.write_dynamic_frame.from_options(
    frame=zomato_dyf,
    connection_type="s3",
    connection_options={"path": output_path},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)

# Commit the Glue job
job.commit()
