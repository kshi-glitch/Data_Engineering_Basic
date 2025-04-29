import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Bookmark-aware read
input_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://kshitij-de-bucket/json/github/"],
        "recurse": True
    },
    format="json"
)

# Write to raw zone (for example)
output_path = "s3://kshitij-de-bucket/parquet/github/"
glueContext.write_dynamic_frame.from_options(
    frame=input_data,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

job.commit()
