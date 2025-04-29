#%%
import boto3
import json
import time

region = "us-east-1"
account_id = "kshitease"
role_name = "ITVFlightsGlueRole"
policy_name = "AmazonS3FullAccess"
glue_db = "flights-db"
bucket = "kshitij-flights"
script_bucket = "itv-flights-glue-scripts"
csv_key = "flightscsv/amazon_flights.csv"
script_key = "csv_to_parquet.py"
job_name = "flights_csv_to_parquet"
crawler_csv = "flightscsv-crawler"
crawler_parquet = "flightsparquet-crawler"

session = boto3.session.Session(region_name=region)
s3 = session.client("s3")
iam = session.client("iam")
glue = session.client("glue")
#%%
# 1. Create S3 buckets
s3.create_bucket(Bucket=bucket)
s3.create_bucket(Bucket=script_bucket)

# Upload sample CSV
csv_data = session.client("s3").put_object(
    Bucket=bucket,
    Key="flightscsv/"
)

# Download + upload CSV
import requests
csv_file = requests.get("https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat").content
s3.put_object(Bucket=bucket, Key=csv_key, Body=csv_file)

#%%
# 2. Create IAM Policy
policy_doc = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": f"arn:aws:s3:::{bucket}"},
        {"Effect": "Allow", "Action": ["s3:*Object"], "Resource": f"arn:aws:s3:::{bucket}/*"}
    ]
}
try:
    iam.create_policy(PolicyName=policy_name, PolicyDocument=json.dumps(policy_doc))
except iam.exceptions.EntityAlreadyExistsException:
    pass
#%%
# 3. Create IAM Role
assume_role = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}
try:
    iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(assume_role))
    print(f"Created role: {role_name}")
except iam.exceptions.EntityAlreadyExistsException:
    pass

# Attach policies
iam.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
iam.attach_role_policy(
    RoleName=role_name,
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
)
#%%
# 4. Create Glue Database
try:
    glue.create_database(DatabaseInput={"Name": glue_db})
except glue.exceptions.AlreadyExistsException:
    pass
#%%
# 5. Create CSV Crawler
glue.create_crawler(
    Name=crawler_csv,
    Role=role_name,
    DatabaseName=glue_db,
    Targets={"S3Targets": [{"Path": f"s3://{bucket}/flightscsv/"}]}
)
#%%
# 6. Upload Glue Job Script
script_code = """
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource = glueContext.create_dynamic_frame.from_catalog(
    database="flights-db",
    table_name="flightscsv"
)

glueContext.write_dynamic_frame.from_options(
    frame=datasource,
    connection_type="s3",
    connection_options={"path": "s3://kshitij-flights/flightsparquet/"},
    format="parquet"
)
job.commit()
"""

s3.put_object(Bucket=script_bucket, Key=script_key, Body=script_code)
#%%
# 7. Create Glue Job
try:
    glue.create_job(
        Name=job_name,
        Role=role_name,
        Command={
            "Name": "glueetl",
            "ScriptLocation": f"s3://{script_bucket}/{script_key}",
            "PythonVersion": "3"
        },
        DefaultArguments={"--job-language": "python", "--enable-continuous-cloudwatch-log": "true"},
        GlueVersion="3.0"
    )
except glue.exceptions.AlreadyExistsException:
    pass
#%%
# 8. Create Parquet Crawler
glue.create_crawler(
    Name=crawler_parquet,
    Role=role_name,
    DatabaseName=glue_db,
    Targets={"S3Targets": [{"Path": f"s3://{bucket}/flightsparquet/"}]}
)
#%%
# 9. Run CSV Crawler
glue.start_crawler(Name=crawler_csv)
print("Running CSV crawler...")
while glue.get_crawler(Name=crawler_csv)["Crawler"]["State"] == "RUNNING":
    time.sleep(5)
#%%
# 10. Run Glue Job
job_run = glue.start_job_run(JobName=job_name)
print("Running Glue job...")
status = "RUNNING"
while status in ["RUNNING", "STARTING", "STOPPING"]:
    run = glue.get_job_run(JobName=job_name, RunId=job_run["JobRunId"])
    status = run["JobRun"]["JobRunState"]
    time.sleep(10)
print(f"Glue job finished with status: {status}")

if status == "FAILED":
    error_message = run["JobRun"].get("ErrorMessage", "No error message provided.")
    print(f"Glue job failed. Reason: {error_message}")
#%%
# 11. Run Parquet Crawler
glue.start_crawler(Name=crawler_parquet)
print("Running Parquet crawler...")
while glue.get_crawler(Name=crawler_parquet)["Crawler"]["State"] == "RUNNING":
    time.sleep(5)

print("✅ All done! You can now query from Athena.")

# %%
