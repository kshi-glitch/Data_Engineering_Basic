#%%
import os
import boto3
from pathlib import Path
from boto3.session import Session

# Step 1: Set up variables
local_dir = Path.home() / 'de_on_aws' / 'amazon_glue' / 'advanced_glue'
s3_bucket = "kshitij-de-bucket"
s3_prefix = "landing/github_activity/"
# aws_profile = "itvgithub"
#%%
# Step 2: Create boto3 session with profile
session = Session()
s3_client = session.client("s3")

# Step 3: Loop through files and upload those matching 2021-01-16*
for file in os.listdir(local_dir):
    if file.startswith("2021-01-16") and file.endswith(".json.gz"):
        file_path = os.path.join(local_dir, file)
        s3_key = s3_prefix + file
        
        # print(f"Uploading {file_path} to s3://{s3_bucket}/{s3_key}")
        s3_client.upload_file(str(file_path), s3_bucket, s3_key)
        print(f"Uploaded to s3://{s3_bucket}/{s3_key}")
        
# %%
import boto3

glue = boto3.client('glue', region_name='ap-southeast-2')  # your region

response = glue.update_job(  # use create_job() if the job doesn't exist
    JobName='github_json_to_parquet',
    JobUpdate={
        'Role': 'AWSGlueServiceRole',  # replace with your actual IAM role name
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://kshitij-de-bucket/scripts/github_json_to_parquet.py',  # update this path
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--job-language': 'python',
            '--TempDir': 's3://kshitij-de-bucket/temp/',
            '--enable-continuous-cloudwatch-log': 'true'
        },
        'MaxRetries': 1,
        'Timeout': 10,
        'GlueVersion': '3.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X',
    }
)

print("Job updated:", response)

#%%
import boto3
from boto3.session import Session

# Step 1: Set variables
job_name = "github_json_to_parquet"
region = "ap-southeast-2"

# Step 2: Create session and Glue client
session = Session()
glue_client = session.client("glue", region_name=region)

# Step 3: Start the job run
response = glue_client.start_job_run(
    JobName=job_name,
    WorkerType="G.1X",
    NumberOfWorkers=10
)

# Step 4: Capture and print run ID
run_id = response["JobRunId"]
print(f"Started Glue Job: {job_name} with Run ID: {run_id}")


# %%
import time

while True:
    status_response = glue_client.get_job_run(
        JobName=job_name,
        RunId=run_id
    )
    status = status_response['JobRun']['JobRunState']
    print(f"Job Status: {status}")

    if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
        break
    time.sleep(10)  # check every 10 seconds


# %%
bookmark_response = glue_client.get_job_bookmark(JobName=job_name)
print("Bookmark Info:")
print(bookmark_response['JobBookmarkEntry'])

# %%
