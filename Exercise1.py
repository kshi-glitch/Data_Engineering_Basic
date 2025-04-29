#%%
import boto3
s3_client = boto3.client('s3')
s3_client.list_buckets()
# %%
buckets = s3_client.list_buckets()
bucket_names = [bucket['Name'] for bucket in buckets['Buckets']]

# %%
import boto3
from botocore.exceptions import ClientError
import os
import subprocess

# Replace this with your actual region
region = "ap-southeast-2"
bucket_name = "kshitij-de-bucket"

# Initialize S3 client
s3 = boto3.client("s3", region_name=region)

# 1. Create the bucket
try:
    s3.create_bucket(Bucket=bucket_name,
                     CreateBucketConfiguration={'LocationConstraint': region})
    print(f"✅ Bucket '{bucket_name}' created.")
except ClientError as e:
    if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        print(f"⚠️ Bucket '{bucket_name}' already exists.")
    else:
        print(f"❌ Error creating bucket: {e}")

# 2. Create folders: landing/ and raw/
folders = ["landing/", "raw/"]
for folder in folders:
    s3.put_object(Bucket=bucket_name, Key=folder)
    print(f"📁 Folder '{folder}' created in bucket '{bucket_name}'.")

# 3. Notes (you don't need to run this but good to know)
"""
- landing/: used for temporary JSON files, deleted after 30 days.
- raw/: used for long-term storage (7-10 years), stored as partitioned Parquet files.
"""

#%%
local_folder = "retail_db"
bucket_name_2 = 'de-retail-db'

# Create the bucket
try:
    s3.create_bucket(Bucket=bucket_name_2,
                     CreateBucketConfiguration={'LocationConstraint': region})
    print(f"✅ Bucket '{bucket_name_2}' created.")
except ClientError as e:
    if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        print(f"⚠️ Bucket '{bucket_name_2}' already exists.")
    else:
        print(f"❌ Error creating bucket: {e}")

# upload the data set
# You can use the following command to upload the data set to the S3 bucket
#%%
# Use boto3 to upload the dataset
s3_path = f"s3://{bucket_name_2}/"
try:
    print(f"🔄 Uploading data from '{local_folder}' to '{s3_path}'...")
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            # Calculate the S3 key based on the local file path
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_key = relative_path.replace('\\', '/')  # Ensure proper S3 path format
            
            # Upload the file
            s3.upload_file(local_file_path, bucket_name_2, s3_key)
            print(f"  Uploaded: {relative_path}")
    
    print(f"✅ Successfully uploaded '{local_folder}' to '{s3_path}'.")
except Exception as e:
    print(f"❌ Error uploading dataset: {e}")




# %%
# Let us get into the details related to s3 Buckets, Folders, and Objects.

# Upload all the 6 folders from the retail_db data set which you might have set up earlier. 

