#%%
import boto3
import os


os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
client = boto3.client("s3")
glue_client = boto3.client("glue")
#%%
crawler_names = glue_client.get_crawlers()["Crawlers"]
print(crawler_names)
# %%
