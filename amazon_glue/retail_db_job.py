#%%
import boto3
import os
import time
import json
import csv
import pandas

# Configuration
LOCAL_DATA_PATH = "/Users/kshitijpatil/de_on_aws/retail_db"
BUCKET_NAME = "kshitij-retail-bucket"  # Must be globally unique
REGION = "ap-southeast-2"  # Change to your preferred region
DATABASE_NAME = "retail_db"
CRAWLER_NAME = "retail_db_crawler"
IAM_ROLE_NAME = "AWSGlueRetailDBRole"
S3_DATA_PATH = f"s3://{BUCKET_NAME}/retail_db/"
ATHENA_RESULTS_PATH = f"s3://{BUCKET_NAME}/athena-results/"
FILE_EXTENSION = ""  # Change to ".json" or ".parquet" if needed

# AWS Clients
s3_client = boto3.client("s3", region_name=REGION)
iam_client = boto3.client("iam")
glue_client = boto3.client("glue", region_name=REGION)
athena_client = boto3.client("athena", region_name=REGION)


# Table column definitions based on PostgreSQL scripts
TABLE_COLUMNS = {
    "departments": ["department_id", "department_name"],
    "categories": ["category_id", "category_department_id", "category_name"],
    "products": ["product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image"],
    "customers": ["customer_id", "customer_fname", "customer_lname", "customer_email", "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode"],
    "orders": ["order_id", "order_date", "order_customer_id", "order_status"],
    "order_items": ["order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price"]
}
#%%
def add_headers_to_files():
    """Add headers to part-00000 files based on table definitions and rename with .csv."""
    for table, columns in TABLE_COLUMNS.items():
        file_path = os.path.join(LOCAL_DATA_PATH, table, "part-00000")
        if os.path.exists(file_path):
            # Read existing data
            with open(file_path, 'r') as infile:
                reader = csv.reader(infile)
                rows = list(reader)
            
            # Create new file path with .csv extension
            new_file_path = file_path + ".csv"
            
            # Check if headers already exist by comparing first row with expected columns
            headers_exist = False
            if rows and len(rows) > 0:
                potential_headers = rows[0]
                # Check if first row matches our column headers (case-insensitive comparison)
                if len(potential_headers) == len(columns) and all(
                    p.lower().strip() == c.lower().strip() for p, c in zip(potential_headers, columns)
                ):
                    headers_exist = True
                    print(f"Headers already exist in {file_path}")
            
            # Add headers and write to new file
            with open(new_file_path, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                
                if not headers_exist:
                    writer.writerow(columns)  # Write headers
                    print(f"Added headers to {new_file_path}")
                else:
                    # Skip writing headers if they already exist
                    print(f"Using existing headers for {new_file_path}")
                
                writer.writerows(rows)  # Write existing data
        else:
            print(f"File {file_path} not found")
            raise Exception(f"Missing data file in {table}")
#%%

def create_s3_bucket():
    """Create S3 bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} already exists")
    except s3_client.exceptions.ClientError:
        s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print(f"Created bucket {BUCKET_NAME}")

#%%
def upload_data_to_s3():
    """Upload data directories to S3."""
    for table in TABLE_COLUMNS.keys():
        local_dir = os.path.join(LOCAL_DATA_PATH, table)
        for file_name in os.listdir(local_dir):
            if file_name.endswith(FILE_EXTENSION):
                local_file = os.path.join(local_dir, file_name)
                s3_key = f"retail_db/{table}/{file_name}"
                s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
                print(f"Uploaded {local_file} to s3://{BUCKET_NAME}/{s3_key}")

#%%
def create_iam_role():
    """Create IAM role for AWS Glue."""
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    try:
        response = iam_client.create_role(
            RoleName=IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        role_arn = response["Role"]["Arn"]
        print(f"Created IAM role {IAM_ROLE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        role_arn = f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/{IAM_ROLE_NAME}"
        print(f"IAM role {IAM_ROLE_NAME} already exists")

    policies = [
        "arn:aws:iam::aws:policy/AWSGlueServiceRole",
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
    ]
    for policy_arn in policies:
        iam_client.attach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn=policy_arn)
        print(f"Attached policy {policy_arn} to {IAM_ROLE_NAME}")

    return role_arn

#%%
def create_glue_database():
    """Create Glue database."""
    try:
        glue_client.create_database(DatabaseInput={"Name": DATABASE_NAME})
        print(f"Created Glue database {DATABASE_NAME}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue database {DATABASE_NAME} already exists")

#%%
def create_glue_crawler(role_arn):
    """Create and run Glue crawler."""
    crawler_config = {
        "Name": CRAWLER_NAME,
        "Role": role_arn,
        "DatabaseName": DATABASE_NAME,
        "Targets": {
            "S3Targets": [{"Path": S3_DATA_PATH}]
        },
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE"
        }
    }
    try:
        glue_client.create_crawler(**crawler_config)
        print(f"Created crawler {CRAWLER_NAME}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Crawler {CRAWLER_NAME} already exists")

    glue_client.start_crawler(Name=CRAWLER_NAME)
    print(f"Started crawler {CRAWLER_NAME}")

    while True:
        response = glue_client.get_crawler(Name=CRAWLER_NAME)
        state = response["Crawler"]["State"]
        if state == "READY":
            print(f"Crawler {CRAWLER_NAME} completed")
            break
        print(f"Crawler {CRAWLER_NAME} is {state}...")
        time.sleep(10)

#%%
def configure_athena():
    """Configure Athena query results location."""
    try:
        athena_client.update_work_group(
            WorkGroup="primary",
            ConfigurationUpdates={
                "ResultConfigurationUpdates": {
                    "OutputLocation": ATHENA_RESULTS_PATH
                }
            }
        )
        print(f"Configured Athena results location to {ATHENA_RESULTS_PATH}")
    except Exception as e:
        print(f"Error configuring Athena: {e}")

#%%

print("Starting setup process...")

add_headers_to_files()

create_s3_bucket()

upload_data_to_s3()

role_arn = "arn:aws:iam::387082969034:role/AWSGlueServiceRole"

create_glue_database()

create_glue_crawler(role_arn)

configure_athena()

print("Setup complete! You can now query the retail_db database in Athena.")

