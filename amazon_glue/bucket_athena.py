#%%
import boto3
import json
import time
from datetime import datetime

def create_athena_results_bucket():
    # Define region
    region = 'us-east-1'
    
    # Initialize clients with explicit region
    s3 = boto3.client('s3', region_name=region)
    athena = boto3.client('athena', region_name=region)
    
    # Generate unique bucket name
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    bucket_name = f"athena-results-{timestamp}"  # Added timestamp for uniqueness

    try:
        # 1. Create S3 bucket
        if region == 'us-east-1':
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        
        print(f"Created bucket: {bucket_name}")
        
        # 2. Create folder structure
        s3.put_object(Bucket=bucket_name, Key='athena-query-results/')
        s3.put_object(Bucket=bucket_name, Key='athena-query-logs/')
        
        # 3. Set bucket policy for Athena
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "athena.amazonaws.com"},
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                }
            ]
        }
        
        s3.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(bucket_policy)
        )
        
        # 4. Set lifecycle rules (auto-delete after 7 days)
        lifecycle_config = {
            "Rules": [
                {
                    "ID": "ExpireQueryResults",
                    "Status": "Enabled",
                    "Prefix": "athena-query-results/",
                    "Expiration": {"Days": 7}
                }
            ]
        }
        
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        
        # 5. Create Athena workgroup
        workgroup_name = "primary-workgroup"
        try:
            athena.create_work_group(
                Name=workgroup_name,
                Configuration={
                    'ResultConfiguration': {
                        'OutputLocation': f's3://{bucket_name}/athena-query-results/'
                    },
                    'EnforceWorkGroupConfiguration': True,
                    'PublishCloudWatchMetricsEnabled': True
                },
                Description='Primary workgroup for Athena queries'
            )
            print(f"Created Athena workgroup: {workgroup_name}")
        except athena.exceptions.InvalidRequestException as e:
            if "WorkGroup already exists" in str(e):
                print(f"Workgroup {workgroup_name} already exists. Updating configuration...")
                athena.update_work_group(
                    WorkGroup=workgroup_name,
                    Configuration={
                        'ResultConfiguration': {
                            'OutputLocation': f's3://{bucket_name}/athena-query-results/'
                        },
                        'EnforceWorkGroupConfiguration': True,
                        'PublishCloudWatchMetricsEnabled': True
                    },
                    Description='Primary workgroup for Athena queries'
                )
            else:
                raise
        
        return {
            'status': 'success',
            'bucket_name': bucket_name,
            'workgroup_name': workgroup_name,
            'query_location': f's3://{bucket_name}/athena-query-results/'
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }
#%%
# Example usage
if __name__ == "__main__":
    result = create_athena_results_bucket()
    print("\nSetup Results:")
    print(json.dumps(result, indent=2))
# %%
