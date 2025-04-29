#%%
import time
import boto3

# ─── Config ────────────────────────────────────────────────────────────
AWS_REGION        = "ap-southeast-2"                                   # e.g. ap-southeast-2
ACCOUNT_ID        = "387082969034"                                    # Your 12-digit account ID
CLUSTER_ID        = "j-1T3VS7SLHM8AA"                                 # Your existing EMR cluster ID
NOTEBOOK_NAME     = "kshitij-activity-notebook"                        # Friendly name for your notebook
STUDIO_ROLE_ARN   = f"arn:aws:iam::387082969034:role/service-role/AmazonEMRStudio_ServiceRole_1745501988667"
NOTEBOOK_ROLE_ARN = f"arn:aws:iam::387082969034:role/EMR_EC2_DefaultRole"
EMR_CLIENT        = boto3.client("emr", region_name=AWS_REGION)
EMR_NOTEBOOKS     = boto3.client("emr-notebooks", region_name=AWS_REGION)

# ───────────────────────────────────────────────────────────────────────
#%%
def create_emr_notebook():
    print(f"🚀 Creating EMR Notebook: {NOTEBOOK_NAME}")
    response = EMR_NOTEBOOKS.create_notebook(
        EditorName=NOTEBOOK_NAME,
        WorkspaceId=WORKSPACE_ID,
        RoleArn=NOTEBOOK_ROLE_ARN,
        Arn=STUDIO_ROLE_ARN    # Service role
    )
    notebook_arn = response["Notebook"]["NotebookArn"]
    print(f"➡️ Notebook ARN: {notebook_arn}")
    return notebook_arn

def wait_for_ready(notebook_arn):
    print("⏳ Waiting for notebook to become READY...")
    while True:
        resp = EMR_NOTEBOOKS.describe_notebook(NotebookArn=notebook_arn)
        status = resp["Notebook"]["Status"]
        print("   Status:", status)
        if status in ("STARTING", "UP", "DOWN", "FAILED"):
            break
        time.sleep(10)
    return status

def main():
    arn = create_emr_notebook()
    final_status = wait_for_ready(arn)
    if final_status == "UP":
        url = EMR_NOTEBOOKS.describe_notebook(NotebookArn=arn)["Notebook"]["Url"]
        print(f"✅ Notebook is UP! Access it here:\n{url}")
    else:
        print(f"❌ Notebook ended in status {final_status}. Please check CloudWatch for errors.")
#%%
if __name__ == "__main__":
    main()

# %%
