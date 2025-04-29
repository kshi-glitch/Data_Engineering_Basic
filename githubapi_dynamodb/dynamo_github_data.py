#%%
import requests
import json
github_token = "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"
github_url = "https://api.github.com/repositories?since=974636246"

res = requests.get(github_url, headers={"Authorization": f"Bearer {github_token}"})
#%%
print(res.status_code)
print(res.content)
print(res.content.decode('utf-8'))
#%%
github_content = res.content.decode('utf-8')
github_results = json.loads(github_content)
# %%
len(github_results)
first_repo = github_results[0]
print("First repository details:")
print(f"ID: {first_repo['id']}")
print(f"Name: {first_repo['name']}")
print(f"Owner Login: {first_repo['owner']['login']}")



# %%
# Get the ID of the last repository in the list
last_repo_id = github_results[-1]['id']
print(f"Last repository ID: {last_repo_id}")

# Fetch the next 100 repositories using the 'since' parameter
next_url = f"https://api.github.com/repositories?since={last_repo_id}"
next_response = requests.get(next_url)

if next_response.status_code == 200:
    next_repos = next_response.json()
    print(f"Number of next repositories fetched: {len(next_repos)}")
else:
    print(f"Failed to fetch next repositories: {next_response.status_code}")

# %%
# Select the first repository
repo = github_results[0]
owner = repo['owner']['login']
repo_name = repo['name']

# Construct the API URL for the specific repository
repo_url = f"https://api.github.com/repos/{owner}/{repo_name}"

# Make the GET request
repo_response = requests.get(repo_url)

# Check if the request was successful
if repo_response.status_code == 200:
    repo_details = repo_response.json()
    print("Repository details:")
    print(f"Full Name: {repo_details['full_name']}")
    print(f"Description: {repo_details['description']}")
    print(f"Created At: {repo_details['created_at']}")
    print(f"Updated At: {repo_details['updated_at']}")
    print(f"Pushed At: {repo_details['pushed_at']}")
else:
    print(f"Failed to fetch repository details: {repo_response.status_code}")
# %%
def list_repos(token=None, since=None):
    url = "https://api.github.com/repositories"
    if since:
        url += f"?since={since}"
    
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to list repositories: {response.status_code}")
        return []

def get_repo_details(owner, repo_name, token=None):

    url = f"https://api.github.com/repos/{owner}/{repo_name}"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch details for {owner}/{repo_name}: {response.status_code}")
        return None

def extract_repo_fields(repo_details):
    try:
        return {
            "id": repo_details["id"],
            "node_id": repo_details["node_id"],
            "name": repo_details["name"],
            "full_name": repo_details["full_name"],
            "html_url": repo_details["html_url"],
            "description": repo_details.get("description", ""),
            "fork": repo_details["fork"],
            "created_at": repo_details["created_at"],
            "owner": {
                "login": repo_details["owner"]["login"],
                "id": repo_details["owner"]["id"],
                "node_id": repo_details["owner"]["node_id"],
                "type": repo_details["owner"]["type"],
                "site_admin": repo_details["owner"]["site_admin"]
            }
        }
    except (KeyError, TypeError) as e:
        print(f"Error extracting fields: {e}")
        return None

def get_repos(repos, token=None):
    repos_details = []
    for repo in repos:
        try:
            owner = repo["owner"]["login"]
            repo_name = repo["name"]
            repo_details = get_repo_details(owner, repo_name, token)
            if repo_details:
                extracted_fields = extract_repo_fields(repo_details)
                if extracted_fields:
                    repos_details.append(extracted_fields)
        except (KeyError, TypeError) as e:
            print(f"Error processing repository {repo.get('name', 'unknown')}: {e}")
            continue
    return repos_details
# %%
# Fetch public repositories
since = 974636246
repos = list_repos(github_token, since)
print(f"Fetched {len(repos)} repositories")

# Process repositories and extract required fields
repos_details = get_repos(repos, github_token)
print(f"Processed {len(repos_details)} repositories")

# Validate the first and last repository details
if repos_details:
    print("First repository details:")
    print(json.dumps(repos_details[0], indent=2))
    print("Last repository ID:")
    print(repos_details[-1]["id"])
# %%
import boto3

dynamodb = boto3.resource('dynamodb',user = , region_name='ap-southeast-2')


table_name = 'github_repos'

# Create the table if it doesn't exist
try:
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'N'  # Number
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    print("Table created successfully.")

except dynamodb.meta.client.exceptions.ResourceInUseException:
    print("Table already exists. Proceeding with data insertion.")
    table = dynamodb.Table(table_name)
except Exception as e:  
    print(f"Error creating table: {e}")
    raise

#%%
marker_table = dynamodb.Table('github_repos_marker')
# Create the marker table if it doesn't exist
try:
    marker_table = dynamodb.create_table(
        TableName='github_repos_marker',
        KeySchema=[
            {
                'AttributeName': 'tn',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'tn',
                'AttributeType': 'N'  # Number
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    print("Marker table created successfully.")
except dynamodb.meta.client.exceptions.ResourceInUseException:
    print("Marker table already exists. Proceeding with data insertion.")
    marker_table = dynamodb.Table('github_repos_marker')
except Exception as e:
    print(f"Error creating marker table: {e}")
    raise

# %%

def load_repos(repos_details, ghrepos_table, batch_size=50):
    with ghrepos_table.batch_writer() as batch:
        repos_count = len(repos_details)
        for i in range(0, repos_count, batch_size):
            print(f'Processing from {i} to {i + batch_size}')
            for repo in repos_details[i:i + batch_size]:
                try:
                    batch.put_item(Item=repo)
                except Exception as e:
                    print(f"Error inserting repo {repo.get('id')}: {e}")
# %%
load_repos(repos_details, table)

# %%
