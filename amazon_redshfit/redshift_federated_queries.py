#%%
import pandas as pd
import boto3
import json
import os
import os
import pandas as pd
import sqlalchemy
from sqlalchemy.exc import IntegrityError
#%%

secret_name = "retail_db_postgres_creds"

def get_secrets(secret_name):
    sm_client = boto3.client('secretsmanager')

    secret_value = sm_client.get_secret_value(SecretId = secret_name)

    secret_string = json.loads(secret_value['SecretString'])

    return secret_string


def json_to_df(path,table_name):
    file_name = os.listdir(f'{path}/{table_name}')[0]
    file_path = f'{path}/{table_name}/{file_name}'

    dataframe = pd.read_json(file_path,lines=True)
    return dataframe


#%%
secret_string = get_secrets(secret_name)
username = secret_string['username']
password =  secret_string['password']
host = secret_string['host']
port = secret_string['port']
engine = secret_string['engine']
dbname = secret_string['dbname']


path = '/Users/kshitijmac/de_on_aws/retail_db_json'
table_name = 'categories'
df_categories = json_to_df(path,table_name)
#%%

conn = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
df_categories.to_sql('categories',conn, if_exists= 'append', index=False)
# %%

#populate every table


# Base path for JSON files
path = '/Users/kshitijmac/de_on_aws/retail_db_json'

# List of table names based on directories in the path
tables = [
    'categories',
    'customers',
    'departments',
    'order_items',
    'orders',
    'products'
]

def json_to_df(path, table_name):
    """Read JSON file into a pandas DataFrame."""
    file_name = os.listdir(f'{path}/{table_name}')[0]
    file_path = f'{path}/{table_name}/{file_name}'
    dataframe = pd.read_json(file_path, lines=True)
    return dataframe

def populate_table(df, table_name, conn_string):
    """Populate table with DataFrame data, handling existing data."""
    try:
        engine = sqlalchemy.create_engine(conn_string)
        # Append data, skip duplicates based on primary key constraints
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Successfully populated {table_name} with {len(df)} records.")
    except IntegrityError as e:
        print(f"IntegrityError for {table_name}: {e}. Possible duplicate entries skipped.")
    except Exception as e:
        print(f"Error populating {table_name}: {e}")
    finally:
        engine.dispose()

def main():
    # Database connection string
    conn_string = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    
    # Iterate through each table
    for table_name in tables:
        print(f"Processing {table_name}...")
        try:
            # Read JSON data into DataFrame
            df = json_to_df(path, table_name)
            # Populate table
            populate_table(df, table_name, conn_string)
        except Exception as e:
            print(f"Failed to process {table_name}: {e}")

if __name__ == "__main__":
    main()
# %%
