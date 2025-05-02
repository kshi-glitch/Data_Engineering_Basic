#%%
import boto3
import os

athena_client  = boto3.client('athena')
print("Client Created")
athena_client.list_work_groups()

#%%
CatalogName = 'AWSDataCatalog'
databases = athena_client.list_databases(CatalogName = CatalogName)
type(databases)


list_databases = databases['DatabaseList']
#%%
name_db = []
for database in list_databases:
    x = database['Name']
    name_db.append(x)

# %%
name_db = [database['Name'] for database in list_databases]

# %%
database_name = name_db[3]
tables_metadata = athena_client.list_table_metadata(CatalogName = CatalogName, DatabaseName = database_name )


table_name = [tables['Name'] for tables in tables_metadata['TableMetadataList']]

 
# %%
category_table = table_name[0]
category_metadata = athena_client.get_table_metadata(CatalogName = CatalogName, DatabaseName = database_name, TableName = category_table)
# %%
import pandas as pd

def metadata_to_dataframe(metadata):
    columns = metadata['TableMetadata']['Columns']
    
    df = pd.DataFrame(columns)
    
    df.attrs['table_name'] = metadata['TableMetadata']['Name']
    df.attrs['database_name'] = metadata['TableMetadata'].get('DatabaseName', '')
    df.attrs['created_time'] = metadata['TableMetadata'].get('CreateTime', '')
    df.attrs['table_type'] = metadata['TableMetadata'].get('TableType', '')
    
    return df

category_metadata = athena_client.get_table_metadata(
    CatalogName=CatalogName, 
    DatabaseName=database_name, 
    TableName=category_table
)

category_df = metadata_to_dataframe(category_metadata)

# %%
import time
s3_output = 's3://kshitij-de-bucket/boto3queryresults/'  
database = 'retail_db' 
query = 'SELECT count(*) FROM retail_db.orders'

response = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': database},
    ResultConfiguration={'OutputLocation': s3_output}
)

query_execution_id = response['QueryExecutionId']

while True:
    result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    status = result['QueryExecution']['Status']['State']
    if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        break
    time.sleep(2)


if status == 'SUCCEEDED':
    result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    rows = result_response['ResultSet']['Rows']
    result_value = rows[1]['Data'][0]['VarCharValue']  
    print("Count:", result_value)
else:
    print("Query failed or was cancelled. Status:", status)

# %%
#now if the query results are in data and i want to convert them to dataframe
 
import pandas as pd

query_dataframe = f'SELECT * FROM retail_db.categories LIMIT 10'

response_1 = athena_client.start_query_execution(
    QueryString=query_dataframe,
    QueryExecutionContext={'Database': database},
    ResultConfiguration={'OutputLocation': s3_output}
)

query_execution_id_1 = response_1['QueryExecutionId']

def wait_for_query(query_id):
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_id)
        status = result['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status
        time.sleep(1)

status = wait_for_query(query_execution_id_1)

if status == 'SUCCEEDED':
    result = athena_client.get_query_results(QueryExecutionId=query_execution_id_1)
    
    # Extract column names
    columns = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    
    # Skip header row (it's included as first row)
    rows = result['ResultSet']['Rows'][1:]
    
    # Parse rows
    data = []
    for row in rows:
        data.append([field.get('VarCharValue', '') for field in row['Data']])

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=columns)
    print(df)
else:
    print(f"Query failed with status: {status}")

# %%
