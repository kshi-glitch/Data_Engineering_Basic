#%%
import redshift_connector

# Connect to Redshift
conn = redshift_connector.connect(
    host='default-workgroup.387082969034.ap-southeast-2.redshift-serverless.amazonaws.com:5439/dev',
    database='retail_db',
    user='retail_user',
    password='********',
    port=5439
)

# from sqlalchemy import create_engine

# # Format: redshift+redshift_connector://username:password@host:port/database
# engine = create_engine(
#     "redshift+redshift_connector://my_user:my_password@my-cluster.my-region.redshift.amazonaws.com:5439/dev"
# )

# # Now you can connect and run queries
# with engine.connect() as connection:
#     result = connection.execute("SELECT * FROM your_table LIMIT 5;")
#     for row in result:
#         print(row)
 
#%%
# Create cursor and execute query
cursor = conn.cursor()
cursor.execute("SELECT * FROM your_table_name LIMIT 10;")

# Fetch results
results = cursor.fetchall()
for row in results:
    print(row)

# Close connection
cursor.close()
conn.close()
