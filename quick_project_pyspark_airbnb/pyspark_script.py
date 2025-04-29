from pyspark.sql import SparkSession

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("Quick Airbnb Analysis") \
    .getOrCreate()

# 2. Load CSV
df = spark.read.csv("/Users/kshitijpatil/de_on_aws/quick_project_pyspark_airbnb/AB_NYC_2019.csv", header=True, inferSchema=True)

# 3. Drop Nulls
df_clean = df.dropna()

# 4. Group by Neighbourhood Group
top_neighbourhoods = (
    df_clean.groupBy("neighbourhood_group")
    .count()
    .orderBy("count", ascending=False)
)

# 5. Show Top 5
top_neighbourhoods.show(5)

# 6. Stop Spark
spark.stop()
