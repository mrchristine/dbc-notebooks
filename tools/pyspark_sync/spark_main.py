
# MAGIC %run "./import_helper"

# COMMAND ----------
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.json("/mnt/mwc/companies")
print("Count the number of rows: ")
print(df.count())

df_complete = spark.read.parquet("/mnt/mwc/reddit_year_p/")
print("\nCount number of rows in reddit comment dataset: ")
print(df_complete.count())

helper_one()
helper_two()
