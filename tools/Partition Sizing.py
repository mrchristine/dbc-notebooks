# Databricks notebook source exported at Tue, 31 May 2016 18:26:16 UTC
# MAGIC %md
# MAGIC ### Managing Partition Sizes
# MAGIC Below is an example reading parquet files, looking at their sizes, then compacting them into larger files. 

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh /dbfs/mnt/mwc/tpch_orders

# COMMAND ----------

# MAGIC %md
# MAGIC Read the parquet files, write 1 parquet file per table level partition. 

# COMMAND ----------

df = sqlContext.read.parquet("/mnt/mwc/tpch_orders")
df.repartition(1).write.partitionBy('OrderPriority').parquet("/mnt/mwc/tpch_orders_partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC List the file sizes and directories. 

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh /dbfs/mnt/mwc/tpch_orders_partitioned

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh /dbfs/mnt/mwc/tpch_orders_partitioned/OrderPriority=1-URGENT

# COMMAND ----------

# MAGIC %md
# MAGIC Compact a table that is not partitioned by a column value. 

# COMMAND ----------

df.repartition(2).write.parquet("/mnt/mwc/tpch_orders_compact")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh /dbfs/mnt/mwc/tpch_orders_compact

# COMMAND ----------

def get_parquet_sizes_df(path):
  # Convert path location to DataFrame
  df_size = sc.parallelize(dbutils.fs.ls(path)).toDF(["path", "name", "size"])
  # Parse the parquet files
  df_parquet = df_size.filter("name like \"%parquet%\"")
  # Convert sizes in MBs and return the DataFrame
  df_mb = df_parquet.withColumn('size_mb', df_parquet.size / 1000000)
  return df_mb

# COMMAND ----------

# MAGIC %md
# MAGIC Parse the directory and pull the data size in MBs. View as a graph to see the average size of the files. This example shows that the files are ~18MBs in size, and we should compact them better. 

# COMMAND ----------

df_mb = get_parquet_sizes_df("/mnt/mwc/tpch_orders")
display(df_mb)

# COMMAND ----------

df_mb = get_parquet_sizes_df("/mnt/mwc/tpch_orders_compact")
display(df_mb)