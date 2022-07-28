# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp

# COMMAND ----------

table_name = 'Date'
read_format = 'csv'
write_format = 'delta'
database_name = 'FLAT_FILE'
read_path = '/mnt/uct-landing-gen-dev/Flat File/Date/Date_1.csv'
write_path = '/mnt/uct-transform-gen-dev/Flat_File/Date/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter","\t") \
      .load(read_path)

# COMMAND ----------

df.write.format(write_format).mode("overwrite").save(write_path) 

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_sf = spark.sql("select * from flat_file.date")

# COMMAND ----------

options = {
  "sfUrl": "ultra_clean_holdings_dataplatform.snowflakecomputing.com",
  "sfUser": "PKUSHWAH",
  "sfPassword": "PpanKAJ_9826",
  "sfDatabase": "UCT_DEVELOPMENT",
  "sfSchema": "LANDING",
  "sfWarehouse": "UCT_DEV",
  "sfRole": "PUBLIC",
  "insecureMode": "true"
}

# COMMAND ----------

df_sf.write \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("dbtable", "Calendar") \
  .mode("OVERWRITE") \
  .save()

# COMMAND ----------


