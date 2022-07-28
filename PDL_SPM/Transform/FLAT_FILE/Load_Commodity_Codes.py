# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp,ltrim
from delta.tables import *

# COMMAND ----------

table_name = 'CommodityCode'
#read_format = 'csv'
read_format = 'com.crealytics.spark.excel'
write_format = 'delta'
database_name = 'FLAT_FILE'
#read_path = '/mnt/uct-landing-gen-dev/Flat File/CommodityCode/SAP Commodity Codes From Agile.csv'
read_path = '/mnt/uct-landing-gen-dev/Flat File/CommodityCode/UCT Commodity Code Hierarchy.xlsx'
write_path = '/mnt/uct-transform-gen-dev/Flat_File/Date/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("inferschema", True) \
      .option("delimiter",",") \
      .load(read_path)
      

# COMMAND ----------

df = df.withColumn('CommodityCodeSAP', ltrim(df.CommodityCodeSAP)) 
df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('FLAT_FILE'))

# COMMAND ----------

df_add_column.write.format(write_format).mode("overwrite").save(write_path) 

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------


