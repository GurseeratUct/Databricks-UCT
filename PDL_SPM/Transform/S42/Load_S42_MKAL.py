# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'MKAL'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
#read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
#print(read_path)
read_path = '/mnt/uct-landing-gen-dev/SAP/S42/PLPO/PLPO_07202022-103726.csv'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

df_MKAL = spark.read.format(read_format) \
          .option("header", True) \
          .option("delimiter","^") \
          .option("inferSchema",True)  \
          .load(read_path)

# COMMAND ----------

df_MKAL.count()

# COMMAND ----------


