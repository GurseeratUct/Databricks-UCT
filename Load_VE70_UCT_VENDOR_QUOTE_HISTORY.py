# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'UCT_VENDOR_QUOTE_HISTORY'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE70'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

schema  = StructType([ \
                        StructField('ROWID',IntegerType(),True) ,\
                        StructField('VENDOR_ID',StringType(),True) ,\
                        StructField('VENDOR_PART_ID',StringType(),True) ,\
                        StructField('PART_ID',StringType(),True) ,\
                        StructField('CHANGE_DATE',StringType(),True) ,\
                        StructField('CHANGE_USER_ID',StringType(),True) ,\
                        StructField('MFG_NAME',StringType(),True) ,\
                        StructField('MFG_PART_ID',StringType(),True) ,\
                        StructField('OLD_DEFAULT_UNIT_PRICE',DoubleType(),True) ,\
                        StructField('NEW_DEFAULT_UNIT_PRICE',DoubleType(),True) ,\
                        StructField('DEF_UNIT_PRICE_CURR',DoubleType(),True) ,\
                        StructField('NEW_PURCHASE_UM',StringType(),True) ,\
                        StructField('QUOTE_DATE',StringType(),True) ,\
                        StructField('OLD_PURCHASE_UM',StringType(),True) ,\
                        StructField('OLD_DEF_UNIT_PRICE_CURR',DoubleType(),True) ,\
                        StructField('OLD_MFG_NAME',StringType(),True) ,\
                        StructField('OLD_MFG_PART_ID',StringType(),True) ,\
                        StructField('NEW_UCT_EXCEPTION',StringType(),True) ,\
                        StructField('OLD_UCT_EXCEPTION',StringType(),True) ,\
                        StructField('TYPE',StringType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
                      ])

# COMMAND ----------

def get_csv_files(directory_path):
  csv_files = []
  files_to_treat = dbutils.fs.ls(directory_path)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith('.csv'):
      csv_files.append(path)
  return csv_files

files = get_csv_files(read_path)
read_path_file = files.pop()

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path_file)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE70'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
                            .withColumn("CHANGE_DATE", to_timestamp(regexp_replace(df_add_column.CHANGE_DATE,'\.','-'))) \
                            .withColumn("QUOTE_DATE", to_date(regexp_replace(df_add_column.QUOTE_DATE,'\.','-'))) \
                            .na.fill(0)
                                          
                                        

# COMMAND ----------

df_transform.write.format(write_format).mode("overwrite").save(write_path) 

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)
