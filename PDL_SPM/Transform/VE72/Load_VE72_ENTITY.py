# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp

# COMMAND ----------

table_name = 'ENTITY'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE72'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

schema = StructType([ \
StructField('ROWID',IntegerType(),True),\
StructField('ID',StringType(),True),\
StructField('NAME',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True) ])

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

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE72'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-',''))

# COMMAND ----------

df_transform.write.format(write_format).mode("overwrite").save(write_path) 

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)
