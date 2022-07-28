# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp

# COMMAND ----------

table_name = 'UCT_PART_COST_CHANGE'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE70'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.options(header='True', inferSchema='True', delimiter='^').csv(read_path)

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


schema = StructType([ \
StructField('ROWID',StringType(),True),\
StructField('PART_ID',StringType(),True),\
StructField('O_UNIT_MATERIAL_COST',DoubleType(),True),\
StructField('O_UNIT_LABOR_COST',DoubleType(),True),\
StructField('O_UNIT_BURDEN_COST',DoubleType(),True),\
StructField('O_UNIT_SERVICE_COST',DoubleType(),True),\
StructField('N_UNIT_MATERIAL_COST',DoubleType(),True),\
StructField('N_UNIT_LABOR_COST',DoubleType(),True),\
StructField('N_UNIT_BURDEN_COST',DoubleType(),True),\
StructField('N_UNIT_SERVICE_COST',DoubleType(),True),\
StructField('PRODUCT_CODE',StringType(),True),\
StructField('DATE_POSTED',StringType(),True),\
StructField('USER_ID',StringType(),True),\
StructField('QTY_ON_HAND',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True) \
                     ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path_file)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE70'))

# COMMAND ----------

df_transform = df_add_column.na.fill(0).withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
                                       .withColumn("DATE_POSTED", to_timestamp(regexp_replace(df_add_column.DATE_POSTED,'\.','-'))) 

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

df_transform.write.format(write_format).option("overwriteSchema",True).mode("overwrite").save(write_path)

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)
