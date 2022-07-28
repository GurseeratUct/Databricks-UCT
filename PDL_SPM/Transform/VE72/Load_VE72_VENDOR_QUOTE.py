# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'VENDOR_QUOTE'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE72'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

schema =  StructType([ \
                            StructField('ROWID',IntegerType(),True) ,\
                            StructField('VENDOR_ID',StringType(),True) ,\
                            StructField('VENDOR_PART_ID',StringType(),True) ,\
                            StructField('MFG_NAME',StringType(),True) ,\
                            StructField('MFG_PART_ID',StringType(),True) ,\
                            StructField('QTY_BREAK_1',DoubleType(),True) ,\
                            StructField('QTY_BREAK_2',DoubleType(),True) ,\
                            StructField('QTY_BREAK_3',DoubleType(),True) ,\
                            StructField('QTY_BREAK_4',DoubleType(),True) ,\
                            StructField('QTY_BREAK_5',DoubleType(),True) ,\
                            StructField('QTY_BREAK_6',DoubleType(),True) ,\
                            StructField('QTY_BREAK_7',DoubleType(),True) ,\
                            StructField('QTY_BREAK_8',DoubleType(),True) ,\
                            StructField('QTY_BREAK_9',DoubleType(),True) ,\
                            StructField('QTY_BREAK_10',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_1',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_2',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_3',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_4',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_5',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_6',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_7',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_8',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_9',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_10',DoubleType(),True) ,\
                            StructField('DEFAULT_UNIT_PRICE',DoubleType(),True) ,\
                            StructField('PURCHASE_UM',StringType(),True) ,\
                            StructField('QUOTE_DATE',StringType(),True) ,\
                            StructField('LONG_DESCRIPTION',StringType(),True) ,\
                            StructField('CURRENCY_ID',StringType(),True) ,\
                            StructField('UNIT_PRICE_CURR_1',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_2',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_3',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_4',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_5',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_6',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_7',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_8',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_9',DoubleType(),True) ,\
                            StructField('UNIT_PRICE_CURR_10',DoubleType(),True) ,\
                            StructField('DEF_UNIT_PRICE_CURR',DoubleType(),True) ,\
                            StructField('UCT_EXCEPTION',StringType(),True) ,\
                            StructField('LandingFileTimeStamp',StringType(),True) \
                       ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE72'))

# COMMAND ----------

df_transform = df_add_column.na.fill(0).withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-',''))

# COMMAND ----------

df_transform.write.format(write_format).mode("overwrite").save(write_path) 

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------


