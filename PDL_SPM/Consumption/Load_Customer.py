# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Customer'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Fact/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_kna1 = [  'KUNNR',
                    'LAND1',
                    'NAME1',
                    'ORT01',
                    'PSTLZ',
                    'REGIO',
                    'SORTL',
                    'STRAS',
                    'TELF1',
                    'TELFX',
                    'XCPDK',
                    'ADRNR',
                    'ANRED',
                    'BBBNR',
                    'BBSNR',
                    'BUBKZ',
                    'ERDAT',
                    'ERNAM',
                    'EXABL',
                    'KTOKD',
                    'LIFNR',
                    'SPRAS',
                    'STCEG',
                    'DEAR1',
                    'DEAR2',
                    'DEAR3',
                    'DEAR4',
                    'UMJAH',
                    'JMZAH',
                    'JMJAH',
                    'WERKS',
                    'DUEFL',
                    'HZUOR',
                    'RIC',
                    'LEGALNAT',
                    '/VSO/R_I_NO_LYR',
                    '/VSO/R_ONE_MAT',
                    '/VSO/R_ONE_SORT',
                    '/VSO/R_ULD_SIDE',
                    '/VSO/R_LOAD_PREF',
                    'PSPNR']

# COMMAND ----------

col_names_customer = [
                        'ID',
                        'COUNTRY',
                        'NAME',
                        'CITY',
                        'ZIPCODE' ,
                        'language_id'
                                    ]

# COMMAND ----------

df = spark.sql("select * from S42.kna1 where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'KNA1' and DatabaseName = 'S42')")
df_kna1 = df.select(col_names_kna1)
df_kna1.count()
df_kna1.createOrReplaceTempView("kna1_tmp")
df_ts_kna1 = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_kna1 = df_ts_kna1["max(UpdatedOn)"]
print(ts_kna1)

# COMMAND ----------

plant_code_singapore = spark.sql("select variable_value from Config.config_constant where variable_name = 'plant_code_singapore'")
plant_code_singapore = plant_code_singapore.first()[0]
print(plant_code_singapore)

plant_code_shangai = spark.sql("select variable_value from Config.config_constant where variable_name = 'plant_code_shangai'")
plant_code_shangai = plant_code_shangai.first()[0]
print(plant_code_shangai)

# COMMAND ----------

df = spark.sql("select * from VE70.customer where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'CUSTOMER' and DatabaseName = 'VE70')")
df_ve70_customer = df.select(col_names_customer).withColumn('Plant',lit(plant_code_shangai)).withColumn('DataSource',lit('VE70'))
df_ve70_customer.createOrReplaceTempView("ve70_customer_tmp")
df_ts_ve70_customer = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_customer = df_ts_ve70_customer["max(UpdatedOn)"]
print(ts_ve70_customer)
