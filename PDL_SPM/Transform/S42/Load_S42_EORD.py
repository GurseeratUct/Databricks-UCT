# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'EORD'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

schema = StructType([StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
                     StructField('DI_OPERATION_TYPE',StringType(),True),\
  StructField('MANDT', IntegerType(),True),\
                     StructField('MATNR',StringType(),True),\
                     StructField('WERKS',IntegerType(),True),\
                     StructField('ZEORD',IntegerType(),True),\
                     StructField('ERDAT',StringType(),True),\
                     StructField('ERNAM',StringType(),True),\
                     StructField('VDATU',StringType(),True),\
                     StructField('BDATU',StringType(),True),\
                     StructField('LIFNR',StringType(),True),\
                     StructField('FLIFN',StringType(),True),\
                     StructField('EBELN',StringType(),True),\
                     StructField('EBELP',IntegerType(),True),\
                     StructField('FEBEL',StringType(),True),\
                     StructField('RESWK',StringType(),True),\
                     StructField('FRESW',StringType(),True),\
                     StructField('EMATN',StringType(),True),\
                     StructField('NOTKZ',StringType(),True),\
                     StructField('EKORG',StringType(),True),\
                     StructField('VRTYP',StringType(),True),\
                     StructField('EORTP',StringType(),True),\
                     StructField('AUTET',StringType(),True),\
                     StructField('MEINS',StringType(),True),\
                     StructField('LOGSY',StringType(),True),\
                     StructField('SOBKZ',StringType(),True),\
                     StructField('SRM_CONTRACT_ID',StringType(),True),\
                     StructField('SRM_CONTRACT_ITM',IntegerType(),True),\
                     StructField('DUMMY_EORD_INCL_EEW_PS',StringType(),True),\
                     StructField('LASTCHANGEDATETIME',DoubleType(),True),\
                     StructField('ODQ_CHANGEMODE',StringType(),True),\
                     StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
                     StructField('LandingFileTimeStamp',StringType(),True)])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP'))

# COMMAND ----------

df_transform = df_add_column.withColumn("ERDAT", to_date(regexp_replace(df_add_column.ERDAT,'\.','-'))) \
                            .withColumn("VDATU", to_date(regexp_replace(df_add_column.VDATU,'\.','-'))) \
                            .withColumn("BDATU", to_date(regexp_replace(df_add_column.BDATU,'\.','-'))) \
                            .na.fill(0)
     

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.EORD as S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MATNR,WERKS,ZEORD ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_EORD)A where A.rn = 1 ) as T 
# MAGIC ON S.MATNR = T.MATNR and
# MAGIC S.WERKS = T.WERKS and
# MAGIC S.ZEORD = T.ZEORD 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  S.MANDT = T.MANDT,
# MAGIC S.MATNR = T.MATNR,
# MAGIC S.WERKS = T.WERKS,
# MAGIC S.ZEORD = T.ZEORD,
# MAGIC S.ERDAT = T.ERDAT,
# MAGIC S.ERNAM = T.ERNAM,
# MAGIC S.VDATU = T.VDATU,
# MAGIC S.BDATU = T.BDATU,
# MAGIC S.LIFNR = T.LIFNR,
# MAGIC S.FLIFN = T.FLIFN,
# MAGIC S.EBELN = T.EBELN,
# MAGIC S.EBELP = T.EBELP,
# MAGIC S.FEBEL = T.FEBEL,
# MAGIC S.RESWK = T.RESWK,
# MAGIC S.FRESW = T.FRESW,
# MAGIC S.EMATN = T.EMATN,
# MAGIC S.NOTKZ = T.NOTKZ,
# MAGIC S.EKORG = T.EKORG,
# MAGIC S.VRTYP = T.VRTYP,
# MAGIC S.EORTP = T.EORTP,
# MAGIC S.AUTET = T.AUTET,
# MAGIC S.MEINS = T.MEINS,
# MAGIC S.LOGSY = T.LOGSY,
# MAGIC S.SOBKZ = T.SOBKZ,
# MAGIC S.SRM_CONTRACT_ID = T.SRM_CONTRACT_ID,
# MAGIC S.SRM_CONTRACT_ITM = T.SRM_CONTRACT_ITM,
# MAGIC S.DUMMY_EORD_INCL_EEW_PS = T.DUMMY_EORD_INCL_EEW_PS,
# MAGIC S.LASTCHANGEDATETIME = T.LASTCHANGEDATETIME,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC MANDT,
# MAGIC MATNR,
# MAGIC WERKS,
# MAGIC ZEORD,
# MAGIC ERDAT,
# MAGIC ERNAM,
# MAGIC VDATU,
# MAGIC BDATU,
# MAGIC LIFNR,
# MAGIC FLIFN,
# MAGIC EBELN,
# MAGIC EBELP,
# MAGIC FEBEL,
# MAGIC RESWK,
# MAGIC FRESW,
# MAGIC EMATN,
# MAGIC NOTKZ,
# MAGIC EKORG,
# MAGIC VRTYP,
# MAGIC EORTP,
# MAGIC AUTET,
# MAGIC MEINS,
# MAGIC LOGSY,
# MAGIC SOBKZ,
# MAGIC SRM_CONTRACT_ID,
# MAGIC SRM_CONTRACT_ITM,
# MAGIC DUMMY_EORD_INCL_EEW_PS,
# MAGIC LASTCHANGEDATETIME,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource)
# MAGIC VALUES
# MAGIC (T.MANDT ,
# MAGIC T.MATNR ,
# MAGIC T.WERKS ,
# MAGIC T.ZEORD ,
# MAGIC T.ERDAT ,
# MAGIC T.ERNAM ,
# MAGIC T.VDATU ,
# MAGIC T.BDATU ,
# MAGIC T.LIFNR ,
# MAGIC T.FLIFN ,
# MAGIC T.EBELN ,
# MAGIC T.EBELP ,
# MAGIC T.FEBEL ,
# MAGIC T.RESWK ,
# MAGIC T.FRESW ,
# MAGIC T.EMATN ,
# MAGIC T.NOTKZ ,
# MAGIC T.EKORG ,
# MAGIC T.VRTYP ,
# MAGIC T.EORTP ,
# MAGIC T.AUTET ,
# MAGIC T.MEINS ,
# MAGIC T.LOGSY ,
# MAGIC T.SOBKZ ,
# MAGIC T.SRM_CONTRACT_ID ,
# MAGIC T.SRM_CONTRACT_ITM ,
# MAGIC T.DUMMY_EORD_INCL_EEW_PS ,
# MAGIC T.LASTCHANGEDATETIME ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC 'SAP')

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


