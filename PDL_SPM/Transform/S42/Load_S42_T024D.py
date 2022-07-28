# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'T024D'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
#read_path = '/mnt/uct-landing-gen-dev/SAP/'+table_name+'/'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

schema  = StructType([ \
                        StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('WERKS',IntegerType(),True) ,\
                        StructField('DISPO',StringType(),True) ,\
                        StructField('DSNAM',StringType(),True) ,\
                        StructField('DSTEL',StringType(),True) ,\
                        StructField('EKGRP',StringType(),True) ,\
                        StructField('MEMPF',StringType(),True) ,\
                        StructField('GSBER',StringType(),True) ,\
                        StructField('PRCTR',StringType(),True) ,\
                        StructField('USRTYP',StringType(),True) ,\
                        StructField('USRKEY',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
                     ])

# COMMAND ----------

df_T024D = spark.read.format(read_format) \
          .option("header", True) \
          .option("delimiter","^") \
          .schema(schema)  \
          .load(read_path)

# COMMAND ----------

df_add_column = df_T024D.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(regexp_replace(df_add_column.LandingFileTimeStamp, '_', ''),'-','')) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO S42.T024D  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,WERKS,DISPO ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T024D)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.MANDT = T.MANDT 
# MAGIC and 
# MAGIC S.WERKS = T.WERKS
# MAGIC and
# MAGIC S.DISPO = T.DISPO
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC S.DI_SEQUENCE_NUMBER = T.DI_SEQUENCE_NUMBER,
# MAGIC S.DI_OPERATION_TYPE = T.DI_OPERATION_TYPE,
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.WERKS = T.WERKS,
# MAGIC S.DISPO = T.DISPO,
# MAGIC S.DSNAM = T.DSNAM,
# MAGIC S.DSTEL = T.DSTEL,
# MAGIC S.EKGRP = T.EKGRP,
# MAGIC S.MEMPF = T.MEMPF,
# MAGIC S.GSBER = T.GSBER,
# MAGIC S.PRCTR = T.PRCTR,
# MAGIC S.USRTYP = T.USRTYP,
# MAGIC S.USRKEY = T.USRKEY,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT
# MAGIC (
# MAGIC DI_SEQUENCE_NUMBER ,
# MAGIC DI_OPERATION_TYPE ,
# MAGIC MANDT ,
# MAGIC WERKS ,
# MAGIC DISPO ,
# MAGIC DSNAM ,
# MAGIC DSTEL ,
# MAGIC EKGRP ,
# MAGIC MEMPF ,
# MAGIC GSBER ,
# MAGIC PRCTR ,
# MAGIC USRTYP ,
# MAGIC USRKEY ,
# MAGIC ODQ_CHANGEMODE ,
# MAGIC ODQ_ENTITYCNTR ,
# MAGIC LandingFileTimeStamp ,
# MAGIC UpdatedOn ,
# MAGIC DataSource 
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.DI_SEQUENCE_NUMBER ,
# MAGIC T.DI_OPERATION_TYPE ,
# MAGIC T.MANDT ,
# MAGIC T.WERKS ,
# MAGIC T.DISPO ,
# MAGIC T.DSNAM ,
# MAGIC T.DSTEL ,
# MAGIC T.EKGRP ,
# MAGIC T.MEMPF ,
# MAGIC T.GSBER ,
# MAGIC T.PRCTR ,
# MAGIC T.USRTYP ,
# MAGIC T.USRKEY ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


