# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'T001K'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

schema = StructType([ \
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('BWKEY',IntegerType(),True) ,\
                        StructField('BUKRS',IntegerType(),True) ,\
                        StructField('BWMOD',IntegerType(),True) ,\
                        StructField('XBKNG',StringType(),True) ,\
                        StructField('MLBWA',StringType(),True) ,\
                        StructField('MLBWV',StringType(),True) ,\
                        StructField('XVKBW',StringType(),True) ,\
                        StructField('ERKLAERKOM',StringType(),True) ,\
                        StructField('UPROF',StringType(),True) ,\
                        StructField('WBPRO',StringType(),True) ,\
                        StructField('MLAST',IntegerType(),True) ,\
                        StructField('MLASV',StringType(),True) ,\
                        StructField('BDIFP',DoubleType(),True) ,\
                        StructField('XLBPD',StringType(),True) ,\
                        StructField('XEWRX',StringType(),True) ,\
                        StructField('X2FDO',StringType(),True) ,\
                        StructField('PRSFR',StringType(),True) ,\
                        StructField('MLCCS',StringType(),True) ,\
                        StructField('XEFRE',StringType(),True) ,\
                        StructField('EFREJ',IntegerType(),True) ,\
                        StructField('/FMP/PRSFR',StringType(),True) ,\
                        StructField('/FMP/PRFRGR',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
                         ])

# COMMAND ----------

df_T001K = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter","^") \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df_T001K.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(regexp_replace(df_add_column.LandingFileTimeStamp, '_', ''),'-','')) 
                            
                           

# COMMAND ----------

df_transform.createOrReplaceTempView('df_transform')

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO S42.T001K  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,BWKEY ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T001K)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.MANDT = T.MANDT 
# MAGIC and 
# MAGIC S.BWKEY = T.BWKEY
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  S.MANDT = T.MANDT,
# MAGIC S.BWKEY = T.BWKEY,
# MAGIC S.BUKRS = T.BUKRS,
# MAGIC S.BWMOD = T.BWMOD,
# MAGIC S.XBKNG = T.XBKNG,
# MAGIC S.MLBWA = T.MLBWA,
# MAGIC S.MLBWV = T.MLBWV,
# MAGIC S.XVKBW = T.XVKBW,
# MAGIC S.ERKLAERKOM = T.ERKLAERKOM,
# MAGIC S.UPROF = T.UPROF,
# MAGIC S.WBPRO = T.WBPRO,
# MAGIC S.MLAST = T.MLAST,
# MAGIC S.MLASV = T.MLASV,
# MAGIC S.BDIFP = T.BDIFP,
# MAGIC S.XLBPD = T.XLBPD,
# MAGIC S.XEWRX = T.XEWRX,
# MAGIC S.X2FDO = T.X2FDO,
# MAGIC S.PRSFR = T.PRSFR,
# MAGIC S.MLCCS = T.MLCCS,
# MAGIC S.XEFRE = T.XEFRE,
# MAGIC S.EFREJ = T.EFREJ,
# MAGIC S.`/FMP/PRSFR` = T.`/FMP/PRSFR`,
# MAGIC S.`/FMP/PRFRGR` = T.`/FMP/PRFRGR`,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT 
# MAGIC (
# MAGIC MANDT,
# MAGIC BWKEY,
# MAGIC BUKRS,
# MAGIC BWMOD,
# MAGIC XBKNG,
# MAGIC MLBWA,
# MAGIC MLBWV,
# MAGIC XVKBW,
# MAGIC ERKLAERKOM,
# MAGIC UPROF,
# MAGIC WBPRO,
# MAGIC MLAST,
# MAGIC MLASV,
# MAGIC BDIFP,
# MAGIC XLBPD,
# MAGIC XEWRX,
# MAGIC X2FDO,
# MAGIC PRSFR,
# MAGIC MLCCS,
# MAGIC XEFRE,
# MAGIC EFREJ,
# MAGIC `/FMP/PRSFR`,
# MAGIC `/FMP/PRFRGR`,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.MANDT ,
# MAGIC T.BWKEY ,
# MAGIC T.BUKRS ,
# MAGIC T.BWMOD ,
# MAGIC T.XBKNG ,
# MAGIC T.MLBWA ,
# MAGIC T.MLBWV ,
# MAGIC T.XVKBW ,
# MAGIC T.ERKLAERKOM ,
# MAGIC T.UPROF ,
# MAGIC T.WBPRO ,
# MAGIC T.MLAST ,
# MAGIC T.MLASV ,
# MAGIC T.BDIFP ,
# MAGIC T.XLBPD ,
# MAGIC T.XEWRX ,
# MAGIC T.X2FDO ,
# MAGIC T.PRSFR ,
# MAGIC T.MLCCS ,
# MAGIC T.XEFRE ,
# MAGIC T.EFREJ ,
# MAGIC T.`/FMP/PRSFR` ,
# MAGIC T.`/FMP/PRFRGR` ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now() ,
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------



# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


