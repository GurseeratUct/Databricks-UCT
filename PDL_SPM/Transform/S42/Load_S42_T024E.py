# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'T024E'
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

schema =  StructType([\
                        StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('EKORG',StringType(),True) ,\
                        StructField('EKOTX',StringType(),True) ,\
                        StructField('BUKRS',StringType(),True) ,\
                        StructField('TXADR',StringType(),True) ,\
                        StructField('TXKOP',StringType(),True) ,\
                        StructField('TXFUS',StringType(),True) ,\
                        StructField('TXGRU',StringType(),True) ,\
                        StructField('KALSE',StringType(),True) ,\
                        StructField('MKALS',StringType(),True) ,\
                        StructField('BPEFF',StringType(),True) ,\
                        StructField('BUKRS_NTR',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
                        ])

# COMMAND ----------

df_T024E = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter","^") \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_T024E.count()

# COMMAND ----------

df_add_column = df_T024E.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

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
# MAGIC MERGE INTO S42.T024E  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,EKORG ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T024E)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.MANDT = T.MANDT 
# MAGIC and 
# MAGIC S.EKORG = T.EKORG
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  S.DI_SEQUENCE_NUMBER = T.DI_SEQUENCE_NUMBER,
# MAGIC S.DI_OPERATION_TYPE = T.DI_OPERATION_TYPE,
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.EKORG = T.EKORG,
# MAGIC S.EKOTX = T.EKOTX,
# MAGIC S.BUKRS = T.BUKRS,
# MAGIC S.TXADR = T.TXADR,
# MAGIC S.TXKOP = T.TXKOP,
# MAGIC S.TXFUS = T.TXFUS,
# MAGIC S.TXGRU = T.TXGRU,
# MAGIC S.KALSE = T.KALSE,
# MAGIC S.MKALS = T.MKALS,
# MAGIC S.BPEFF = T.BPEFF,
# MAGIC S.BUKRS_NTR = T.BUKRS_NTR,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT 
# MAGIC     (
# MAGIC     DI_SEQUENCE_NUMBER ,
# MAGIC     DI_OPERATION_TYPE ,
# MAGIC     MANDT ,
# MAGIC     EKORG ,
# MAGIC     EKOTX ,
# MAGIC     BUKRS ,
# MAGIC     TXADR ,
# MAGIC     TXKOP ,
# MAGIC     TXFUS ,
# MAGIC     TXGRU ,
# MAGIC     KALSE ,
# MAGIC     MKALS ,
# MAGIC     BPEFF ,
# MAGIC     BUKRS_NTR ,
# MAGIC     ODQ_CHANGEMODE ,
# MAGIC     ODQ_ENTITYCNTR ,
# MAGIC     LandingFileTimeStamp ,
# MAGIC     UpdatedOn ,
# MAGIC     DataSource
# MAGIC     )
# MAGIC VALUES
# MAGIC (
# MAGIC   T.DI_SEQUENCE_NUMBER ,
# MAGIC T.DI_OPERATION_TYPE ,
# MAGIC T.MANDT ,
# MAGIC T.EKORG ,
# MAGIC T.EKOTX ,
# MAGIC T.BUKRS ,
# MAGIC T.TXADR ,
# MAGIC T.TXKOP ,
# MAGIC T.TXFUS ,
# MAGIC T.TXGRU ,
# MAGIC T.KALSE ,
# MAGIC T.MKALS ,
# MAGIC T.BPEFF ,
# MAGIC T.BUKRS_NTR ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now() ,
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


