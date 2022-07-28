# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'TCURT'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.options(header='True', inferSchema='True', delimiter='^').csv(read_path)

# COMMAND ----------

schema = StructType([ \
StructField('MANDT',IntegerType(),True),\
StructField('SPRAS',StringType(),True),\
StructField('WAERS',StringType(),True),\
StructField('LTEXT',StringType(),True),\
StructField('KTEXT',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
StructField('LandingFileTimeStamp',StringType(),True)
                     ])

                     

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
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
# MAGIC MERGE INTO S42.TCURT as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY SPRAS,WAERS ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_TCURT where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and 
# MAGIC T.SPRAS = S.SPRAS and 
# MAGIC T.WAERS = S.WAERS  
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.MANDT =  S.MANDT,
# MAGIC T.SPRAS =  S.SPRAS,
# MAGIC T.WAERS =  S.WAERS,
# MAGIC T.LTEXT =  S.LTEXT,
# MAGIC T.KTEXT =  S.KTEXT,
# MAGIC T.ODQ_CHANGEMODE =  S.ODQ_CHANGEMODE,
# MAGIC T.ODQ_ENTITYCNTR =  S.ODQ_ENTITYCNTR,
# MAGIC T.LandingFileTimeStamp =  S.LandingFileTimeStamp,
# MAGIC T.UpdatedOn = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC MANDT,
# MAGIC SPRAS,
# MAGIC WAERS,
# MAGIC LTEXT,
# MAGIC KTEXT,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC DataSource,
# MAGIC UpdatedOn
# MAGIC ) VALUES 
# MAGIC (
# MAGIC S.MANDT,
# MAGIC S.SPRAS,
# MAGIC S.WAERS,
# MAGIC S.LTEXT,
# MAGIC S.KTEXT,
# MAGIC S.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp,
# MAGIC 'SAP',
# MAGIC now()
# MAGIC )
# MAGIC  

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


