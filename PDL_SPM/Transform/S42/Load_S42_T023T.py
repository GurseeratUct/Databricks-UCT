# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date 
from delta.tables import *

# COMMAND ----------

table_name = 'T023T'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

#df_T023T = spark.read.format(read_format) \
#      .option("header", True) \
#      .option("delimiter","^") \
#      .option("inferschema",True) \
#      .load(read_path)

# COMMAND ----------

schema = StructType([ \
                     StructField('DI_SEQUENCE_NUMBER',StringType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',StringType(),True),\
StructField('SPRAS',StringType(),True),\
StructField('MATKL',StringType(),True),\
StructField('WGBEZ',StringType(),True),\
StructField('WGBEZ60',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
   ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter","^") \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.T023T as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,SPRAS,MATKL ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T023T where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON 
# MAGIC T.MANDT = S.MANDT and 
# MAGIC T.SPRAS = S.SPRAS and
# MAGIC T.MATKL = S.MATKL 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`DI_SEQUENCE_NUMBER` =  S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` =  S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`SPRAS` =  S.`SPRAS`,
# MAGIC T.`MATKL` =  S.`MATKL`,
# MAGIC T.`WGBEZ` =  S.`WGBEZ`,
# MAGIC T.`WGBEZ60` =  S.`WGBEZ60`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `SPRAS`,
# MAGIC `MATKL`,
# MAGIC `WGBEZ`,
# MAGIC `WGBEZ60`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC   VALUES (
# MAGIC   S.`DI_SEQUENCE_NUMBER`,
# MAGIC S.`DI_OPERATION_TYPE`,
# MAGIC S.`MANDT`,
# MAGIC S.`SPRAS`,
# MAGIC S.`MATKL`,
# MAGIC S.`WGBEZ`,
# MAGIC S.`WGBEZ60`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


