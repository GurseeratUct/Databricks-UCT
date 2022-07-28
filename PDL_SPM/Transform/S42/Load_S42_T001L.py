# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date 
from delta.tables import *

# COMMAND ----------

table_name = 'T001L'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

#df_T001L = spark.read.format(read_format) \
#      .option("header", True) \
#      .option("delimiter","^") \
#      .option("inferschema",True) \
#      .load(read_path)

# COMMAND ----------

schema = StructType([ \
                     StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',IntegerType(),True),\
StructField('WERKS',StringType(),True),\
StructField('LGORT',StringType(),True),\
StructField('LGOBE',StringType(),True),\
StructField('SPART',StringType(),True),\
StructField('XLONG',StringType(),True),\
StructField('XBUFX',StringType(),True),\
StructField('DISKZ',StringType(),True),\
StructField('XBLGO',StringType(),True),\
StructField('XRESS',StringType(),True),\
StructField('XHUPF',StringType(),True),\
StructField('PARLG',StringType(),True),\
StructField('VKORG',StringType(),True),\
StructField('VTWEG',StringType(),True),\
StructField('VSTEL',StringType(),True),\
StructField('LIFNR',StringType(),True),\
StructField('KUNNR',StringType(),True),\
StructField('MESBS',StringType(),True),\
StructField('MESST',StringType(),True),\
StructField('OIH_LICNO',StringType(),True),\
StructField('OIG_ITRFL',StringType(),True),\
StructField('OIB_TNKASSIGN',StringType(),True),\
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
# MAGIC MERGE INTO S42.T001L as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,WERKS,LGORT ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T001L where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON 
# MAGIC T.MANDT = S.MANDT and 
# MAGIC T.WERKS = S.WERKS and
# MAGIC T.LGORT = S.LGORT 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`DI_SEQUENCE_NUMBER` =  S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` =  S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`WERKS` =  S.`WERKS`,
# MAGIC T.`LGORT` =  S.`LGORT`,
# MAGIC T.`LGOBE` =  S.`LGOBE`,
# MAGIC T.`SPART` =  S.`SPART`,
# MAGIC T.`XLONG` =  S.`XLONG`,
# MAGIC T.`XBUFX` =  S.`XBUFX`,
# MAGIC T.`DISKZ` =  S.`DISKZ`,
# MAGIC T.`XBLGO` =  S.`XBLGO`,
# MAGIC T.`XRESS` =  S.`XRESS`,
# MAGIC T.`XHUPF` =  S.`XHUPF`,
# MAGIC T.`PARLG` =  S.`PARLG`,
# MAGIC T.`VKORG` =  S.`VKORG`,
# MAGIC T.`VTWEG` =  S.`VTWEG`,
# MAGIC T.`VSTEL` =  S.`VSTEL`,
# MAGIC T.`LIFNR` =  S.`LIFNR`,
# MAGIC T.`KUNNR` =  S.`KUNNR`,
# MAGIC T.`MESBS` =  S.`MESBS`,
# MAGIC T.`MESST` =  S.`MESST`,
# MAGIC T.`OIH_LICNO` =  S.`OIH_LICNO`,
# MAGIC T.`OIG_ITRFL` =  S.`OIG_ITRFL`,
# MAGIC T.`OIB_TNKASSIGN` =  S.`OIB_TNKASSIGN`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `WERKS`,
# MAGIC `LGORT`,
# MAGIC `LGOBE`,
# MAGIC `SPART`,
# MAGIC `XLONG`,
# MAGIC `XBUFX`,
# MAGIC `DISKZ`,
# MAGIC `XBLGO`,
# MAGIC `XRESS`,
# MAGIC `XHUPF`,
# MAGIC `PARLG`,
# MAGIC `VKORG`,
# MAGIC `VTWEG`,
# MAGIC `VSTEL`,
# MAGIC `LIFNR`,
# MAGIC `KUNNR`,
# MAGIC `MESBS`,
# MAGIC `MESST`,
# MAGIC `OIH_LICNO`,
# MAGIC `OIG_ITRFL`,
# MAGIC `OIB_TNKASSIGN`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC   VALUES (
# MAGIC S.`DI_SEQUENCE_NUMBER`,
# MAGIC S.`DI_OPERATION_TYPE`,
# MAGIC S.`MANDT`,
# MAGIC S.`WERKS`,
# MAGIC S.`LGORT`,
# MAGIC S.`LGOBE`,
# MAGIC S.`SPART`,
# MAGIC S.`XLONG`,
# MAGIC S.`XBUFX`,
# MAGIC S.`DISKZ`,
# MAGIC S.`XBLGO`,
# MAGIC S.`XRESS`,
# MAGIC S.`XHUPF`,
# MAGIC S.`PARLG`,
# MAGIC S.`VKORG`,
# MAGIC S.`VTWEG`,
# MAGIC S.`VSTEL`,
# MAGIC S.`LIFNR`,
# MAGIC S.`KUNNR`,
# MAGIC S.`MESBS`,
# MAGIC S.`MESST`,
# MAGIC S.`OIH_LICNO`,
# MAGIC S.`OIG_ITRFL`,
# MAGIC S.`OIB_TNKASSIGN`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


