# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'SKA1'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

#df = spark.read.format(read_format) \
#      .option("header", True) \
#      .option("delimiter","^") \
#      .option("inferschema",True) \
#      .load(read_path)

# COMMAND ----------

schema = StructType([ \
                     StructField('DI_SEQUENCE_NUMBER',StringType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',StringType(),True),\
StructField('KTOPL',StringType(),True),\
StructField('SAKNR',StringType(),True),\
StructField('XBILK',StringType(),True),\
StructField('SAKAN',StringType(),True),\
StructField('BILKT',StringType(),True),\
StructField('ERDAT',StringType(),True),\
StructField('ERNAM',StringType(),True),\
StructField('GVTYP',StringType(),True),\
StructField('KTOKS',StringType(),True),\
StructField('MUSTR',StringType(),True),\
StructField('VBUND',StringType(),True),\
StructField('XLOEV',StringType(),True),\
StructField('XSPEA',StringType(),True),\
StructField('XSPEB',StringType(),True),\
StructField('XSPEP',StringType(),True),\
StructField('MCOD1',StringType(),True),\
StructField('FUNC_AREA',StringType(),True),\
StructField('GLACCOUNT_TYPE',StringType(),True),\
StructField('GLACCOUNT_SUBTYPE',StringType(),True),\
StructField('MAIN_SAKNR',StringType(),True),\
StructField('LAST_CHANGED_TS',StringType(),True),\
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

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp, '-','')) \
                            .withColumn("ERDAT", to_date(regexp_replace(df_add_column.ERDAT,'\.','-')))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.SKA1 as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,KTOPL,SAKNR ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_SKA1 where MANDT = '100' )A where A.rn = 1 ) as S 
# MAGIC ON 
# MAGIC T.MANDT = S.MANDT and 
# MAGIC T.KTOPL = S.KTOPL and
# MAGIC T.SAKNR = S.SAKNR 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`DI_SEQUENCE_NUMBER` =  S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` =  S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`KTOPL` =  S.`KTOPL`,
# MAGIC T.`SAKNR` =  S.`SAKNR`,
# MAGIC T.`XBILK` =  S.`XBILK`,
# MAGIC T.`SAKAN` =  S.`SAKAN`,
# MAGIC T.`BILKT` =  S.`BILKT`,
# MAGIC T.`ERDAT` =  S.`ERDAT`,
# MAGIC T.`ERNAM` =  S.`ERNAM`,
# MAGIC T.`GVTYP` =  S.`GVTYP`,
# MAGIC T.`KTOKS` =  S.`KTOKS`,
# MAGIC T.`MUSTR` =  S.`MUSTR`,
# MAGIC T.`VBUND` =  S.`VBUND`,
# MAGIC T.`XLOEV` =  S.`XLOEV`,
# MAGIC T.`XSPEA` =  S.`XSPEA`,
# MAGIC T.`XSPEB` =  S.`XSPEB`,
# MAGIC T.`XSPEP` =  S.`XSPEP`,
# MAGIC T.`MCOD1` =  S.`MCOD1`,
# MAGIC T.`FUNC_AREA` =  S.`FUNC_AREA`,
# MAGIC T.`GLACCOUNT_TYPE` =  S.`GLACCOUNT_TYPE`,
# MAGIC T.`GLACCOUNT_SUBTYPE` =  S.`GLACCOUNT_SUBTYPE`,
# MAGIC T.`MAIN_SAKNR` =  S.`MAIN_SAKNR`,
# MAGIC T.`LAST_CHANGED_TS` =  S.`LAST_CHANGED_TS`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `KTOPL`,
# MAGIC `SAKNR`,
# MAGIC `XBILK`,
# MAGIC `SAKAN`,
# MAGIC `BILKT`,
# MAGIC `ERDAT`,
# MAGIC `ERNAM`,
# MAGIC `GVTYP`,
# MAGIC `KTOKS`,
# MAGIC `MUSTR`,
# MAGIC `VBUND`,
# MAGIC `XLOEV`,
# MAGIC `XSPEA`,
# MAGIC `XSPEB`,
# MAGIC `XSPEP`,
# MAGIC `MCOD1`,
# MAGIC `FUNC_AREA`,
# MAGIC `GLACCOUNT_TYPE`,
# MAGIC `GLACCOUNT_SUBTYPE`,
# MAGIC `MAIN_SAKNR`,
# MAGIC `LAST_CHANGED_TS`,
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
# MAGIC S.`KTOPL`,
# MAGIC S.`SAKNR`,
# MAGIC S.`XBILK`,
# MAGIC S.`SAKAN`,
# MAGIC S.`BILKT`,
# MAGIC S.`ERDAT`,
# MAGIC S.`ERNAM`,
# MAGIC S.`GVTYP`,
# MAGIC S.`KTOKS`,
# MAGIC S.`MUSTR`,
# MAGIC S.`VBUND`,
# MAGIC S.`XLOEV`,
# MAGIC S.`XSPEA`,
# MAGIC S.`XSPEB`,
# MAGIC S.`XSPEP`,
# MAGIC S.`MCOD1`,
# MAGIC S.`FUNC_AREA`,
# MAGIC S.`GLACCOUNT_TYPE`,
# MAGIC S.`GLACCOUNT_SUBTYPE`,
# MAGIC S.`MAIN_SAKNR`,
# MAGIC S.`LAST_CHANGED_TS`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


