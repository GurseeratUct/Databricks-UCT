# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date 
from delta.tables import *

# COMMAND ----------

table_name = 'T134'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

#df_T134 = spark.read.format(read_format) \
#     .option("header", True) \
#     .option("delimiter","^") \
#     .option("inferschema",True) \
#     .load(read_path)

# COMMAND ----------

schema = StructType([ \
StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',IntegerType(),True),\
StructField('MTART',StringType(),True),\
StructField('MTREF',StringType(),True),\
StructField('MBREF',StringType(),True),\
StructField('FLREF',StringType(),True),\
StructField('NUMKI',StringType(),True),\
StructField('NUMKE',StringType(),True),\
StructField('ENVOP',StringType(),True),\
StructField('BSEXT',IntegerType(),True),\
StructField('BSINT',IntegerType(),True),\
StructField('PSTAT',StringType(),True),\
StructField('KKREF',StringType(),True),\
StructField('VPRSV',StringType(),True),\
StructField('KZVPR',StringType(),True),\
StructField('VMTPO',StringType(),True),\
StructField('EKALR',StringType(),True),\
StructField('KZGRP',StringType(),True),\
StructField('KZKFG',StringType(),True),\
StructField('BEGRU',StringType(),True),\
StructField('KZPRC',StringType(),True),\
StructField('KZPIP',StringType(),True),\
StructField('PRDRU',StringType(),True),\
StructField('ARANZ',StringType(),True),\
StructField('WMAKG',StringType(),True),\
StructField('IZUST',StringType(),True),\
StructField('ARDEL',IntegerType(),True),\
StructField('KZMPN',StringType(),True),\
StructField('MSTAE',StringType(),True),\
StructField('CCHIS',StringType(),True),\
StructField('CTYPE',StringType(),True),\
StructField('CLASS',StringType(),True),\
StructField('CHNEU',StringType(),True),\
StructField('PROD_TYPE_CODE',StringType(),True),\
StructField('CONCTD_MATNR',StringType(),True),\
StructField('VTYPE',StringType(),True),\
StructField('VNUMKI',StringType(),True),\
StructField('VNUMKE',StringType(),True),\
StructField('KZFFF',StringType(),True),\
StructField('KZRAC',StringType(),True),\
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
# MAGIC MERGE INTO S42.T134 as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,MTART ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T134 where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON 
# MAGIC T.MANDT = S.MANDT and 
# MAGIC T.MTART = S.MTART 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`DI_SEQUENCE_NUMBER` =  S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` =  S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`MTART` =  S.`MTART`,
# MAGIC T.`MTREF` =  S.`MTREF`,
# MAGIC T.`MBREF` =  S.`MBREF`,
# MAGIC T.`FLREF` =  S.`FLREF`,
# MAGIC T.`NUMKI` =  S.`NUMKI`,
# MAGIC T.`NUMKE` =  S.`NUMKE`,
# MAGIC T.`ENVOP` =  S.`ENVOP`,
# MAGIC T.`BSEXT` =  S.`BSEXT`,
# MAGIC T.`BSINT` =  S.`BSINT`,
# MAGIC T.`PSTAT` =  S.`PSTAT`,
# MAGIC T.`KKREF` =  S.`KKREF`,
# MAGIC T.`VPRSV` =  S.`VPRSV`,
# MAGIC T.`KZVPR` =  S.`KZVPR`,
# MAGIC T.`VMTPO` =  S.`VMTPO`,
# MAGIC T.`EKALR` =  S.`EKALR`,
# MAGIC T.`KZGRP` =  S.`KZGRP`,
# MAGIC T.`KZKFG` =  S.`KZKFG`,
# MAGIC T.`BEGRU` =  S.`BEGRU`,
# MAGIC T.`KZPRC` =  S.`KZPRC`,
# MAGIC T.`KZPIP` =  S.`KZPIP`,
# MAGIC T.`PRDRU` =  S.`PRDRU`,
# MAGIC T.`ARANZ` =  S.`ARANZ`,
# MAGIC T.`WMAKG` =  S.`WMAKG`,
# MAGIC T.`IZUST` =  S.`IZUST`,
# MAGIC T.`ARDEL` =  S.`ARDEL`,
# MAGIC T.`KZMPN` =  S.`KZMPN`,
# MAGIC T.`MSTAE` =  S.`MSTAE`,
# MAGIC T.`CCHIS` =  S.`CCHIS`,
# MAGIC T.`CTYPE` =  S.`CTYPE`,
# MAGIC T.`CLASS` =  S.`CLASS`,
# MAGIC T.`CHNEU` =  S.`CHNEU`,
# MAGIC T.`PROD_TYPE_CODE` =  S.`PROD_TYPE_CODE`,
# MAGIC T.`CONCTD_MATNR` =  S.`CONCTD_MATNR`,
# MAGIC T.`VTYPE` =  S.`VTYPE`,
# MAGIC T.`VNUMKI` =  S.`VNUMKI`,
# MAGIC T.`VNUMKE` =  S.`VNUMKE`,
# MAGIC T.`KZFFF` =  S.`KZFFF`,
# MAGIC T.`KZRAC` =  S.`KZRAC`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `MTART`,
# MAGIC `MTREF`,
# MAGIC `MBREF`,
# MAGIC `FLREF`,
# MAGIC `NUMKI`,
# MAGIC `NUMKE`,
# MAGIC `ENVOP`,
# MAGIC `BSEXT`,
# MAGIC `BSINT`,
# MAGIC `PSTAT`,
# MAGIC `KKREF`,
# MAGIC `VPRSV`,
# MAGIC `KZVPR`,
# MAGIC `VMTPO`,
# MAGIC `EKALR`,
# MAGIC `KZGRP`,
# MAGIC `KZKFG`,
# MAGIC `BEGRU`,
# MAGIC `KZPRC`,
# MAGIC `KZPIP`,
# MAGIC `PRDRU`,
# MAGIC `ARANZ`,
# MAGIC `WMAKG`,
# MAGIC `IZUST`,
# MAGIC `ARDEL`,
# MAGIC `KZMPN`,
# MAGIC `MSTAE`,
# MAGIC `CCHIS`,
# MAGIC `CTYPE`,
# MAGIC `CLASS`,
# MAGIC `CHNEU`,
# MAGIC `PROD_TYPE_CODE`,
# MAGIC `CONCTD_MATNR`,
# MAGIC `VTYPE`,
# MAGIC `VNUMKI`,
# MAGIC `VNUMKE`,
# MAGIC `KZFFF`,
# MAGIC `KZRAC`,
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
# MAGIC S.`MTART`,
# MAGIC S.`MTREF`,
# MAGIC S.`MBREF`,
# MAGIC S.`FLREF`,
# MAGIC S.`NUMKI`,
# MAGIC S.`NUMKE`,
# MAGIC S.`ENVOP`,
# MAGIC S.`BSEXT`,
# MAGIC S.`BSINT`,
# MAGIC S.`PSTAT`,
# MAGIC S.`KKREF`,
# MAGIC S.`VPRSV`,
# MAGIC S.`KZVPR`,
# MAGIC S.`VMTPO`,
# MAGIC S.`EKALR`,
# MAGIC S.`KZGRP`,
# MAGIC S.`KZKFG`,
# MAGIC S.`BEGRU`,
# MAGIC S.`KZPRC`,
# MAGIC S.`KZPIP`,
# MAGIC S.`PRDRU`,
# MAGIC S.`ARANZ`,
# MAGIC S.`WMAKG`,
# MAGIC S.`IZUST`,
# MAGIC S.`ARDEL`,
# MAGIC S.`KZMPN`,
# MAGIC S.`MSTAE`,
# MAGIC S.`CCHIS`,
# MAGIC S.`CTYPE`,
# MAGIC S.`CLASS`,
# MAGIC S.`CHNEU`,
# MAGIC S.`PROD_TYPE_CODE`,
# MAGIC S.`CONCTD_MATNR`,
# MAGIC S.`VTYPE`,
# MAGIC S.`VNUMKI`,
# MAGIC S.`VNUMKE`,
# MAGIC S.`KZFFF`,
# MAGIC S.`KZRAC`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


