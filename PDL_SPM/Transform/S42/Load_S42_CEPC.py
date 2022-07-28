# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'CEPC'
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
StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',IntegerType(),True),\
StructField('PRCTR',StringType(),True),\
StructField('DATBI',StringType(),True),\
StructField('KOKRS',StringType(),True),\
StructField('DATAB',StringType(),True),\
StructField('ERSDA',StringType(),True),\
StructField('USNAM',StringType(),True),\
StructField('MERKMAL',StringType(),True),\
StructField('ABTEI',StringType(),True),\
StructField('VERAK',StringType(),True),\
StructField('VERAK_USER',StringType(),True),\
StructField('WAERS',StringType(),True),\
StructField('NPRCTR',StringType(),True),\
StructField('LAND1',StringType(),True),\
StructField('ANRED',StringType(),True),\
StructField('NAME1',StringType(),True),\
StructField('NAME2',StringType(),True),\
StructField('NAME3',StringType(),True),\
StructField('NAME4',StringType(),True),\
StructField('ORT01',StringType(),True),\
StructField('ORT02',StringType(),True),\
StructField('STRAS',StringType(),True),\
StructField('PFACH',StringType(),True),\
StructField('PSTLZ',StringType(),True),\
StructField('PSTL2',StringType(),True),\
StructField('SPRAS',StringType(),True),\
StructField('TELBX',StringType(),True),\
StructField('TELF1',StringType(),True),\
StructField('TELF2',StringType(),True),\
StructField('TELFX',StringType(),True),\
StructField('TELTX',StringType(),True),\
StructField('TELX1',StringType(),True),\
StructField('DATLT',StringType(),True),\
StructField('DRNAM',StringType(),True),\
StructField('KHINR',StringType(),True),\
StructField('BUKRS',StringType(),True),\
StructField('VNAME',StringType(),True),\
StructField('RECID',StringType(),True),\
StructField('ETYPE',StringType(),True),\
StructField('TXJCD',StringType(),True),\
StructField('REGIO',StringType(),True),\
StructField('KVEWE',StringType(),True),\
StructField('KAPPL',StringType(),True),\
StructField('KALSM',StringType(),True),\
StructField('LOGSYSTEM',StringType(),True),\
StructField('LOCK_IND',StringType(),True),\
StructField('PCA_TEMPLATE',StringType(),True),\
StructField('SEGMENT',StringType(),True),\
StructField('EEW_CEPC_PS_DUMMY',StringType(),True),\
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

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp, '-',''))\
                            .withColumn("DATBI", to_date(regexp_replace(df_add_column.DATBI,'\.','-')))\
                            .withColumn("DATAB", to_date(regexp_replace(df_add_column.DATAB,'\.','-')))\
                            .withColumn("ERSDA", to_date(regexp_replace(df_add_column.ERSDA,'\.','-')))
                            

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.CEPC as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,PRCTR,DATBI,KOKRS ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_CEPC where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON 
# MAGIC T.MANDT = S.MANDT and 
# MAGIC T.PRCTR = S.PRCTR and
# MAGIC T.DATBI = S.DATBI and
# MAGIC T.KOKRS = S.KOKRS 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`DI_SEQUENCE_NUMBER` =  S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` =  S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`PRCTR` =  S.`PRCTR`,
# MAGIC T.`DATBI` =  S.`DATBI`,
# MAGIC T.`KOKRS` =  S.`KOKRS`,
# MAGIC T.`DATAB` =  S.`DATAB`,
# MAGIC T.`ERSDA` =  S.`ERSDA`,
# MAGIC T.`USNAM` =  S.`USNAM`,
# MAGIC T.`MERKMAL` =  S.`MERKMAL`,
# MAGIC T.`ABTEI` =  S.`ABTEI`,
# MAGIC T.`VERAK` =  S.`VERAK`,
# MAGIC T.`VERAK_USER` =  S.`VERAK_USER`,
# MAGIC T.`WAERS` =  S.`WAERS`,
# MAGIC T.`NPRCTR` =  S.`NPRCTR`,
# MAGIC T.`LAND1` =  S.`LAND1`,
# MAGIC T.`ANRED` =  S.`ANRED`,
# MAGIC T.`NAME1` =  S.`NAME1`,
# MAGIC T.`NAME2` =  S.`NAME2`,
# MAGIC T.`NAME3` =  S.`NAME3`,
# MAGIC T.`NAME4` =  S.`NAME4`,
# MAGIC T.`ORT01` =  S.`ORT01`,
# MAGIC T.`ORT02` =  S.`ORT02`,
# MAGIC T.`STRAS` =  S.`STRAS`,
# MAGIC T.`PFACH` =  S.`PFACH`,
# MAGIC T.`PSTLZ` =  S.`PSTLZ`,
# MAGIC T.`PSTL2` =  S.`PSTL2`,
# MAGIC T.`SPRAS` =  S.`SPRAS`,
# MAGIC T.`TELBX` =  S.`TELBX`,
# MAGIC T.`TELF1` =  S.`TELF1`,
# MAGIC T.`TELF2` =  S.`TELF2`,
# MAGIC T.`TELFX` =  S.`TELFX`,
# MAGIC T.`TELTX` =  S.`TELTX`,
# MAGIC T.`TELX1` =  S.`TELX1`,
# MAGIC T.`DATLT` =  S.`DATLT`,
# MAGIC T.`DRNAM` =  S.`DRNAM`,
# MAGIC T.`KHINR` =  S.`KHINR`,
# MAGIC T.`BUKRS` =  S.`BUKRS`,
# MAGIC T.`VNAME` =  S.`VNAME`,
# MAGIC T.`RECID` =  S.`RECID`,
# MAGIC T.`ETYPE` =  S.`ETYPE`,
# MAGIC T.`TXJCD` =  S.`TXJCD`,
# MAGIC T.`REGIO` =  S.`REGIO`,
# MAGIC T.`KVEWE` =  S.`KVEWE`,
# MAGIC T.`KAPPL` =  S.`KAPPL`,
# MAGIC T.`KALSM` =  S.`KALSM`,
# MAGIC T.`LOGSYSTEM` =  S.`LOGSYSTEM`,
# MAGIC T.`LOCK_IND` =  S.`LOCK_IND`,
# MAGIC T.`PCA_TEMPLATE` =  S.`PCA_TEMPLATE`,
# MAGIC T.`SEGMENT` =  S.`SEGMENT`,
# MAGIC T.`EEW_CEPC_PS_DUMMY` =  S.`EEW_CEPC_PS_DUMMY`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `PRCTR`,
# MAGIC `DATBI`,
# MAGIC `KOKRS`,
# MAGIC `DATAB`,
# MAGIC `ERSDA`,
# MAGIC `USNAM`,
# MAGIC `MERKMAL`,
# MAGIC `ABTEI`,
# MAGIC `VERAK`,
# MAGIC `VERAK_USER`,
# MAGIC `WAERS`,
# MAGIC `NPRCTR`,
# MAGIC `LAND1`,
# MAGIC `ANRED`,
# MAGIC `NAME1`,
# MAGIC `NAME2`,
# MAGIC `NAME3`,
# MAGIC `NAME4`,
# MAGIC `ORT01`,
# MAGIC `ORT02`,
# MAGIC `STRAS`,
# MAGIC `PFACH`,
# MAGIC `PSTLZ`,
# MAGIC `PSTL2`,
# MAGIC `SPRAS`,
# MAGIC `TELBX`,
# MAGIC `TELF1`,
# MAGIC `TELF2`,
# MAGIC `TELFX`,
# MAGIC `TELTX`,
# MAGIC `TELX1`,
# MAGIC `DATLT`,
# MAGIC `DRNAM`,
# MAGIC `KHINR`,
# MAGIC `BUKRS`,
# MAGIC `VNAME`,
# MAGIC `RECID`,
# MAGIC `ETYPE`,
# MAGIC `TXJCD`,
# MAGIC `REGIO`,
# MAGIC `KVEWE`,
# MAGIC `KAPPL`,
# MAGIC `KALSM`,
# MAGIC `LOGSYSTEM`,
# MAGIC `LOCK_IND`,
# MAGIC `PCA_TEMPLATE`,
# MAGIC `SEGMENT`,
# MAGIC `EEW_CEPC_PS_DUMMY`,
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
# MAGIC S.`PRCTR`,
# MAGIC S.`DATBI`,
# MAGIC S.`KOKRS`,
# MAGIC S.`DATAB`,
# MAGIC S.`ERSDA`,
# MAGIC S.`USNAM`,
# MAGIC S.`MERKMAL`,
# MAGIC S.`ABTEI`,
# MAGIC S.`VERAK`,
# MAGIC S.`VERAK_USER`,
# MAGIC S.`WAERS`,
# MAGIC S.`NPRCTR`,
# MAGIC S.`LAND1`,
# MAGIC S.`ANRED`,
# MAGIC S.`NAME1`,
# MAGIC S.`NAME2`,
# MAGIC S.`NAME3`,
# MAGIC S.`NAME4`,
# MAGIC S.`ORT01`,
# MAGIC S.`ORT02`,
# MAGIC S.`STRAS`,
# MAGIC S.`PFACH`,
# MAGIC S.`PSTLZ`,
# MAGIC S.`PSTL2`,
# MAGIC S.`SPRAS`,
# MAGIC S.`TELBX`,
# MAGIC S.`TELF1`,
# MAGIC S.`TELF2`,
# MAGIC S.`TELFX`,
# MAGIC S.`TELTX`,
# MAGIC S.`TELX1`,
# MAGIC S.`DATLT`,
# MAGIC S.`DRNAM`,
# MAGIC S.`KHINR`,
# MAGIC S.`BUKRS`,
# MAGIC S.`VNAME`,
# MAGIC S.`RECID`,
# MAGIC S.`ETYPE`,
# MAGIC S.`TXJCD`,
# MAGIC S.`REGIO`,
# MAGIC S.`KVEWE`,
# MAGIC S.`KAPPL`,
# MAGIC S.`KALSM`,
# MAGIC S.`LOGSYSTEM`,
# MAGIC S.`LOCK_IND`,
# MAGIC S.`PCA_TEMPLATE`,
# MAGIC S.`SEGMENT`,
# MAGIC S.`EEW_CEPC_PS_DUMMY`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


