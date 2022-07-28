# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'MARM'
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
#      .option("delimiter",delimiter) \
#      .option("InferSchema",True) \
#      .load(read_path)

# COMMAND ----------

schema = StructType([ \
StructField("DI_SEQUENCE_NUMBER",IntegerType(),True),\
                     StructField("DI_OPERATION_TYPE",StringType(),True),\
StructField("MANDT",StringType(),True),\
StructField("MATNR",StringType(),True),\
StructField("MEINH",StringType(),True),\
StructField("UMREZ",StringType(),True),\
StructField("UMREN",StringType(),True),\
StructField("EANNR",StringType(),True),\
StructField("EAN11",StringType(),True),\
StructField("NUMTP",StringType(),True),\
StructField("LAENG",StringType(),True),\
StructField("BREIT",StringType(),True),\
StructField("HOEHE",StringType(),True),\
StructField("MEABM",StringType(),True),\
StructField("VOLUM",StringType(),True),\
StructField("VOLEH",StringType(),True),\
StructField("BRGEW",StringType(),True),\
StructField("GEWEI",StringType(),True),\
StructField("MESUB",StringType(),True),\
StructField("ATINN",StringType(),True),\
StructField("MESRT",StringType(),True),\
StructField("XFHDW",StringType(),True),\
StructField("XBEWW",StringType(),True),\
StructField("KZWSO",StringType(),True),\
StructField("MSEHI",StringType(),True),\
StructField("BFLME_MARM",StringType(),True),\
StructField("GTIN_VARIANT",StringType(),True),\
StructField("NEST_FTR",StringType(),True),\
StructField("MAX_STACK",StringType(),True),\
StructField("TOP_LOAD_FULL",StringType(),True),\
StructField("TOP_LOAD_FULL_UOM",StringType(),True),\
StructField("CAPAUSE",StringType(),True),\
StructField("TY2TQ",StringType(),True),\
StructField("DUMMY_UOM_INCL_EEW_PS",StringType(),True),\
StructField("/CWM/TY2TQ",StringType(),True),\
StructField("/STTPEC/NCODE",StringType(),True),\
StructField("/STTPEC/NCODE_TY",StringType(),True),\
StructField("/STTPEC/RCODE",StringType(),True),\
StructField("/STTPEC/SERUSE",StringType(),True),\
StructField("/STTPEC/SYNCCHG",StringType(),True),\
StructField("/STTPEC/SERNO_MANAGED",StringType(),True),\
StructField("/STTPEC/SERNO_PROV_BUP",StringType(),True),\
StructField("/STTPEC/UOM_SYNC",StringType(),True),\
StructField("/STTPEC/SER_GTIN",StringType(),True),\
StructField("PCBUT",StringType(),True),\
StructField("ODQ_CHANGEMODE",StringType(),True),\
StructField("ODQ_ENTITYCNTR",StringType(),True),\
StructField("LandingFileTimeStamp",StringType(),True),\
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
# MAGIC MERGE INTO S42.MARM as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY MANDT,MATNR,MEINH ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_MARM)A where  A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and
# MAGIC T.MATNR = S.MATNR and
# MAGIC T.MEINH = S.MEINH 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`DI_SEQUENCE_NUMBER` = S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` = S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`MATNR` =  S.`MATNR`,
# MAGIC T.`MEINH` =  S.`MEINH`,
# MAGIC T.`UMREZ` =  S.`UMREZ`,
# MAGIC T.`UMREN` =  S.`UMREN`,
# MAGIC T.`EANNR` =  S.`EANNR`,
# MAGIC T.`EAN11` =  S.`EAN11`,
# MAGIC T.`NUMTP` =  S.`NUMTP`,
# MAGIC T.`LAENG` =  S.`LAENG`,
# MAGIC T.`BREIT` =  S.`BREIT`,
# MAGIC T.`HOEHE` =  S.`HOEHE`,
# MAGIC T.`MEABM` =  S.`MEABM`,
# MAGIC T.`VOLUM` =  S.`VOLUM`,
# MAGIC T.`VOLEH` =  S.`VOLEH`,
# MAGIC T.`BRGEW` =  S.`BRGEW`,
# MAGIC T.`GEWEI` =  S.`GEWEI`,
# MAGIC T.`MESUB` =  S.`MESUB`,
# MAGIC T.`ATINN` =  S.`ATINN`,
# MAGIC T.`MESRT` =  S.`MESRT`,
# MAGIC T.`XFHDW` =  S.`XFHDW`,
# MAGIC T.`XBEWW` =  S.`XBEWW`,
# MAGIC T.`KZWSO` =  S.`KZWSO`,
# MAGIC T.`MSEHI` =  S.`MSEHI`,
# MAGIC T.`BFLME_MARM` =  S.`BFLME_MARM`,
# MAGIC T.`GTIN_VARIANT` =  S.`GTIN_VARIANT`,
# MAGIC T.`NEST_FTR` =  S.`NEST_FTR`,
# MAGIC T.`MAX_STACK` =  S.`MAX_STACK`,
# MAGIC T.`TOP_LOAD_FULL` =  S.`TOP_LOAD_FULL`,
# MAGIC T.`TOP_LOAD_FULL_UOM` =  S.`TOP_LOAD_FULL_UOM`,
# MAGIC T.`CAPAUSE` =  S.`CAPAUSE`,
# MAGIC T.`TY2TQ` =  S.`TY2TQ`,
# MAGIC T.`DUMMY_UOM_INCL_EEW_PS` =  S.`DUMMY_UOM_INCL_EEW_PS`,
# MAGIC T.`/CWM/TY2TQ` =  S.`/CWM/TY2TQ`,
# MAGIC T.`/STTPEC/NCODE` =  S.`/STTPEC/NCODE`,
# MAGIC T.`/STTPEC/NCODE_TY` =  S.`/STTPEC/NCODE_TY`,
# MAGIC T.`/STTPEC/RCODE` =  S.`/STTPEC/RCODE`,
# MAGIC T.`/STTPEC/SERUSE` =  S.`/STTPEC/SERUSE`,
# MAGIC T.`/STTPEC/SYNCCHG` =  S.`/STTPEC/SYNCCHG`,
# MAGIC T.`/STTPEC/SERNO_MANAGED` =  S.`/STTPEC/SERNO_MANAGED`,
# MAGIC T.`/STTPEC/SERNO_PROV_BUP` =  S.`/STTPEC/SERNO_PROV_BUP`,
# MAGIC T.`/STTPEC/UOM_SYNC` =  S.`/STTPEC/UOM_SYNC`,
# MAGIC T.`/STTPEC/SER_GTIN` =  S.`/STTPEC/SER_GTIN`,
# MAGIC T.`PCBUT` =  S.`PCBUT`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `DI_SEQUENCE_NUMBER`,
# MAGIC     `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `MATNR`,
# MAGIC `MEINH`,
# MAGIC `UMREZ`,
# MAGIC `UMREN`,
# MAGIC `EANNR`,
# MAGIC `EAN11`,
# MAGIC `NUMTP`,
# MAGIC `LAENG`,
# MAGIC `BREIT`,
# MAGIC `HOEHE`,
# MAGIC `MEABM`,
# MAGIC `VOLUM`,
# MAGIC `VOLEH`,
# MAGIC `BRGEW`,
# MAGIC `GEWEI`,
# MAGIC `MESUB`,
# MAGIC `ATINN`,
# MAGIC `MESRT`,
# MAGIC `XFHDW`,
# MAGIC `XBEWW`,
# MAGIC `KZWSO`,
# MAGIC `MSEHI`,
# MAGIC `BFLME_MARM`,
# MAGIC `GTIN_VARIANT`,
# MAGIC `NEST_FTR`,
# MAGIC `MAX_STACK`,
# MAGIC `TOP_LOAD_FULL`,
# MAGIC `TOP_LOAD_FULL_UOM`,
# MAGIC `CAPAUSE`,
# MAGIC `TY2TQ`,
# MAGIC `DUMMY_UOM_INCL_EEW_PS`,
# MAGIC `/CWM/TY2TQ`,
# MAGIC `/STTPEC/NCODE`,
# MAGIC `/STTPEC/NCODE_TY`,
# MAGIC `/STTPEC/RCODE`,
# MAGIC `/STTPEC/SERUSE`,
# MAGIC `/STTPEC/SYNCCHG`,
# MAGIC `/STTPEC/SERNO_MANAGED`,
# MAGIC `/STTPEC/SERNO_PROV_BUP`,
# MAGIC `/STTPEC/UOM_SYNC`,
# MAGIC `/STTPEC/SER_GTIN`,
# MAGIC `PCBUT`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`DI_SEQUENCE_NUMBER`,
# MAGIC S.`DI_OPERATION_TYPE`,
# MAGIC S.`MANDT`,
# MAGIC S.`MATNR`,
# MAGIC S.`MEINH`,
# MAGIC S.`UMREZ`,
# MAGIC S.`UMREN`,
# MAGIC S.`EANNR`,
# MAGIC S.`EAN11`,
# MAGIC S.`NUMTP`,
# MAGIC S.`LAENG`,
# MAGIC S.`BREIT`,
# MAGIC S.`HOEHE`,
# MAGIC S.`MEABM`,
# MAGIC S.`VOLUM`,
# MAGIC S.`VOLEH`,
# MAGIC S.`BRGEW`,
# MAGIC S.`GEWEI`,
# MAGIC S.`MESUB`,
# MAGIC S.`ATINN`,
# MAGIC S.`MESRT`,
# MAGIC S.`XFHDW`,
# MAGIC S.`XBEWW`,
# MAGIC S.`KZWSO`,
# MAGIC S.`MSEHI`,
# MAGIC S.`BFLME_MARM`,
# MAGIC S.`GTIN_VARIANT`,
# MAGIC S.`NEST_FTR`,
# MAGIC S.`MAX_STACK`,
# MAGIC S.`TOP_LOAD_FULL`,
# MAGIC S.`TOP_LOAD_FULL_UOM`,
# MAGIC S.`CAPAUSE`,
# MAGIC S.`TY2TQ`,
# MAGIC S.`DUMMY_UOM_INCL_EEW_PS`,
# MAGIC S.`/CWM/TY2TQ`,
# MAGIC S.`/STTPEC/NCODE`,
# MAGIC S.`/STTPEC/NCODE_TY`,
# MAGIC S.`/STTPEC/RCODE`,
# MAGIC S.`/STTPEC/SERUSE`,
# MAGIC S.`/STTPEC/SYNCCHG`,
# MAGIC S.`/STTPEC/SERNO_MANAGED`,
# MAGIC S.`/STTPEC/SERNO_PROV_BUP`,
# MAGIC S.`/STTPEC/UOM_SYNC`,
# MAGIC S.`/STTPEC/SER_GTIN`,
# MAGIC S.`PCBUT`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now()
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


