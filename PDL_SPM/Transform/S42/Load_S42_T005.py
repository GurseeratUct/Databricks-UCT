# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'T005'
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

schema = StructType([\
                        StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('LAND1',StringType(),True) ,\
                        StructField('LANDK',StringType(),True) ,\
                        StructField('LNPLZ',IntegerType(),True) ,\
                        StructField('PRPLZ',StringType(),True) ,\
                        StructField('ADDRS',StringType(),True) ,\
                        StructField('XPLZS',StringType(),True) ,\
                        StructField('XPLPF',StringType(),True) ,\
                        StructField('SPRAS',StringType(),True) ,\
                        StructField('XLAND',StringType(),True) ,\
                        StructField('XADDR',StringType(),True) ,\
                        StructField('NMFMT',StringType(),True) ,\
                        StructField('XREGS',StringType(),True) ,\
                        StructField('XPLST',StringType(),True) ,\
                        StructField('INTCA',StringType(),True) ,\
                        StructField('INTCA3',StringType(),True) ,\
                        StructField('INTCN3',IntegerType(),True) ,\
                        StructField('XEGLD',StringType(),True) ,\
                        StructField('XSKFN',StringType(),True) ,\
                        StructField('XMWSN',StringType(),True) ,\
                        StructField('LNBKN',IntegerType(),True) ,\
                        StructField('PRBKN',StringType(),True) ,\
                        StructField('LNBLZ',IntegerType(),True) ,\
                        StructField('PRBLZ',StringType(),True) ,\
                        StructField('LNPSK',IntegerType(),True) ,\
                        StructField('PRPSK',StringType(),True) ,\
                        StructField('XPRBK',StringType(),True) ,\
                        StructField('BNKEY',StringType(),True) ,\
                        StructField('LNBKS',IntegerType(),True) ,\
                        StructField('PRBKS',StringType(),True) ,\
                        StructField('XPRSO',StringType(),True) ,\
                        StructField('PRUIN',StringType(),True) ,\
                        StructField('UINLN',IntegerType(),True) ,\
                        StructField('LNST1',IntegerType(),True) ,\
                        StructField('PRST1',StringType(),True) ,\
                        StructField('LNST2',IntegerType(),True) ,\
                        StructField('PRST2',StringType(),True) ,\
                        StructField('LNST3',IntegerType(),True) ,\
                        StructField('PRST3',StringType(),True) ,\
                        StructField('LNST4',IntegerType(),True) ,\
                        StructField('PRST4',StringType(),True) ,\
                        StructField('LNST5',IntegerType(),True) ,\
                        StructField('PRST5',StringType(),True) ,\
                        StructField('LANDD',StringType(),True) ,\
                        StructField('KALSM',StringType(),True) ,\
                        StructField('LANDA',StringType(),True) ,\
                        StructField('WECHF',IntegerType(),True) ,\
                        StructField('LKVRZ',StringType(),True) ,\
                        StructField('INTCN',IntegerType(),True) ,\
                        StructField('XDEZP',StringType(),True) ,\
                        StructField('DATFM',StringType(),True) ,\
                        StructField('CURIN',StringType(),True) ,\
                        StructField('CURHA',StringType(),True) ,\
                        StructField('WAERS',StringType(),True) ,\
                        StructField('KURST',StringType(),True) ,\
                        StructField('AFAPL',StringType(),True) ,\
                        StructField('GWGWRT',DoubleType(),True) ,\
                        StructField('UMRWRT',DoubleType(),True) ,\
                        StructField('KZRBWB',StringType(),True) ,\
                        StructField('XANZUM',StringType(),True) ,\
                        StructField('CTNCONCEPT',StringType(),True) ,\
                        StructField('KZSRV',StringType(),True) ,\
                        StructField('XXINVE',StringType(),True) ,\
                        StructField('XGCCV',StringType(),True) ,\
                        StructField('SUREG',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
                      ])

# COMMAND ----------

df_T005 = spark.read.format(read_format) \
          .option("header", True) \
          .option("delimiter","^") \
          .schema(schema)  \
          .load(read_path)

# COMMAND ----------

df_add_column = df_T005.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

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
# MAGIC MERGE INTO S42.T005  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,LAND1 ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T005)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.MANDT = T.MANDT 
# MAGIC and 
# MAGIC S.LAND1 = T.LAND1
# MAGIC WHEN  MATCHED 
# MAGIC THEN UPDATE SET
# MAGIC S.DI_SEQUENCE_NUMBER = T.DI_SEQUENCE_NUMBER,
# MAGIC S.DI_OPERATION_TYPE = T.DI_OPERATION_TYPE,
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.LAND1 = T.LAND1,
# MAGIC S.LANDK = T.LANDK,
# MAGIC S.LNPLZ = T.LNPLZ,
# MAGIC S.PRPLZ = T.PRPLZ,
# MAGIC S.ADDRS = T.ADDRS,
# MAGIC S.XPLZS = T.XPLZS,
# MAGIC S.XPLPF = T.XPLPF,
# MAGIC S.SPRAS = T.SPRAS,
# MAGIC S.XLAND = T.XLAND,
# MAGIC S.XADDR = T.XADDR,
# MAGIC S.NMFMT = T.NMFMT,
# MAGIC S.XREGS = T.XREGS,
# MAGIC S.XPLST = T.XPLST,
# MAGIC S.INTCA = T.INTCA,
# MAGIC S.INTCA3 = T.INTCA3,
# MAGIC S.INTCN3 = T.INTCN3,
# MAGIC S.XEGLD = T.XEGLD,
# MAGIC S.XSKFN = T.XSKFN,
# MAGIC S.XMWSN = T.XMWSN,
# MAGIC S.LNBKN = T.LNBKN,
# MAGIC S.PRBKN = T.PRBKN,
# MAGIC S.LNBLZ = T.LNBLZ,
# MAGIC S.PRBLZ = T.PRBLZ,
# MAGIC S.LNPSK = T.LNPSK,
# MAGIC S.PRPSK = T.PRPSK,
# MAGIC S.XPRBK = T.XPRBK,
# MAGIC S.BNKEY = T.BNKEY,
# MAGIC S.LNBKS = T.LNBKS,
# MAGIC S.PRBKS = T.PRBKS,
# MAGIC S.XPRSO = T.XPRSO,
# MAGIC S.PRUIN = T.PRUIN,
# MAGIC S.UINLN = T.UINLN,
# MAGIC S.LNST1 = T.LNST1,
# MAGIC S.PRST1 = T.PRST1,
# MAGIC S.LNST2 = T.LNST2,
# MAGIC S.PRST2 = T.PRST2,
# MAGIC S.LNST3 = T.LNST3,
# MAGIC S.PRST3 = T.PRST3,
# MAGIC S.LNST4 = T.LNST4,
# MAGIC S.PRST4 = T.PRST4,
# MAGIC S.LNST5 = T.LNST5,
# MAGIC S.PRST5 = T.PRST5,
# MAGIC S.LANDD = T.LANDD,
# MAGIC S.KALSM = T.KALSM,
# MAGIC S.LANDA = T.LANDA,
# MAGIC S.WECHF = T.WECHF,
# MAGIC S.LKVRZ = T.LKVRZ,
# MAGIC S.INTCN = T.INTCN,
# MAGIC S.XDEZP = T.XDEZP,
# MAGIC S.DATFM = T.DATFM,
# MAGIC S.CURIN = T.CURIN,
# MAGIC S.CURHA = T.CURHA,
# MAGIC S.WAERS = T.WAERS,
# MAGIC S.KURST = T.KURST,
# MAGIC S.AFAPL = T.AFAPL,
# MAGIC S.GWGWRT = T.GWGWRT,
# MAGIC S.UMRWRT = T.UMRWRT,
# MAGIC S.KZRBWB = T.KZRBWB,
# MAGIC S.XANZUM = T.XANZUM,
# MAGIC S.CTNCONCEPT = T.CTNCONCEPT,
# MAGIC S.KZSRV = T.KZSRV,
# MAGIC S.XXINVE = T.XXINVE,
# MAGIC S.XGCCV = T.XGCCV,
# MAGIC S.SUREG = T.SUREG,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT
# MAGIC (
# MAGIC DI_SEQUENCE_NUMBER,
# MAGIC DI_OPERATION_TYPE,
# MAGIC MANDT,
# MAGIC LAND1,
# MAGIC LANDK,
# MAGIC LNPLZ,
# MAGIC PRPLZ,
# MAGIC ADDRS,
# MAGIC XPLZS,
# MAGIC XPLPF,
# MAGIC SPRAS,
# MAGIC XLAND,
# MAGIC XADDR,
# MAGIC NMFMT,
# MAGIC XREGS,
# MAGIC XPLST,
# MAGIC INTCA,
# MAGIC INTCA3,
# MAGIC INTCN3,
# MAGIC XEGLD,
# MAGIC XSKFN,
# MAGIC XMWSN,
# MAGIC LNBKN,
# MAGIC PRBKN,
# MAGIC LNBLZ,
# MAGIC PRBLZ,
# MAGIC LNPSK,
# MAGIC PRPSK,
# MAGIC XPRBK,
# MAGIC BNKEY,
# MAGIC LNBKS,
# MAGIC PRBKS,
# MAGIC XPRSO,
# MAGIC PRUIN,
# MAGIC UINLN,
# MAGIC LNST1,
# MAGIC PRST1,
# MAGIC LNST2,
# MAGIC PRST2,
# MAGIC LNST3,
# MAGIC PRST3,
# MAGIC LNST4,
# MAGIC PRST4,
# MAGIC LNST5,
# MAGIC PRST5,
# MAGIC LANDD,
# MAGIC KALSM,
# MAGIC LANDA,
# MAGIC WECHF,
# MAGIC LKVRZ,
# MAGIC INTCN,
# MAGIC XDEZP,
# MAGIC DATFM,
# MAGIC CURIN,
# MAGIC CURHA,
# MAGIC WAERS,
# MAGIC KURST,
# MAGIC AFAPL,
# MAGIC GWGWRT,
# MAGIC UMRWRT,
# MAGIC KZRBWB,
# MAGIC XANZUM,
# MAGIC CTNCONCEPT,
# MAGIC KZSRV,
# MAGIC XXINVE,
# MAGIC XGCCV,
# MAGIC SUREG,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC  T.DI_SEQUENCE_NUMBER ,
# MAGIC T.DI_OPERATION_TYPE ,
# MAGIC T.MANDT ,
# MAGIC T.LAND1 ,
# MAGIC T.LANDK ,
# MAGIC T.LNPLZ ,
# MAGIC T.PRPLZ ,
# MAGIC T.ADDRS ,
# MAGIC T.XPLZS ,
# MAGIC T.XPLPF ,
# MAGIC T.SPRAS ,
# MAGIC T.XLAND ,
# MAGIC T.XADDR ,
# MAGIC T.NMFMT ,
# MAGIC T.XREGS ,
# MAGIC T.XPLST ,
# MAGIC T.INTCA ,
# MAGIC T.INTCA3 ,
# MAGIC T.INTCN3 ,
# MAGIC T.XEGLD ,
# MAGIC T.XSKFN ,
# MAGIC T.XMWSN ,
# MAGIC T.LNBKN ,
# MAGIC T.PRBKN ,
# MAGIC T.LNBLZ ,
# MAGIC T.PRBLZ ,
# MAGIC T.LNPSK ,
# MAGIC T.PRPSK ,
# MAGIC T.XPRBK ,
# MAGIC T.BNKEY ,
# MAGIC T.LNBKS ,
# MAGIC T.PRBKS ,
# MAGIC T.XPRSO ,
# MAGIC T.PRUIN ,
# MAGIC T.UINLN ,
# MAGIC T.LNST1 ,
# MAGIC T.PRST1 ,
# MAGIC T.LNST2 ,
# MAGIC T.PRST2 ,
# MAGIC T.LNST3 ,
# MAGIC T.PRST3 ,
# MAGIC T.LNST4 ,
# MAGIC T.PRST4 ,
# MAGIC T.LNST5 ,
# MAGIC T.PRST5 ,
# MAGIC T.LANDD ,
# MAGIC T.KALSM ,
# MAGIC T.LANDA ,
# MAGIC T.WECHF ,
# MAGIC T.LKVRZ ,
# MAGIC T.INTCN ,
# MAGIC T.XDEZP ,
# MAGIC T.DATFM ,
# MAGIC T.CURIN ,
# MAGIC T.CURHA ,
# MAGIC T.WAERS ,
# MAGIC T.KURST ,
# MAGIC T.AFAPL ,
# MAGIC T.GWGWRT ,
# MAGIC T.UMRWRT ,
# MAGIC T.KZRBWB ,
# MAGIC T.XANZUM ,
# MAGIC T.CTNCONCEPT ,
# MAGIC T.KZSRV ,
# MAGIC T.XXINVE ,
# MAGIC T.XGCCV ,
# MAGIC T.SUREG ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


