# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType,LongType,BooleanType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'EKKN'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

schema  = StructType([\
                        StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('EBELN',LongType(),True) ,\
                        StructField('EBELP',IntegerType(),True) ,\
                        StructField('ZEKKN',IntegerType(),True) ,\
                        StructField('LOEKZ',StringType(),True) ,\
                        StructField('AEDAT',StringType(),True) ,\
                        StructField('KFLAG',StringType(),True) ,\
                        StructField('MENGE',DoubleType(),True) ,\
                        StructField('VPROZ',DoubleType(),True) ,\
                        StructField('NETWR',DoubleType(),True) ,\
                        StructField('SAKTO',IntegerType(),True) ,\
                        StructField('GSBER',StringType(),True) ,\
                        StructField('KOSTL',StringType(),True) ,\
                        StructField('PROJN',StringType(),True) ,\
                        StructField('VBELN',StringType(),True) ,\
                        StructField('VBELP',IntegerType(),True) ,\
                        StructField('VETEN',IntegerType(),True) ,\
                        StructField('KZBRB',StringType(),True) ,\
                        StructField('ANLN1',StringType(),True) ,\
                        StructField('ANLN2',StringType(),True) ,\
                        StructField('AUFNR',StringType(),True) ,\
                        StructField('WEMPF',StringType(),True) ,\
                        StructField('ABLAD',StringType(),True) ,\
                        StructField('KOKRS',StringType(),True) ,\
                        StructField('XBKST',StringType(),True) ,\
                        StructField('XBAUF',StringType(),True) ,\
                        StructField('XBPRO',StringType(),True) ,\
                        StructField('EREKZ',StringType(),True) ,\
                        StructField('KSTRG',StringType(),True) ,\
                        StructField('PAOBJNR',IntegerType(),True) ,\
                        StructField('PRCTR',IntegerType(),True) ,\
                        StructField('PS_PSP_PNR',IntegerType(),True) ,\
                        StructField('NPLNR',StringType(),True) ,\
                        StructField('AUFPL',IntegerType(),True) ,\
                        StructField('IMKEY',StringType(),True) ,\
                        StructField('APLZL',IntegerType(),True) ,\
                        StructField('VPTNR',StringType(),True) ,\
                        StructField('FIPOS',StringType(),True) ,\
                        StructField('RECID',StringType(),True) ,\
                        StructField('SERVICE_DOC_TYPE',StringType(),True) ,\
                        StructField('SERVICE_DOC_ID',StringType(),True) ,\
                        StructField('SERVICE_DOC_ITEM_ID',IntegerType(),True) ,\
                        StructField('DUMMY_INCL_EEW_COBL',StringType(),True) ,\
                        StructField('FISTL',StringType(),True) ,\
                        StructField('GEBER',StringType(),True) ,\
                        StructField('FKBER',StringType(),True) ,\
                        StructField('DABRZ',StringType(),True) ,\
                        StructField('AUFPL_ORD',IntegerType(),True) ,\
                        StructField('APLZL_ORD',IntegerType(),True) ,\
                        StructField('MWSKZ',StringType(),True) ,\
                        StructField('TXJCD',StringType(),True) ,\
                        StructField('NAVNW',DoubleType(),True) ,\
                        StructField('KBLNR',StringType(),True) ,\
                        StructField('KBLPOS',IntegerType(),True) ,\
                        StructField('LSTAR',StringType(),True) ,\
                        StructField('PRZNR',StringType(),True) ,\
                        StructField('GRANT_NBR',StringType(),True) ,\
                        StructField('BUDGET_PD',StringType(),True) ,\
                        StructField('FM_SPLIT_BATCH',IntegerType(),True) ,\
                        StructField('FM_SPLIT_BEGRU',StringType(),True) ,\
                        StructField('AA_FINAL_IND',StringType(),True) ,\
                        StructField('AA_FINAL_REASON',StringType(),True) ,\
                        StructField('AA_FINAL_QTY',DoubleType(),True) ,\
                        StructField('AA_FINAL_QTY_F',IntegerType(),True) ,\
                        StructField('MENGE_F',IntegerType(),True) ,\
                        StructField('FMFGUS_KEY',StringType(),True) ,\
                        StructField('_DATAAGING',StringType(),True) ,\
                        StructField('EGRUP',StringType(),True) ,\
                        StructField('VNAME',StringType(),True) ,\
                        StructField('KBLNR_CAB',StringType(),True) ,\
                        StructField('KBLPOS_CAB',IntegerType(),True) ,\
                        StructField('TCOBJNR',StringType(),True) ,\
                        StructField('DATEOFSERVICE',StringType(),True) ,\
                        StructField('NOTAXCORR',StringType(),True) ,\
                        StructField('DIFFOPTRATE',DoubleType(),True) ,\
                        StructField('HASDIFFOPTRATE',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
                     ])

# COMMAND ----------

df_EKKN = spark.read.format(read_format) \
          .option("header", True) \
          .option("delimiter","^") \
          .schema(schema)  \
          .load(read_path)

# COMMAND ----------

df_add_column = df_EKKN.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
                             .withColumn("AEDAT",to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
                             .withColumn("DABRZ",to_date(regexp_replace(df_add_column.DABRZ,'\.','-'))) \
                             .withColumn("_DATAAGING",to_date(regexp_replace(df_add_column._DATAAGING,'\.','-'))) \
                             .withColumn("DATEOFSERVICE",to_date(regexp_replace(df_add_column.DATEOFSERVICE,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.EKKN as S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,EBELN,EBELP,ZEKKN ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_EKKN )A where A.rn = 1 ) as T
# MAGIC ON T.MANDT = S.MANDT and 
# MAGIC T.EBELN = S.EBELN and
# MAGIC T.EBELP = S.EBELP and
# MAGIC T.ZEKKN = S.ZEKKN
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  S.DI_SEQUENCE_NUMBER = T.DI_SEQUENCE_NUMBER,
# MAGIC S.DI_OPERATION_TYPE = T.DI_OPERATION_TYPE,
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.EBELN = T.EBELN,
# MAGIC S.EBELP = T.EBELP,
# MAGIC S.ZEKKN = T.ZEKKN,
# MAGIC S.LOEKZ = T.LOEKZ,
# MAGIC S.AEDAT = T.AEDAT,
# MAGIC S.KFLAG = T.KFLAG,
# MAGIC S.MENGE = T.MENGE,
# MAGIC S.VPROZ = T.VPROZ,
# MAGIC S.NETWR = T.NETWR,
# MAGIC S.SAKTO = T.SAKTO,
# MAGIC S.GSBER = T.GSBER,
# MAGIC S.KOSTL = T.KOSTL,
# MAGIC S.PROJN = T.PROJN,
# MAGIC S.VBELN = T.VBELN,
# MAGIC S.VBELP = T.VBELP,
# MAGIC S.VETEN = T.VETEN,
# MAGIC S.KZBRB = T.KZBRB,
# MAGIC S.ANLN1 = T.ANLN1,
# MAGIC S.ANLN2 = T.ANLN2,
# MAGIC S.AUFNR = T.AUFNR,
# MAGIC S.WEMPF = T.WEMPF,
# MAGIC S.ABLAD = T.ABLAD,
# MAGIC S.KOKRS = T.KOKRS,
# MAGIC S.XBKST = T.XBKST,
# MAGIC S.XBAUF = T.XBAUF,
# MAGIC S.XBPRO = T.XBPRO,
# MAGIC S.EREKZ = T.EREKZ,
# MAGIC S.KSTRG = T.KSTRG,
# MAGIC S.PAOBJNR = T.PAOBJNR,
# MAGIC S.PRCTR = T.PRCTR,
# MAGIC S.PS_PSP_PNR = T.PS_PSP_PNR,
# MAGIC S.NPLNR = T.NPLNR,
# MAGIC S.AUFPL = T.AUFPL,
# MAGIC S.IMKEY = T.IMKEY,
# MAGIC S.APLZL = T.APLZL,
# MAGIC S.VPTNR = T.VPTNR,
# MAGIC S.FIPOS = T.FIPOS,
# MAGIC S.RECID = T.RECID,
# MAGIC S.SERVICE_DOC_TYPE = T.SERVICE_DOC_TYPE,
# MAGIC S.SERVICE_DOC_ID = T.SERVICE_DOC_ID,
# MAGIC S.SERVICE_DOC_ITEM_ID = T.SERVICE_DOC_ITEM_ID,
# MAGIC S.DUMMY_INCL_EEW_COBL = T.DUMMY_INCL_EEW_COBL,
# MAGIC S.FISTL = T.FISTL,
# MAGIC S.GEBER = T.GEBER,
# MAGIC S.FKBER = T.FKBER,
# MAGIC S.DABRZ = T.DABRZ,
# MAGIC S.AUFPL_ORD = T.AUFPL_ORD,
# MAGIC S.APLZL_ORD = T.APLZL_ORD,
# MAGIC S.MWSKZ = T.MWSKZ,
# MAGIC S.TXJCD = T.TXJCD,
# MAGIC S.NAVNW = T.NAVNW,
# MAGIC S.KBLNR = T.KBLNR,
# MAGIC S.KBLPOS = T.KBLPOS,
# MAGIC S.LSTAR = T.LSTAR,
# MAGIC S.PRZNR = T.PRZNR,
# MAGIC S.GRANT_NBR = T.GRANT_NBR,
# MAGIC S.BUDGET_PD = T.BUDGET_PD,
# MAGIC S.FM_SPLIT_BATCH = T.FM_SPLIT_BATCH,
# MAGIC S.FM_SPLIT_BEGRU = T.FM_SPLIT_BEGRU,
# MAGIC S.AA_FINAL_IND = T.AA_FINAL_IND,
# MAGIC S.AA_FINAL_REASON = T.AA_FINAL_REASON,
# MAGIC S.AA_FINAL_QTY = T.AA_FINAL_QTY,
# MAGIC S.AA_FINAL_QTY_F = T.AA_FINAL_QTY_F,
# MAGIC S.MENGE_F = T.MENGE_F,
# MAGIC S.FMFGUS_KEY = T.FMFGUS_KEY,
# MAGIC S._DATAAGING = T._DATAAGING,
# MAGIC S.EGRUP = T.EGRUP,
# MAGIC S.VNAME = T.VNAME,
# MAGIC S.KBLNR_CAB = T.KBLNR_CAB,
# MAGIC S.KBLPOS_CAB = T.KBLPOS_CAB,
# MAGIC S.TCOBJNR = T.TCOBJNR,
# MAGIC S.DATEOFSERVICE = T.DATEOFSERVICE,
# MAGIC S.NOTAXCORR = T.NOTAXCORR,
# MAGIC S.DIFFOPTRATE = T.DIFFOPTRATE,
# MAGIC S.HASDIFFOPTRATE = T.HASDIFFOPTRATE,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = now(),
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED THEN INSERT
# MAGIC (
# MAGIC DI_SEQUENCE_NUMBER,
# MAGIC DI_OPERATION_TYPE,
# MAGIC MANDT,
# MAGIC EBELN,
# MAGIC EBELP,
# MAGIC ZEKKN,
# MAGIC LOEKZ,
# MAGIC AEDAT,
# MAGIC KFLAG,
# MAGIC MENGE,
# MAGIC VPROZ,
# MAGIC NETWR,
# MAGIC SAKTO,
# MAGIC GSBER,
# MAGIC KOSTL,
# MAGIC PROJN,
# MAGIC VBELN,
# MAGIC VBELP,
# MAGIC VETEN,
# MAGIC KZBRB,
# MAGIC ANLN1,
# MAGIC ANLN2,
# MAGIC AUFNR,
# MAGIC WEMPF,
# MAGIC ABLAD,
# MAGIC KOKRS,
# MAGIC XBKST,
# MAGIC XBAUF,
# MAGIC XBPRO,
# MAGIC EREKZ,
# MAGIC KSTRG,
# MAGIC PAOBJNR,
# MAGIC PRCTR,
# MAGIC PS_PSP_PNR,
# MAGIC NPLNR,
# MAGIC AUFPL,
# MAGIC IMKEY,
# MAGIC APLZL,
# MAGIC VPTNR,
# MAGIC FIPOS,
# MAGIC RECID,
# MAGIC SERVICE_DOC_TYPE,
# MAGIC SERVICE_DOC_ID,
# MAGIC SERVICE_DOC_ITEM_ID,
# MAGIC DUMMY_INCL_EEW_COBL,
# MAGIC FISTL,
# MAGIC GEBER,
# MAGIC FKBER,
# MAGIC DABRZ,
# MAGIC AUFPL_ORD,
# MAGIC APLZL_ORD,
# MAGIC MWSKZ,
# MAGIC TXJCD,
# MAGIC NAVNW,
# MAGIC KBLNR,
# MAGIC KBLPOS,
# MAGIC LSTAR,
# MAGIC PRZNR,
# MAGIC GRANT_NBR,
# MAGIC BUDGET_PD,
# MAGIC FM_SPLIT_BATCH,
# MAGIC FM_SPLIT_BEGRU,
# MAGIC AA_FINAL_IND,
# MAGIC AA_FINAL_REASON,
# MAGIC AA_FINAL_QTY,
# MAGIC AA_FINAL_QTY_F,
# MAGIC MENGE_F,
# MAGIC FMFGUS_KEY,
# MAGIC `_DATAAGING`,
# MAGIC EGRUP,
# MAGIC VNAME,
# MAGIC KBLNR_CAB,
# MAGIC KBLPOS_CAB,
# MAGIC TCOBJNR,
# MAGIC DATEOFSERVICE,
# MAGIC NOTAXCORR,
# MAGIC DIFFOPTRATE,
# MAGIC HASDIFFOPTRATE,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.DI_SEQUENCE_NUMBER ,
# MAGIC T.DI_OPERATION_TYPE ,
# MAGIC T.MANDT ,
# MAGIC T.EBELN ,
# MAGIC T.EBELP ,
# MAGIC T.ZEKKN ,
# MAGIC T.LOEKZ ,
# MAGIC T.AEDAT ,
# MAGIC T.KFLAG ,
# MAGIC T.MENGE ,
# MAGIC T.VPROZ ,
# MAGIC T.NETWR ,
# MAGIC T.SAKTO ,
# MAGIC T.GSBER ,
# MAGIC T.KOSTL ,
# MAGIC T.PROJN ,
# MAGIC T.VBELN ,
# MAGIC T.VBELP ,
# MAGIC T.VETEN ,
# MAGIC T.KZBRB ,
# MAGIC T.ANLN1 ,
# MAGIC T.ANLN2 ,
# MAGIC T.AUFNR ,
# MAGIC T.WEMPF ,
# MAGIC T.ABLAD ,
# MAGIC T.KOKRS ,
# MAGIC T.XBKST ,
# MAGIC T.XBAUF ,
# MAGIC T.XBPRO ,
# MAGIC T.EREKZ ,
# MAGIC T.KSTRG ,
# MAGIC T.PAOBJNR ,
# MAGIC T.PRCTR ,
# MAGIC T.PS_PSP_PNR ,
# MAGIC T.NPLNR ,
# MAGIC T.AUFPL ,
# MAGIC T.IMKEY ,
# MAGIC T.APLZL ,
# MAGIC T.VPTNR ,
# MAGIC T.FIPOS ,
# MAGIC T.RECID ,
# MAGIC T.SERVICE_DOC_TYPE ,
# MAGIC T.SERVICE_DOC_ID ,
# MAGIC T.SERVICE_DOC_ITEM_ID ,
# MAGIC T.DUMMY_INCL_EEW_COBL ,
# MAGIC T.FISTL ,
# MAGIC T.GEBER ,
# MAGIC T.FKBER ,
# MAGIC T.DABRZ ,
# MAGIC T.AUFPL_ORD ,
# MAGIC T.APLZL_ORD ,
# MAGIC T.MWSKZ ,
# MAGIC T.TXJCD ,
# MAGIC T.NAVNW ,
# MAGIC T.KBLNR ,
# MAGIC T.KBLPOS ,
# MAGIC T.LSTAR ,
# MAGIC T.PRZNR ,
# MAGIC T.GRANT_NBR ,
# MAGIC T.BUDGET_PD ,
# MAGIC T.FM_SPLIT_BATCH ,
# MAGIC T.FM_SPLIT_BEGRU ,
# MAGIC T.AA_FINAL_IND ,
# MAGIC T.AA_FINAL_REASON ,
# MAGIC T.AA_FINAL_QTY ,
# MAGIC T.AA_FINAL_QTY_F ,
# MAGIC T.MENGE_F ,
# MAGIC T.FMFGUS_KEY ,
# MAGIC T._DATAAGING ,
# MAGIC T.EGRUP ,
# MAGIC T.VNAME ,
# MAGIC T.KBLNR_CAB ,
# MAGIC T.KBLPOS_CAB ,
# MAGIC T.TCOBJNR ,
# MAGIC T.DATEOFSERVICE ,
# MAGIC T.NOTAXCORR ,
# MAGIC T.DIFFOPTRATE ,
# MAGIC T.HASDIFFOPTRATE ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from s42.ekkn;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from s42.ekkn where
# MAGIC ebeln in (
# MAGIC '4500000650','4500000797','4500001326','4500001589','4500001630'  
# MAGIC )
# MAGIC and 
# MAGIC ebelp in ('10');

# COMMAND ----------


