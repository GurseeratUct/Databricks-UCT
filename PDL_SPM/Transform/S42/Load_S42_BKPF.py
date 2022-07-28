# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'BKPF'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
read_path = '/mnt/uct-landing-fin-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-fin-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-fin-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.format(read_format) \
#      .option("header", True) \
#      .option("delimiter",delimiter) \
#      .option('inferSchema',True) \
#      .load(read_path)

# COMMAND ----------

schema = StructType([ StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                       StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('BUKRS',IntegerType(),True) ,\
                        StructField('BELNR',LongType(),True) ,\
                        StructField('GJAHR',IntegerType(),True) ,\
                        StructField('BLART',StringType(),True) ,\
                        StructField('BLDAT',StringType(),True) ,\
                        StructField('BUDAT',StringType(),True) ,\
                        StructField('MONAT',IntegerType(),True) ,\
                        StructField('CPUDT',StringType(),True) ,\
                        StructField('CPUTM',TimestampType(),True) ,\
                        StructField('AEDAT',StringType(),True) ,\
                        StructField('UPDDT',StringType(),True) ,\
                        StructField('WWERT',StringType(),True) ,\
                        StructField('USNAM',StringType(),True) ,\
                        StructField('TCODE',StringType(),True) ,\
                        StructField('BVORG',StringType(),True) ,\
                        StructField('XBLNR',StringType(),True) ,\
                        StructField('DBBLG',StringType(),True) ,\
                        StructField('DBBLG_GJAHR',IntegerType(),True) ,\
                        StructField('DBBLG_BUKRS',StringType(),True) ,\
                        StructField('STBLG',StringType(),True) ,\
                        StructField('STJAH',IntegerType(),True) ,\
                        StructField('BKTXT',StringType(),True) ,\
                        StructField('WAERS',StringType(),True) ,\
                        StructField('KURSF',DoubleType(),True) ,\
                        StructField('KZWRS',StringType(),True) ,\
                        StructField('KZKRS',DoubleType(),True) ,\
                        StructField('BSTAT',StringType(),True) ,\
                        StructField('XNETB',StringType(),True) ,\
                        StructField('FRATH',DoubleType(),True) ,\
                        StructField('XRUEB',StringType(),True) ,\
                        StructField('GLVOR',StringType(),True) ,\
                        StructField('GRPID',StringType(),True) ,\
                        StructField('DOKID',StringType(),True) ,\
                        StructField('ARCID',StringType(),True) ,\
                        StructField('IBLAR',StringType(),True) ,\
                        StructField('AWTYP',StringType(),True) ,\
                        StructField('AWKEY',StringType(),True) ,\
                        StructField('FIKRS',StringType(),True) ,\
                        StructField('HWAER',StringType(),True) ,\
                        StructField('HWAE2',StringType(),True) ,\
                        StructField('HWAE3',StringType(),True) ,\
                        StructField('KURS2',DoubleType(),True) ,\
                        StructField('KURS3',DoubleType(),True) ,\
                        StructField('BASW2',StringType(),True) ,\
                        StructField('BASW3',StringType(),True) ,\
                        StructField('UMRD2',StringType(),True) ,\
                        StructField('UMRD3',StringType(),True) ,\
                        StructField('XSTOV',StringType(),True) ,\
                        StructField('STODT',StringType(),True) ,\
                        StructField('XMWST',StringType(),True) ,\
                        StructField('CURT2',IntegerType(),True) ,\
                        StructField('CURT3',StringType(),True) ,\
                        StructField('KUTY2',StringType(),True) ,\
                        StructField('KUTY3',StringType(),True) ,\
                        StructField('XSNET',StringType(),True) ,\
                        StructField('AUSBK',StringType(),True) ,\
                        StructField('XUSVR',StringType(),True) ,\
                        StructField('DUEFL',StringType(),True) ,\
                        StructField('AWSYS',StringType(),True) ,\
                        StructField('TXKRS',DoubleType(),True) ,\
                        StructField('CTXKRS',DoubleType(),True) ,\
                        StructField('LOTKZ',StringType(),True) ,\
                        StructField('XWVOF',StringType(),True) ,\
                        StructField('STGRD',StringType(),True) ,\
                        StructField('PPNAM',StringType(),True) ,\
                        StructField('PPDAT',StringType(),True) ,\
                        StructField('PPTME',TimestampType(),True) ,\
                        StructField('PPTCOD',StringType(),True) ,\
                        StructField('BRNCH',StringType(),True) ,\
                        StructField('NUMPG',IntegerType(),True) ,\
                        StructField('ADISC',StringType(),True) ,\
                        StructField('XREF1_HD',StringType(),True) ,\
                        StructField('XREF2_HD',StringType(),True) ,\
                        StructField('XREVERSAL',StringType(),True) ,\
                        StructField('REINDAT',StringType(),True) ,\
                        StructField('RLDNR',StringType(),True) ,\
                        StructField('LDGRP',StringType(),True) ,\
                        StructField('PROPMANO',StringType(),True) ,\
                        StructField('XBLNR_ALT',StringType(),True) ,\
                        StructField('VATDATE',StringType(),True) ,\
                        StructField('FULFILLDATE',StringType(),True) ,\
                        StructField('DOCCAT',StringType(),True) ,\
                        StructField('XSPLIT',StringType(),True) ,\
                        StructField('CASH_ALLOC',StringType(),True) ,\
                        StructField('FOLLOW_ON',StringType(),True) ,\
                        StructField('XREORG',StringType(),True) ,\
                        StructField('SUBSET',StringType(),True) ,\
                        StructField('KURST',StringType(),True) ,\
                        StructField('KURSX',DoubleType(),True) ,\
                        StructField('KUR2X',DoubleType(),True) ,\
                        StructField('KUR3X',DoubleType(),True) ,\
                        StructField('XMCA',StringType(),True) ,\
                        StructField('RESUBMISSION',StringType(),True) ,\
                        StructField('LOGSYSTEM_SENDER',StringType(),True) ,\
                        StructField('BUKRS_SENDER',StringType(),True) ,\
                        StructField('BELNR_SENDER',StringType(),True) ,\
                        StructField('GJAHR_SENDER',IntegerType(),True) ,\
                        StructField('INTSUBID',IntegerType(),True) ,\
                        StructField('AWORG_REV',StringType(),True) ,\
                        StructField('AWREF_REV',StringType(),True) ,\
                        StructField('XREVERSING',StringType(),True) ,\
                        StructField('XREVERSED',StringType(),True) ,\
                        StructField('GLBTGRP',StringType(),True) ,\
                        StructField('CO_VRGNG',StringType(),True) ,\
                        StructField('CO_REFBT',StringType(),True) ,\
                        StructField('CO_ALEBN',StringType(),True) ,\
                        StructField('CO_VALDT',StringType(),True) ,\
                        StructField('CO_BELNR_SENDER',StringType(),True) ,\
                        StructField('KOKRS_SENDER',StringType(),True) ,\
                        StructField('ACC_PRINCIPLE',StringType(),True) ,\
                        StructField('_DATAAGING',StringType(),True) ,\
                        StructField('TRAVA_PN',StringType(),True) ,\
                        StructField('LDGRPSPEC_PN',StringType(),True) ,\
                        StructField('AFABESPEC_PN',StringType(),True) ,\
                        StructField('XSECONDARY',StringType(),True) ,\
                        StructField('REPROCESSING_STATUS_CODE',StringType(),True) ,\
                        StructField('TRR_PARTIAL_IND',StringType(),True) ,\
                        StructField('ITEM_REMOVAL_STATUS',StringType(),True) ,\
                        StructField('PENRC',StringType(),True) ,\
                        StructField('GLO_REF1_HD',StringType(),True) ,\
                        StructField('GLO_DAT1_HD',StringType(),True) ,\
                        StructField('GLO_REF2_HD',StringType(),True) ,\
                        StructField('GLO_DAT2_HD',StringType(),True) ,\
                        StructField('GLO_REF3_HD',StringType(),True) ,\
                        StructField('GLO_DAT3_HD',StringType(),True) ,\
                        StructField('GLO_REF4_HD',StringType(),True) ,\
                        StructField('GLO_DAT4_HD',StringType(),True) ,\
                        StructField('GLO_REF5_HD',StringType(),True) ,\
                        StructField('GLO_DAT5_HD',StringType(),True) ,\
                        StructField('GLO_BP1_HD',StringType(),True) ,\
                        StructField('GLO_BP2_HD',StringType(),True) ,\
                        StructField('EV_POSTNG_CTRL',StringType(),True) ,\
                        StructField('ANXTYPE',StringType(),True) ,\
                        StructField('ANXAMNT',DoubleType(),True) ,\
                        StructField('ANXPERC',DoubleType(),True) ,\
                        StructField('ZVAT_INDC',StringType(),True) ,\
                        StructField('/SAPF15/STATUS',StringType(),True) ,\
                        StructField('PSOTY',StringType(),True) ,\
                        StructField('PSOAK',StringType(),True) ,\
                        StructField('PSOKS',StringType(),True) ,\
                        StructField('PSOSG',StringType(),True) ,\
                        StructField('PSOFN',StringType(),True) ,\
                        StructField('INTFORM',StringType(),True) ,\
                        StructField('INTDATE',StringType(),True) ,\
                        StructField('PSOBT',StringType(),True) ,\
                        StructField('PSOZL',StringType(),True) ,\
                        StructField('PSODT',StringType(),True) ,\
                        StructField('PSOTM',TimestampType(),True) ,\
                        StructField('FM_UMART',StringType(),True) ,\
                        StructField('CCINS',StringType(),True) ,\
                        StructField('CCNUM',StringType(),True) ,\
                        StructField('SSBLK',StringType(),True) ,\
                        StructField('BATCH',StringType(),True) ,\
                        StructField('SNAME',StringType(),True) ,\
                        StructField('SAMPLED',StringType(),True) ,\
                        StructField('EXCLUDE_FLAG',StringType(),True) ,\
                        StructField('BLIND',StringType(),True) ,\
                        StructField('OFFSET_STATUS',StringType(),True) ,\
                        StructField('OFFSET_REFER_DAT',StringType(),True) ,\
                        StructField('KNUMV',StringType(),True) ,\
                        StructField('BLO',StringType(),True) ,\
                        StructField('CNT',StringType(),True) ,\
                        StructField('PYBASTYP',StringType(),True) ,\
                        StructField('PYBASNO',StringType(),True) ,\
                        StructField('PYBASDAT',StringType(),True) ,\
                        StructField('PYIBAN',StringType(),True) ,\
                        StructField('INWARDNO_HD',StringType(),True) ,\
                        StructField('INWARDDT_HD',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFiletimestamp',StringType(),True) 
                        ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df = df.drop('DI_SEQUENCE_NUMBER','DI_OPERATION_TYPE')

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('S42'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFiletimestamp", regexp_replace(df_add_column.LandingFiletimestamp,'-','')) \
                            .withColumn("AEDAT", to_date(regexp_replace(df_add_column.AEDAT,"\.","-"))) \
                            .withColumn("BLDAT", to_date(regexp_replace(df_add_column.BLDAT,"\.","-"))) \
                            .withColumn("BUDAT", to_date(regexp_replace(df_add_column.BUDAT,"\.","-"))) \
                            .withColumn("CO_VALDT", to_date(regexp_replace(df_add_column.CO_VALDT,"\.","-"))) \
                            .withColumn("CPUDT", to_date(regexp_replace(df_add_column.CPUDT,"\.","-"))) \
                            .withColumn("FULFILLDATE", to_date(regexp_replace(df_add_column.FULFILLDATE,"\.","-"))) \
                            .withColumn("GLO_DAT1_HD", to_date(regexp_replace(df_add_column.GLO_DAT1_HD,"\.","-"))) \
                            .withColumn("GLO_DAT2_HD", to_date(regexp_replace(df_add_column.GLO_DAT2_HD,"\.","-"))) \
                            .withColumn("GLO_DAT3_HD", to_date(regexp_replace(df_add_column.GLO_DAT3_HD,"\.","-"))) \
                            .withColumn("GLO_DAT4_HD", to_date(regexp_replace(df_add_column.GLO_DAT4_HD,"\.","-"))) \
                            .withColumn("GLO_DAT5_HD", to_date(regexp_replace(df_add_column.GLO_DAT5_HD,"\.","-"))) \
                            .withColumn("INTDATE", to_date(regexp_replace(df_add_column.INTDATE,"\.","-"))) \
                            .withColumn("INWARDDT_HD", to_date(regexp_replace(df_add_column.INWARDDT_HD,"\.","-"))) \
                            .withColumn("OFFSET_REFER_DAT", to_date(regexp_replace(df_add_column.OFFSET_REFER_DAT,"\.","-"))) \
                            .withColumn("PPDAT", to_date(regexp_replace(df_add_column.PPDAT,"\.","-"))) \
                            .withColumn("PSOBT", to_date(regexp_replace(df_add_column.PSOBT,"\.","-"))) \
                            .withColumn("PSODT", to_date(regexp_replace(df_add_column.PSODT,"\.","-"))) \
                            .withColumn("PYBASDAT", to_date(regexp_replace(df_add_column.PYBASDAT,"\.","-"))) \
                            .withColumn("REINDAT", to_date(regexp_replace(df_add_column.REINDAT,"\.","-"))) \
                            .withColumn("RESUBMISSION", to_date(regexp_replace(df_add_column.RESUBMISSION,"\.","-"))) \
                            .withColumn("STODT", to_date(regexp_replace(df_add_column.STODT,"\.","-"))) \
                            .withColumn("UPDDT", to_date(regexp_replace(df_add_column.UPDDT,"\.","-"))) \
                            .withColumn("VATDATE", to_date(regexp_replace(df_add_column.VATDATE,"\.","-"))) \
                            .withColumn("WWERT", to_date(regexp_replace(df_add_column.WWERT,"\.","-"))) \
                            .withColumn("ZVAT_INDC", to_date(regexp_replace(df_add_column.ZVAT_INDC,"\.","-"))) \
                            .withColumn("_DATAAGING", to_date(regexp_replace(df_add_column._DATAAGING,"\.","-"))) 


# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.BKPF as S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY BELNR, BUKRS,GJAHR,MANDT ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_BKPF )A where A.rn = 1 ) as T 
# MAGIC ON S.BELNR = T.BELNR and 
# MAGIC S.BUKRS = T.BUKRS and
# MAGIC S.GJAHR = T.GJAHR and
# MAGIC S.MANDT = T.MANDT 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.BUKRS = T.BUKRS,
# MAGIC S.BELNR = T.BELNR,
# MAGIC S.GJAHR = T.GJAHR,
# MAGIC S.BLART = T.BLART,
# MAGIC S.BLDAT = T.BLDAT,
# MAGIC S.BUDAT = T.BUDAT,
# MAGIC S.MONAT = T.MONAT,
# MAGIC S.CPUDT = T.CPUDT,
# MAGIC S.CPUTM = T.CPUTM,
# MAGIC S.AEDAT = T.AEDAT,
# MAGIC S.UPDDT = T.UPDDT,
# MAGIC S.WWERT = T.WWERT,
# MAGIC S.USNAM = T.USNAM,
# MAGIC S.TCODE = T.TCODE,
# MAGIC S.BVORG = T.BVORG,
# MAGIC S.XBLNR = T.XBLNR,
# MAGIC S.DBBLG = T.DBBLG,
# MAGIC S.DBBLG_GJAHR = T.DBBLG_GJAHR,
# MAGIC S.DBBLG_BUKRS = T.DBBLG_BUKRS,
# MAGIC S.STBLG = T.STBLG,
# MAGIC S.STJAH = T.STJAH,
# MAGIC S.BKTXT = T.BKTXT,
# MAGIC S.WAERS = T.WAERS,
# MAGIC S.KURSF = T.KURSF,
# MAGIC S.KZWRS = T.KZWRS,
# MAGIC S.KZKRS = T.KZKRS,
# MAGIC S.BSTAT = T.BSTAT,
# MAGIC S.XNETB = T.XNETB,
# MAGIC S.FRATH = T.FRATH,
# MAGIC S.XRUEB = T.XRUEB,
# MAGIC S.GLVOR = T.GLVOR,
# MAGIC S.GRPID = T.GRPID,
# MAGIC S.DOKID = T.DOKID,
# MAGIC S.ARCID = T.ARCID,
# MAGIC S.IBLAR = T.IBLAR,
# MAGIC S.AWTYP = T.AWTYP,
# MAGIC S.AWKEY = T.AWKEY,
# MAGIC S.FIKRS = T.FIKRS,
# MAGIC S.HWAER = T.HWAER,
# MAGIC S.HWAE2 = T.HWAE2,
# MAGIC S.HWAE3 = T.HWAE3,
# MAGIC S.KURS2 = T.KURS2,
# MAGIC S.KURS3 = T.KURS3,
# MAGIC S.BASW2 = T.BASW2,
# MAGIC S.BASW3 = T.BASW3,
# MAGIC S.UMRD2 = T.UMRD2,
# MAGIC S.UMRD3 = T.UMRD3,
# MAGIC S.XSTOV = T.XSTOV,
# MAGIC S.STODT = T.STODT,
# MAGIC S.XMWST = T.XMWST,
# MAGIC S.CURT2 = T.CURT2,
# MAGIC S.CURT3 = T.CURT3,
# MAGIC S.KUTY2 = T.KUTY2,
# MAGIC S.KUTY3 = T.KUTY3,
# MAGIC S.XSNET = T.XSNET,
# MAGIC S.AUSBK = T.AUSBK,
# MAGIC S.XUSVR = T.XUSVR,
# MAGIC S.DUEFL = T.DUEFL,
# MAGIC S.AWSYS = T.AWSYS,
# MAGIC S.TXKRS = T.TXKRS,
# MAGIC S.CTXKRS = T.CTXKRS,
# MAGIC S.LOTKZ = T.LOTKZ,
# MAGIC S.XWVOF = T.XWVOF,
# MAGIC S.STGRD = T.STGRD,
# MAGIC S.PPNAM = T.PPNAM,
# MAGIC S.PPDAT = T.PPDAT,
# MAGIC S.PPTME = T.PPTME,
# MAGIC S.PPTCOD = T.PPTCOD,
# MAGIC S.BRNCH = T.BRNCH,
# MAGIC S.NUMPG = T.NUMPG,
# MAGIC S.ADISC = T.ADISC,
# MAGIC S.XREF1_HD = T.XREF1_HD,
# MAGIC S.XREF2_HD = T.XREF2_HD,
# MAGIC S.XREVERSAL = T.XREVERSAL,
# MAGIC S.REINDAT = T.REINDAT,
# MAGIC S.RLDNR = T.RLDNR,
# MAGIC S.LDGRP = T.LDGRP,
# MAGIC S.PROPMANO = T.PROPMANO,
# MAGIC S.XBLNR_ALT = T.XBLNR_ALT,
# MAGIC S.VATDATE = T.VATDATE,
# MAGIC S.FULFILLDATE = T.FULFILLDATE,
# MAGIC S.DOCCAT = T.DOCCAT,
# MAGIC S.XSPLIT = T.XSPLIT,
# MAGIC S.CASH_ALLOC = T.CASH_ALLOC,
# MAGIC S.FOLLOW_ON = T.FOLLOW_ON,
# MAGIC S.XREORG = T.XREORG,
# MAGIC S.SUBSET = T.SUBSET,
# MAGIC S.KURST = T.KURST,
# MAGIC S.KURSX = T.KURSX,
# MAGIC S.KUR2X = T.KUR2X,
# MAGIC S.KUR3X = T.KUR3X,
# MAGIC S.XMCA = T.XMCA,
# MAGIC S.RESUBMISSION = T.RESUBMISSION,
# MAGIC S.LOGSYSTEM_SENDER = T.LOGSYSTEM_SENDER,
# MAGIC S.BUKRS_SENDER = T.BUKRS_SENDER,
# MAGIC S.BELNR_SENDER = T.BELNR_SENDER,
# MAGIC S.GJAHR_SENDER = T.GJAHR_SENDER,
# MAGIC S.INTSUBID = T.INTSUBID,
# MAGIC S.AWORG_REV = T.AWORG_REV,
# MAGIC S.AWREF_REV = T.AWREF_REV,
# MAGIC S.XREVERSING = T.XREVERSING,
# MAGIC S.XREVERSED = T.XREVERSED,
# MAGIC S.GLBTGRP = T.GLBTGRP,
# MAGIC S.CO_VRGNG = T.CO_VRGNG,
# MAGIC S.CO_REFBT = T.CO_REFBT,
# MAGIC S.CO_ALEBN = T.CO_ALEBN,
# MAGIC S.CO_VALDT = T.CO_VALDT,
# MAGIC S.CO_BELNR_SENDER = T.CO_BELNR_SENDER,
# MAGIC S.KOKRS_SENDER = T.KOKRS_SENDER,
# MAGIC S.ACC_PRINCIPLE = T.ACC_PRINCIPLE,
# MAGIC S._DATAAGING = T._DATAAGING,
# MAGIC S.TRAVA_PN = T.TRAVA_PN,
# MAGIC S.LDGRPSPEC_PN = T.LDGRPSPEC_PN,
# MAGIC S.AFABESPEC_PN = T.AFABESPEC_PN,
# MAGIC S.XSECONDARY = T.XSECONDARY,
# MAGIC S.REPROCESSING_STATUS_CODE = T.REPROCESSING_STATUS_CODE,
# MAGIC S.TRR_PARTIAL_IND = T.TRR_PARTIAL_IND,
# MAGIC S.ITEM_REMOVAL_STATUS = T.ITEM_REMOVAL_STATUS,
# MAGIC S.PENRC = T.PENRC,
# MAGIC S.GLO_REF1_HD = T.GLO_REF1_HD,
# MAGIC S.GLO_DAT1_HD = T.GLO_DAT1_HD,
# MAGIC S.GLO_REF2_HD = T.GLO_REF2_HD,
# MAGIC S.GLO_DAT2_HD = T.GLO_DAT2_HD,
# MAGIC S.GLO_REF3_HD = T.GLO_REF3_HD,
# MAGIC S.GLO_DAT3_HD = T.GLO_DAT3_HD,
# MAGIC S.GLO_REF4_HD = T.GLO_REF4_HD,
# MAGIC S.GLO_DAT4_HD = T.GLO_DAT4_HD,
# MAGIC S.GLO_REF5_HD = T.GLO_REF5_HD,
# MAGIC S.GLO_DAT5_HD = T.GLO_DAT5_HD,
# MAGIC S.GLO_BP1_HD = T.GLO_BP1_HD,
# MAGIC S.GLO_BP2_HD = T.GLO_BP2_HD,
# MAGIC S.EV_POSTNG_CTRL = T.EV_POSTNG_CTRL,
# MAGIC S.ANXTYPE = T.ANXTYPE,
# MAGIC S.ANXAMNT = T.ANXAMNT,
# MAGIC S.ANXPERC = T.ANXPERC,
# MAGIC S.ZVAT_INDC = T.ZVAT_INDC,
# MAGIC S.`/SAPF15/STATUS` = T.`/SAPF15/STATUS`,
# MAGIC S.PSOTY = T.PSOTY,
# MAGIC S.PSOAK = T.PSOAK,
# MAGIC S.PSOKS = T.PSOKS,
# MAGIC S.PSOSG = T.PSOSG,
# MAGIC S.PSOFN = T.PSOFN,
# MAGIC S.INTFORM = T.INTFORM,
# MAGIC S.INTDATE = T.INTDATE,
# MAGIC S.PSOBT = T.PSOBT,
# MAGIC S.PSOZL = T.PSOZL,
# MAGIC S.PSODT = T.PSODT,
# MAGIC S.PSOTM = T.PSOTM,
# MAGIC S.FM_UMART = T.FM_UMART,
# MAGIC S.CCINS = T.CCINS,
# MAGIC S.CCNUM = T.CCNUM,
# MAGIC S.SSBLK = T.SSBLK,
# MAGIC S.BATCH = T.BATCH,
# MAGIC S.SNAME = T.SNAME,
# MAGIC S.SAMPLED = T.SAMPLED,
# MAGIC S.EXCLUDE_FLAG = T.EXCLUDE_FLAG,
# MAGIC S.BLIND = T.BLIND,
# MAGIC S.OFFSET_STATUS = T.OFFSET_STATUS,
# MAGIC S.OFFSET_REFER_DAT = T.OFFSET_REFER_DAT,
# MAGIC S.KNUMV = T.KNUMV,
# MAGIC S.BLO = T.BLO,
# MAGIC S.CNT = T.CNT,
# MAGIC S.PYBASTYP = T.PYBASTYP,
# MAGIC S.PYBASNO = T.PYBASNO,
# MAGIC S.PYBASDAT = T.PYBASDAT,
# MAGIC S.PYIBAN = T.PYIBAN,
# MAGIC S.INWARDNO_HD = T.INWARDNO_HD,
# MAGIC S.INWARDDT_HD = T.INWARDDT_HD,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFiletimestamp = T.LandingFiletimestamp,
# MAGIC S.UpdatedOn = now(),
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT
# MAGIC (
# MAGIC MANDT,
# MAGIC BUKRS,
# MAGIC BELNR,
# MAGIC GJAHR,
# MAGIC BLART,
# MAGIC BLDAT,
# MAGIC BUDAT,
# MAGIC MONAT,
# MAGIC CPUDT,
# MAGIC CPUTM,
# MAGIC AEDAT,
# MAGIC UPDDT,
# MAGIC WWERT,
# MAGIC USNAM,
# MAGIC TCODE,
# MAGIC BVORG,
# MAGIC XBLNR,
# MAGIC DBBLG,
# MAGIC DBBLG_GJAHR,
# MAGIC DBBLG_BUKRS,
# MAGIC STBLG,
# MAGIC STJAH,
# MAGIC BKTXT,
# MAGIC WAERS,
# MAGIC KURSF,
# MAGIC KZWRS,
# MAGIC KZKRS,
# MAGIC BSTAT,
# MAGIC XNETB,
# MAGIC FRATH,
# MAGIC XRUEB,
# MAGIC GLVOR,
# MAGIC GRPID,
# MAGIC DOKID,
# MAGIC ARCID,
# MAGIC IBLAR,
# MAGIC AWTYP,
# MAGIC AWKEY,
# MAGIC FIKRS,
# MAGIC HWAER,
# MAGIC HWAE2,
# MAGIC HWAE3,
# MAGIC KURS2,
# MAGIC KURS3,
# MAGIC BASW2,
# MAGIC BASW3,
# MAGIC UMRD2,
# MAGIC UMRD3,
# MAGIC XSTOV,
# MAGIC STODT,
# MAGIC XMWST,
# MAGIC CURT2,
# MAGIC CURT3,
# MAGIC KUTY2,
# MAGIC KUTY3,
# MAGIC XSNET,
# MAGIC AUSBK,
# MAGIC XUSVR,
# MAGIC DUEFL,
# MAGIC AWSYS,
# MAGIC TXKRS,
# MAGIC CTXKRS,
# MAGIC LOTKZ,
# MAGIC XWVOF,
# MAGIC STGRD,
# MAGIC PPNAM,
# MAGIC PPDAT,
# MAGIC PPTME,
# MAGIC PPTCOD,
# MAGIC BRNCH,
# MAGIC NUMPG,
# MAGIC ADISC,
# MAGIC XREF1_HD,
# MAGIC XREF2_HD,
# MAGIC XREVERSAL,
# MAGIC REINDAT,
# MAGIC RLDNR,
# MAGIC LDGRP,
# MAGIC PROPMANO,
# MAGIC XBLNR_ALT,
# MAGIC VATDATE,
# MAGIC FULFILLDATE,
# MAGIC DOCCAT,
# MAGIC XSPLIT,
# MAGIC CASH_ALLOC,
# MAGIC FOLLOW_ON,
# MAGIC XREORG,
# MAGIC SUBSET,
# MAGIC KURST,
# MAGIC KURSX,
# MAGIC KUR2X,
# MAGIC KUR3X,
# MAGIC XMCA,
# MAGIC RESUBMISSION,
# MAGIC LOGSYSTEM_SENDER,
# MAGIC BUKRS_SENDER,
# MAGIC BELNR_SENDER,
# MAGIC GJAHR_SENDER,
# MAGIC INTSUBID,
# MAGIC AWORG_REV,
# MAGIC AWREF_REV,
# MAGIC XREVERSING,
# MAGIC XREVERSED,
# MAGIC GLBTGRP,
# MAGIC CO_VRGNG,
# MAGIC CO_REFBT,
# MAGIC CO_ALEBN,
# MAGIC CO_VALDT,
# MAGIC CO_BELNR_SENDER,
# MAGIC KOKRS_SENDER,
# MAGIC ACC_PRINCIPLE,
# MAGIC _DATAAGING,
# MAGIC TRAVA_PN,
# MAGIC LDGRPSPEC_PN,
# MAGIC AFABESPEC_PN,
# MAGIC XSECONDARY,
# MAGIC REPROCESSING_STATUS_CODE,
# MAGIC TRR_PARTIAL_IND,
# MAGIC ITEM_REMOVAL_STATUS,
# MAGIC PENRC,
# MAGIC GLO_REF1_HD,
# MAGIC GLO_DAT1_HD,
# MAGIC GLO_REF2_HD,
# MAGIC GLO_DAT2_HD,
# MAGIC GLO_REF3_HD,
# MAGIC GLO_DAT3_HD,
# MAGIC GLO_REF4_HD,
# MAGIC GLO_DAT4_HD,
# MAGIC GLO_REF5_HD,
# MAGIC GLO_DAT5_HD,
# MAGIC GLO_BP1_HD,
# MAGIC GLO_BP2_HD,
# MAGIC EV_POSTNG_CTRL,
# MAGIC ANXTYPE,
# MAGIC ANXAMNT,
# MAGIC ANXPERC,
# MAGIC ZVAT_INDC,
# MAGIC `/SAPF15/STATUS`,
# MAGIC PSOTY,
# MAGIC PSOAK,
# MAGIC PSOKS,
# MAGIC PSOSG,
# MAGIC PSOFN,
# MAGIC INTFORM,
# MAGIC INTDATE,
# MAGIC PSOBT,
# MAGIC PSOZL,
# MAGIC PSODT,
# MAGIC PSOTM,
# MAGIC FM_UMART,
# MAGIC CCINS,
# MAGIC CCNUM,
# MAGIC SSBLK,
# MAGIC BATCH,
# MAGIC SNAME,
# MAGIC SAMPLED,
# MAGIC EXCLUDE_FLAG,
# MAGIC BLIND,
# MAGIC OFFSET_STATUS,
# MAGIC OFFSET_REFER_DAT,
# MAGIC KNUMV,
# MAGIC BLO,
# MAGIC CNT,
# MAGIC PYBASTYP,
# MAGIC PYBASNO,
# MAGIC PYBASDAT,
# MAGIC PYIBAN,
# MAGIC INWARDNO_HD,
# MAGIC INWARDDT_HD,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFiletimestamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.MANDT ,
# MAGIC T.BUKRS ,
# MAGIC T.BELNR ,
# MAGIC T.GJAHR ,
# MAGIC T.BLART ,
# MAGIC T.BLDAT ,
# MAGIC T.BUDAT ,
# MAGIC T.MONAT ,
# MAGIC T.CPUDT ,
# MAGIC T.CPUTM ,
# MAGIC T.AEDAT ,
# MAGIC T.UPDDT ,
# MAGIC T.WWERT ,
# MAGIC T.USNAM ,
# MAGIC T.TCODE ,
# MAGIC T.BVORG ,
# MAGIC T.XBLNR ,
# MAGIC T.DBBLG ,
# MAGIC T.DBBLG_GJAHR ,
# MAGIC T.DBBLG_BUKRS ,
# MAGIC T.STBLG ,
# MAGIC T.STJAH ,
# MAGIC T.BKTXT ,
# MAGIC T.WAERS ,
# MAGIC T.KURSF ,
# MAGIC T.KZWRS ,
# MAGIC T.KZKRS ,
# MAGIC T.BSTAT ,
# MAGIC T.XNETB ,
# MAGIC T.FRATH ,
# MAGIC T.XRUEB ,
# MAGIC T.GLVOR ,
# MAGIC T.GRPID ,
# MAGIC T.DOKID ,
# MAGIC T.ARCID ,
# MAGIC T.IBLAR ,
# MAGIC T.AWTYP ,
# MAGIC T.AWKEY ,
# MAGIC T.FIKRS ,
# MAGIC T.HWAER ,
# MAGIC T.HWAE2 ,
# MAGIC T.HWAE3 ,
# MAGIC T.KURS2 ,
# MAGIC T.KURS3 ,
# MAGIC T.BASW2 ,
# MAGIC T.BASW3 ,
# MAGIC T.UMRD2 ,
# MAGIC T.UMRD3 ,
# MAGIC T.XSTOV ,
# MAGIC T.STODT ,
# MAGIC T.XMWST ,
# MAGIC T.CURT2 ,
# MAGIC T.CURT3 ,
# MAGIC T.KUTY2 ,
# MAGIC T.KUTY3 ,
# MAGIC T.XSNET ,
# MAGIC T.AUSBK ,
# MAGIC T.XUSVR ,
# MAGIC T.DUEFL ,
# MAGIC T.AWSYS ,
# MAGIC T.TXKRS ,
# MAGIC T.CTXKRS ,
# MAGIC T.LOTKZ ,
# MAGIC T.XWVOF ,
# MAGIC T.STGRD ,
# MAGIC T.PPNAM ,
# MAGIC T.PPDAT ,
# MAGIC T.PPTME ,
# MAGIC T.PPTCOD ,
# MAGIC T.BRNCH ,
# MAGIC T.NUMPG ,
# MAGIC T.ADISC ,
# MAGIC T.XREF1_HD ,
# MAGIC T.XREF2_HD ,
# MAGIC T.XREVERSAL ,
# MAGIC T.REINDAT ,
# MAGIC T.RLDNR ,
# MAGIC T.LDGRP ,
# MAGIC T.PROPMANO ,
# MAGIC T.XBLNR_ALT ,
# MAGIC T.VATDATE ,
# MAGIC T.FULFILLDATE ,
# MAGIC T.DOCCAT ,
# MAGIC T.XSPLIT ,
# MAGIC T.CASH_ALLOC ,
# MAGIC T.FOLLOW_ON ,
# MAGIC T.XREORG ,
# MAGIC T.SUBSET ,
# MAGIC T.KURST ,
# MAGIC T.KURSX ,
# MAGIC T.KUR2X ,
# MAGIC T.KUR3X ,
# MAGIC T.XMCA ,
# MAGIC T.RESUBMISSION ,
# MAGIC T.LOGSYSTEM_SENDER ,
# MAGIC T.BUKRS_SENDER ,
# MAGIC T.BELNR_SENDER ,
# MAGIC T.GJAHR_SENDER ,
# MAGIC T.INTSUBID ,
# MAGIC T.AWORG_REV ,
# MAGIC T.AWREF_REV ,
# MAGIC T.XREVERSING ,
# MAGIC T.XREVERSED ,
# MAGIC T.GLBTGRP ,
# MAGIC T.CO_VRGNG ,
# MAGIC T.CO_REFBT ,
# MAGIC T.CO_ALEBN ,
# MAGIC T.CO_VALDT ,
# MAGIC T.CO_BELNR_SENDER ,
# MAGIC T.KOKRS_SENDER ,
# MAGIC T.ACC_PRINCIPLE ,
# MAGIC T._DATAAGING ,
# MAGIC T.TRAVA_PN ,
# MAGIC T.LDGRPSPEC_PN ,
# MAGIC T.AFABESPEC_PN ,
# MAGIC T.XSECONDARY ,
# MAGIC T.REPROCESSING_STATUS_CODE ,
# MAGIC T.TRR_PARTIAL_IND ,
# MAGIC T.ITEM_REMOVAL_STATUS ,
# MAGIC T.PENRC ,
# MAGIC T.GLO_REF1_HD ,
# MAGIC T.GLO_DAT1_HD ,
# MAGIC T.GLO_REF2_HD ,
# MAGIC T.GLO_DAT2_HD ,
# MAGIC T.GLO_REF3_HD ,
# MAGIC T.GLO_DAT3_HD ,
# MAGIC T.GLO_REF4_HD ,
# MAGIC T.GLO_DAT4_HD ,
# MAGIC T.GLO_REF5_HD ,
# MAGIC T.GLO_DAT5_HD ,
# MAGIC T.GLO_BP1_HD ,
# MAGIC T.GLO_BP2_HD ,
# MAGIC T.EV_POSTNG_CTRL ,
# MAGIC T.ANXTYPE ,
# MAGIC T.ANXAMNT ,
# MAGIC T.ANXPERC ,
# MAGIC T.ZVAT_INDC ,
# MAGIC T.`/SAPF15/STATUS` ,
# MAGIC T.PSOTY ,
# MAGIC T.PSOAK ,
# MAGIC T.PSOKS ,
# MAGIC T.PSOSG ,
# MAGIC T.PSOFN ,
# MAGIC T.INTFORM ,
# MAGIC T.INTDATE ,
# MAGIC T.PSOBT ,
# MAGIC T.PSOZL ,
# MAGIC T.PSODT ,
# MAGIC T.PSOTM ,
# MAGIC T.FM_UMART ,
# MAGIC T.CCINS ,
# MAGIC T.CCNUM ,
# MAGIC T.SSBLK ,
# MAGIC T.BATCH ,
# MAGIC T.SNAME ,
# MAGIC T.SAMPLED ,
# MAGIC T.EXCLUDE_FLAG ,
# MAGIC T.BLIND ,
# MAGIC T.OFFSET_STATUS ,
# MAGIC T.OFFSET_REFER_DAT ,
# MAGIC T.KNUMV ,
# MAGIC T.BLO ,
# MAGIC T.CNT ,
# MAGIC T.PYBASTYP ,
# MAGIC T.PYBASNO ,
# MAGIC T.PYBASDAT ,
# MAGIC T.PYIBAN ,
# MAGIC T.INWARDNO_HD ,
# MAGIC T.INWARDDT_HD ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFiletimestamp ,
# MAGIC now(),
# MAGIC 'S42'
# MAGIC );

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path,True)

# COMMAND ----------


