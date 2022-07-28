# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'CKIS'
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
StructField('DI_SEQUENCE_NUMBER',StringType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',IntegerType(),True),\
StructField('LEDNR',IntegerType(),True),\
StructField('BZOBJ',StringType(),True),\
StructField('KALNR',IntegerType(),True),\
StructField('KALKA',IntegerType(),True),\
StructField('KADKY',StringType(),True),\
StructField('TVERS',IntegerType(),True),\
StructField('BWVAR',StringType(),True),\
StructField('KKZMA',StringType(),True),\
StructField('POSNR',IntegerType(),True),\
StructField('STATP',StringType(),True),\
StructField('STRGP',StringType(),True),\
StructField('TYPPS',StringType(),True),\
StructField('KSTAR',StringType(),True),\
StructField('KSTAR_MANUAL',StringType(),True),\
StructField('HRKFT',StringType(),True),\
StructField('ELEMT',IntegerType(),True),\
StructField('ELEMTNS',IntegerType(),True),\
StructField('FVFLG',StringType(),True),\
StructField('OPCOD',StringType(),True),\
StructField('FRLNR',StringType(),True),\
StructField('BUKRS',StringType(),True),\
StructField('WERKS',StringType(),True),\
StructField('MATNR',StringType(),True),\
StructField('PSCHL',StringType(),True),\
StructField('KOKRS_HRK',StringType(),True),\
StructField('EXTNR',StringType(),True),\
StructField('GPREIS',DoubleType(),True),\
StructField('FPREIS',DoubleType(),True),\
StructField('PEINH',IntegerType(),True),\
StructField('PMEHT',StringType(),True),\
StructField('PRICE_MANUAL',StringType(),True),\
StructField('WERTB',DoubleType(),True),\
StructField('WERTN',DoubleType(),True),\
StructField('WRTFX',DoubleType(),True),\
StructField('WRTFW_KPF',DoubleType(),True),\
StructField('WRTFW_KFX',DoubleType(),True),\
StructField('FWAER_KPF',StringType(),True),\
StructField('WRTFW_POS',DoubleType(),True),\
StructField('WRTFW_PFX',DoubleType(),True),\
StructField('FWAER',StringType(),True),\
StructField('MKURS',IntegerType(),True),\
StructField('FWEHT',IntegerType(),True),\
StructField('MENGE',DoubleType(),True),\
StructField('MEEHT',StringType(),True),\
StructField('SUMM1',DoubleType(),True),\
StructField('SUMM2',DoubleType(),True),\
StructField('SUMM3',DoubleType(),True),\
StructField('DPREIS',DoubleType(),True),\
StructField('PREIS1',DoubleType(),True),\
StructField('PREIS2',DoubleType(),True),\
StructField('PREIS3',DoubleType(),True),\
StructField('PREIS4',DoubleType(),True),\
StructField('PREIS5',DoubleType(),True),\
StructField('PFKT1',DoubleType(),True),\
StructField('PFKT2',DoubleType(),True),\
StructField('PFKT3',DoubleType(),True),\
StructField('PFKT4',DoubleType(),True),\
StructField('PFKT5',DoubleType(),True),\
StructField('ZUABS',DoubleType(),True),\
StructField('ZUFKT',DoubleType(),True),\
StructField('PSKNZ',StringType(),True),\
StructField('SBDKZ',StringType(),True),\
StructField('VTKNZ',StringType(),True),\
StructField('LSTAR',StringType(),True),\
StructField('ARBID',IntegerType(),True),\
StructField('KOSTL',StringType(),True),\
StructField('INFNR',StringType(),True),\
StructField('EKORG',StringType(),True),\
StructField('ESOKZ',StringType(),True),\
StructField('LIFNR',StringType(),True),\
StructField('EBELN',StringType(),True),\
StructField('EBELP',IntegerType(),True),\
StructField('STEUS',StringType(),True),\
StructField('FXPRU',StringType(),True),\
StructField('STPOS',IntegerType(),True),\
StructField('AFAKT',IntegerType(),True),\
StructField('BWTAR',StringType(),True),\
StructField('MKALK',StringType(),True),\
StructField('BTYP',StringType(),True),\
StructField('KALNR_BA',IntegerType(),True),\
StructField('MISCH_VERH',DoubleType(),True),\
StructField('UMREZ',IntegerType(),True),\
StructField('UMREN',IntegerType(),True),\
StructField('AUSMG',DoubleType(),True),\
StructField('AUSMGKO',DoubleType(),True),\
StructField('AUSPROZ',DoubleType(),True),\
StructField('APLZL',IntegerType(),True),\
StructField('VORNR',StringType(),True),\
StructField('UVORN',StringType(),True),\
StructField('STEAS',StringType(),True),\
StructField('POSNR_EXT',IntegerType(),True),\
StructField('POINTER1',IntegerType(),True),\
StructField('POINTER2',IntegerType(),True),\
StructField('POINTER3',IntegerType(),True),\
StructField('OPREIS',DoubleType(),True),\
StructField('OPREIFX',DoubleType(),True),\
StructField('TPREIS',DoubleType(),True),\
StructField('TPREIFX',DoubleType(),True),\
StructField('PATNR',IntegerType(),True),\
StructField('VERWS',StringType(),True),\
StructField('PRSKZ',StringType(),True),\
StructField('RLDNR',StringType(),True),\
StructField('STRAT',StringType(),True),\
StructField('SUBSTRAT',StringType(),True),\
StructField('TKURS',DoubleType(),True),\
StructField('SELKZ',StringType(),True),\
StructField('VRGGRP',StringType(),True),\
StructField('KKZMM',StringType(),True),\
StructField('SSEDD',StringType(),True),\
StructField('FEHLKZ',StringType(),True),\
StructField('NO_CCSPLIT',StringType(),True),\
StructField('SCHKZ',StringType(),True),\
StructField('SCHKZNS',StringType(),True),\
StructField('UFIELD_MODE',StringType(),True),\
StructField('UKALN',IntegerType(),True),\
StructField('UKALKA',StringType(),True),\
StructField('UKADKY',StringType(),True),\
StructField('UTVERS',IntegerType(),True),\
StructField('UBWVAR',StringType(),True),\
StructField('HKMAT',StringType(),True),\
StructField('SPOSN',StringType(),True),\
StructField('USTRAT',IntegerType(),True),\
StructField('BAUGR',StringType(),True),\
StructField('ASNUM',StringType(),True),\
StructField('PEINH_2',IntegerType(),True),\
StructField('PEINH_3',IntegerType(),True),\
StructField('PRZNR',StringType(),True),\
StructField('NLAG',StringType(),True),\
StructField('PRCTR',StringType(),True),\
StructField('TPTYP',StringType(),True),\
StructField('SEGUNIT',StringType(),True),\
StructField('GSBER',StringType(),True),\
StructField('PSPNR',IntegerType(),True),\
StructField('PAROB',StringType(),True),\
StructField('KZANW',StringType(),True),\
StructField('KZLB',StringType(),True),\
StructField('SUBKEY',StringType(),True),\
StructField('KZWSO',StringType(),True),\
StructField('DISST',StringType(),True),\
StructField('CUOBJ',IntegerType(),True),\
StructField('GENTYP',StringType(),True),\
StructField('COMPONENT_ID',IntegerType(),True),\
StructField('/CWM/XCWMAT',StringType(),True),\
StructField('/CWM/MENGE_BEW',DoubleType(),True),\
StructField('/CWM/AUSMG_BEW',DoubleType(),True),\
StructField('/CWM/AUSMGKO_BEW',DoubleType(),True),\
StructField('RELATIONSHIP_IND',StringType(),True),\
StructField('MATKL',StringType(),True),\
StructField('PPEGUID',IntegerType(),True),\
StructField('MODEGUID',IntegerType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
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
                            .withColumn("KADKY", to_date(regexp_replace(df_add_column.KADKY,'\.','-'))) \
                            .withColumn("SSEDD", to_date(regexp_replace(df_add_column.SSEDD,'\.','-'))) \
                            .withColumn("STEAS", to_date(regexp_replace(df_add_column.STEAS,'\.','-'))) \
                            .withColumn("UKADKY", to_date(regexp_replace(df_add_column.UKADKY,'\.','-'))) \
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
# MAGIC MERGE INTO S42.CKIS as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,BWVAR,BZOBJ,KADKY,KALKA,KALNR,KKZMA,LEDNR,POSNR,TVERS ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_CKIS where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and 
# MAGIC T.BWVAR = S.BWVAR and
# MAGIC T.BZOBJ = S.BZOBJ and
# MAGIC T.KADKY = S.KADKY and
# MAGIC T.KALKA = S.KALKA and
# MAGIC T.KALNR = S.KALNR and
# MAGIC T.KKZMA = S.KKZMA and
# MAGIC T.LEDNR = S.LEDNR and
# MAGIC T.POSNR = S.POSNR and
# MAGIC T.TVERS = S.TVERS 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`MANDT` =  S.`MANDT`,
# MAGIC T.`LEDNR` =  S.`LEDNR`,
# MAGIC T.`BZOBJ` =  S.`BZOBJ`,
# MAGIC T.`KALNR` =  S.`KALNR`,
# MAGIC T.`KALKA` =  S.`KALKA`,
# MAGIC T.`KADKY` =  S.`KADKY`,
# MAGIC T.`TVERS` =  S.`TVERS`,
# MAGIC T.`BWVAR` =  S.`BWVAR`,
# MAGIC T.`KKZMA` =  S.`KKZMA`,
# MAGIC T.`POSNR` =  S.`POSNR`,
# MAGIC T.`STATP` =  S.`STATP`,
# MAGIC T.`STRGP` =  S.`STRGP`,
# MAGIC T.`TYPPS` =  S.`TYPPS`,
# MAGIC T.`KSTAR` =  S.`KSTAR`,
# MAGIC T.`KSTAR_MANUAL` =  S.`KSTAR_MANUAL`,
# MAGIC T.`HRKFT` =  S.`HRKFT`,
# MAGIC T.`ELEMT` =  S.`ELEMT`,
# MAGIC T.`ELEMTNS` =  S.`ELEMTNS`,
# MAGIC T.`FVFLG` =  S.`FVFLG`,
# MAGIC T.`OPCOD` =  S.`OPCOD`,
# MAGIC T.`FRLNR` =  S.`FRLNR`,
# MAGIC T.`BUKRS` =  S.`BUKRS`,
# MAGIC T.`WERKS` =  S.`WERKS`,
# MAGIC T.`MATNR` =  S.`MATNR`,
# MAGIC T.`PSCHL` =  S.`PSCHL`,
# MAGIC T.`KOKRS_HRK` =  S.`KOKRS_HRK`,
# MAGIC T.`EXTNR` =  S.`EXTNR`,
# MAGIC T.`GPREIS` =  S.`GPREIS`,
# MAGIC T.`FPREIS` =  S.`FPREIS`,
# MAGIC T.`PEINH` =  S.`PEINH`,
# MAGIC T.`PMEHT` =  S.`PMEHT`,
# MAGIC T.`PRICE_MANUAL` =  S.`PRICE_MANUAL`,
# MAGIC T.`WERTB` =  S.`WERTB`,
# MAGIC T.`WERTN` =  S.`WERTN`,
# MAGIC T.`WRTFX` =  S.`WRTFX`,
# MAGIC T.`WRTFW_KPF` =  S.`WRTFW_KPF`,
# MAGIC T.`WRTFW_KFX` =  S.`WRTFW_KFX`,
# MAGIC T.`FWAER_KPF` =  S.`FWAER_KPF`,
# MAGIC T.`WRTFW_POS` =  S.`WRTFW_POS`,
# MAGIC T.`WRTFW_PFX` =  S.`WRTFW_PFX`,
# MAGIC T.`FWAER` =  S.`FWAER`,
# MAGIC T.`MKURS` =  S.`MKURS`,
# MAGIC T.`FWEHT` =  S.`FWEHT`,
# MAGIC T.`MENGE` =  S.`MENGE`,
# MAGIC T.`MEEHT` =  S.`MEEHT`,
# MAGIC T.`SUMM1` =  S.`SUMM1`,
# MAGIC T.`SUMM2` =  S.`SUMM2`,
# MAGIC T.`SUMM3` =  S.`SUMM3`,
# MAGIC T.`DPREIS` =  S.`DPREIS`,
# MAGIC T.`PREIS1` =  S.`PREIS1`,
# MAGIC T.`PREIS2` =  S.`PREIS2`,
# MAGIC T.`PREIS3` =  S.`PREIS3`,
# MAGIC T.`PREIS4` =  S.`PREIS4`,
# MAGIC T.`PREIS5` =  S.`PREIS5`,
# MAGIC T.`PFKT1` =  S.`PFKT1`,
# MAGIC T.`PFKT2` =  S.`PFKT2`,
# MAGIC T.`PFKT3` =  S.`PFKT3`,
# MAGIC T.`PFKT4` =  S.`PFKT4`,
# MAGIC T.`PFKT5` =  S.`PFKT5`,
# MAGIC T.`ZUABS` =  S.`ZUABS`,
# MAGIC T.`ZUFKT` =  S.`ZUFKT`,
# MAGIC T.`PSKNZ` =  S.`PSKNZ`,
# MAGIC T.`SBDKZ` =  S.`SBDKZ`,
# MAGIC T.`VTKNZ` =  S.`VTKNZ`,
# MAGIC T.`LSTAR` =  S.`LSTAR`,
# MAGIC T.`ARBID` =  S.`ARBID`,
# MAGIC T.`KOSTL` =  S.`KOSTL`,
# MAGIC T.`INFNR` =  S.`INFNR`,
# MAGIC T.`EKORG` =  S.`EKORG`,
# MAGIC T.`ESOKZ` =  S.`ESOKZ`,
# MAGIC T.`LIFNR` =  S.`LIFNR`,
# MAGIC T.`EBELN` =  S.`EBELN`,
# MAGIC T.`EBELP` =  S.`EBELP`,
# MAGIC T.`STEUS` =  S.`STEUS`,
# MAGIC T.`FXPRU` =  S.`FXPRU`,
# MAGIC T.`STPOS` =  S.`STPOS`,
# MAGIC T.`AFAKT` =  S.`AFAKT`,
# MAGIC T.`BWTAR` =  S.`BWTAR`,
# MAGIC T.`MKALK` =  S.`MKALK`,
# MAGIC T.`BTYP` =  S.`BTYP`,
# MAGIC T.`KALNR_BA` =  S.`KALNR_BA`,
# MAGIC T.`MISCH_VERH` =  S.`MISCH_VERH`,
# MAGIC T.`UMREZ` =  S.`UMREZ`,
# MAGIC T.`UMREN` =  S.`UMREN`,
# MAGIC T.`AUSMG` =  S.`AUSMG`,
# MAGIC T.`AUSMGKO` =  S.`AUSMGKO`,
# MAGIC T.`AUSPROZ` =  S.`AUSPROZ`,
# MAGIC T.`APLZL` =  S.`APLZL`,
# MAGIC T.`VORNR` =  S.`VORNR`,
# MAGIC T.`UVORN` =  S.`UVORN`,
# MAGIC T.`STEAS` =  S.`STEAS`,
# MAGIC T.`POSNR_EXT` =  S.`POSNR_EXT`,
# MAGIC T.`POINTER1` =  S.`POINTER1`,
# MAGIC T.`POINTER2` =  S.`POINTER2`,
# MAGIC T.`POINTER3` =  S.`POINTER3`,
# MAGIC T.`OPREIS` =  S.`OPREIS`,
# MAGIC T.`OPREIFX` =  S.`OPREIFX`,
# MAGIC T.`TPREIS` =  S.`TPREIS`,
# MAGIC T.`TPREIFX` =  S.`TPREIFX`,
# MAGIC T.`PATNR` =  S.`PATNR`,
# MAGIC T.`VERWS` =  S.`VERWS`,
# MAGIC T.`PRSKZ` =  S.`PRSKZ`,
# MAGIC T.`RLDNR` =  S.`RLDNR`,
# MAGIC T.`STRAT` =  S.`STRAT`,
# MAGIC T.`SUBSTRAT` =  S.`SUBSTRAT`,
# MAGIC T.`TKURS` =  S.`TKURS`,
# MAGIC T.`SELKZ` =  S.`SELKZ`,
# MAGIC T.`VRGGRP` =  S.`VRGGRP`,
# MAGIC T.`KKZMM` =  S.`KKZMM`,
# MAGIC T.`SSEDD` =  S.`SSEDD`,
# MAGIC T.`FEHLKZ` =  S.`FEHLKZ`,
# MAGIC T.`NO_CCSPLIT` =  S.`NO_CCSPLIT`,
# MAGIC T.`SCHKZ` =  S.`SCHKZ`,
# MAGIC T.`SCHKZNS` =  S.`SCHKZNS`,
# MAGIC T.`UFIELD_MODE` =  S.`UFIELD_MODE`,
# MAGIC T.`UKALN` =  S.`UKALN`,
# MAGIC T.`UKALKA` =  S.`UKALKA`,
# MAGIC T.`UKADKY` =  S.`UKADKY`,
# MAGIC T.`UTVERS` =  S.`UTVERS`,
# MAGIC T.`UBWVAR` =  S.`UBWVAR`,
# MAGIC T.`HKMAT` =  S.`HKMAT`,
# MAGIC T.`SPOSN` =  S.`SPOSN`,
# MAGIC T.`USTRAT` =  S.`USTRAT`,
# MAGIC T.`BAUGR` =  S.`BAUGR`,
# MAGIC T.`ASNUM` =  S.`ASNUM`,
# MAGIC T.`PEINH_2` =  S.`PEINH_2`,
# MAGIC T.`PEINH_3` =  S.`PEINH_3`,
# MAGIC T.`PRZNR` =  S.`PRZNR`,
# MAGIC T.`NLAG` =  S.`NLAG`,
# MAGIC T.`PRCTR` =  S.`PRCTR`,
# MAGIC T.`TPTYP` =  S.`TPTYP`,
# MAGIC T.`SEGUNIT` =  S.`SEGUNIT`,
# MAGIC T.`GSBER` =  S.`GSBER`,
# MAGIC T.`PSPNR` =  S.`PSPNR`,
# MAGIC T.`PAROB` =  S.`PAROB`,
# MAGIC T.`KZANW` =  S.`KZANW`,
# MAGIC T.`KZLB` =  S.`KZLB`,
# MAGIC T.`SUBKEY` =  S.`SUBKEY`,
# MAGIC T.`KZWSO` =  S.`KZWSO`,
# MAGIC T.`DISST` =  S.`DISST`,
# MAGIC T.`CUOBJ` =  S.`CUOBJ`,
# MAGIC T.`GENTYP` =  S.`GENTYP`,
# MAGIC T.`COMPONENT_ID` =  S.`COMPONENT_ID`,
# MAGIC T.`/CWM/XCWMAT` =  S.`/CWM/XCWMAT`,
# MAGIC T.`/CWM/MENGE_BEW` =  S.`/CWM/MENGE_BEW`,
# MAGIC T.`/CWM/AUSMG_BEW` =  S.`/CWM/AUSMG_BEW`,
# MAGIC T.`/CWM/AUSMGKO_BEW` =  S.`/CWM/AUSMGKO_BEW`,
# MAGIC T.`RELATIONSHIP_IND` =  S.`RELATIONSHIP_IND`,
# MAGIC T.`MATKL` =  S.`MATKL`,
# MAGIC T.`PPEGUID` =  S.`PPEGUID`,
# MAGIC T.`MODEGUID` =  S.`MODEGUID`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `LEDNR`,
# MAGIC `BZOBJ`,
# MAGIC `KALNR`,
# MAGIC `KALKA`,
# MAGIC `KADKY`,
# MAGIC `TVERS`,
# MAGIC `BWVAR`,
# MAGIC `KKZMA`,
# MAGIC `POSNR`,
# MAGIC `STATP`,
# MAGIC `STRGP`,
# MAGIC `TYPPS`,
# MAGIC `KSTAR`,
# MAGIC `KSTAR_MANUAL`,
# MAGIC `HRKFT`,
# MAGIC `ELEMT`,
# MAGIC `ELEMTNS`,
# MAGIC `FVFLG`,
# MAGIC `OPCOD`,
# MAGIC `FRLNR`,
# MAGIC `BUKRS`,
# MAGIC `WERKS`,
# MAGIC `MATNR`,
# MAGIC `PSCHL`,
# MAGIC `KOKRS_HRK`,
# MAGIC `EXTNR`,
# MAGIC `GPREIS`,
# MAGIC `FPREIS`,
# MAGIC `PEINH`,
# MAGIC `PMEHT`,
# MAGIC `PRICE_MANUAL`,
# MAGIC `WERTB`,
# MAGIC `WERTN`,
# MAGIC `WRTFX`,
# MAGIC `WRTFW_KPF`,
# MAGIC `WRTFW_KFX`,
# MAGIC `FWAER_KPF`,
# MAGIC `WRTFW_POS`,
# MAGIC `WRTFW_PFX`,
# MAGIC `FWAER`,
# MAGIC `MKURS`,
# MAGIC `FWEHT`,
# MAGIC `MENGE`,
# MAGIC `MEEHT`,
# MAGIC `SUMM1`,
# MAGIC `SUMM2`,
# MAGIC `SUMM3`,
# MAGIC `DPREIS`,
# MAGIC `PREIS1`,
# MAGIC `PREIS2`,
# MAGIC `PREIS3`,
# MAGIC `PREIS4`,
# MAGIC `PREIS5`,
# MAGIC `PFKT1`,
# MAGIC `PFKT2`,
# MAGIC `PFKT3`,
# MAGIC `PFKT4`,
# MAGIC `PFKT5`,
# MAGIC `ZUABS`,
# MAGIC `ZUFKT`,
# MAGIC `PSKNZ`,
# MAGIC `SBDKZ`,
# MAGIC `VTKNZ`,
# MAGIC `LSTAR`,
# MAGIC `ARBID`,
# MAGIC `KOSTL`,
# MAGIC `INFNR`,
# MAGIC `EKORG`,
# MAGIC `ESOKZ`,
# MAGIC `LIFNR`,
# MAGIC `EBELN`,
# MAGIC `EBELP`,
# MAGIC `STEUS`,
# MAGIC `FXPRU`,
# MAGIC `STPOS`,
# MAGIC `AFAKT`,
# MAGIC `BWTAR`,
# MAGIC `MKALK`,
# MAGIC `BTYP`,
# MAGIC `KALNR_BA`,
# MAGIC `MISCH_VERH`,
# MAGIC `UMREZ`,
# MAGIC `UMREN`,
# MAGIC `AUSMG`,
# MAGIC `AUSMGKO`,
# MAGIC `AUSPROZ`,
# MAGIC `APLZL`,
# MAGIC `VORNR`,
# MAGIC `UVORN`,
# MAGIC `STEAS`,
# MAGIC `POSNR_EXT`,
# MAGIC `POINTER1`,
# MAGIC `POINTER2`,
# MAGIC `POINTER3`,
# MAGIC `OPREIS`,
# MAGIC `OPREIFX`,
# MAGIC `TPREIS`,
# MAGIC `TPREIFX`,
# MAGIC `PATNR`,
# MAGIC `VERWS`,
# MAGIC `PRSKZ`,
# MAGIC `RLDNR`,
# MAGIC `STRAT`,
# MAGIC `SUBSTRAT`,
# MAGIC `TKURS`,
# MAGIC `SELKZ`,
# MAGIC `VRGGRP`,
# MAGIC `KKZMM`,
# MAGIC `SSEDD`,
# MAGIC `FEHLKZ`,
# MAGIC `NO_CCSPLIT`,
# MAGIC `SCHKZ`,
# MAGIC `SCHKZNS`,
# MAGIC `UFIELD_MODE`,
# MAGIC `UKALN`,
# MAGIC `UKALKA`,
# MAGIC `UKADKY`,
# MAGIC `UTVERS`,
# MAGIC `UBWVAR`,
# MAGIC `HKMAT`,
# MAGIC `SPOSN`,
# MAGIC `USTRAT`,
# MAGIC `BAUGR`,
# MAGIC `ASNUM`,
# MAGIC `PEINH_2`,
# MAGIC `PEINH_3`,
# MAGIC `PRZNR`,
# MAGIC `NLAG`,
# MAGIC `PRCTR`,
# MAGIC `TPTYP`,
# MAGIC `SEGUNIT`,
# MAGIC `GSBER`,
# MAGIC `PSPNR`,
# MAGIC `PAROB`,
# MAGIC `KZANW`,
# MAGIC `KZLB`,
# MAGIC `SUBKEY`,
# MAGIC `KZWSO`,
# MAGIC `DISST`,
# MAGIC `CUOBJ`,
# MAGIC `GENTYP`,
# MAGIC `COMPONENT_ID`,
# MAGIC `/CWM/XCWMAT`,
# MAGIC `/CWM/MENGE_BEW`,
# MAGIC `/CWM/AUSMG_BEW`,
# MAGIC `/CWM/AUSMGKO_BEW`,
# MAGIC `RELATIONSHIP_IND`,
# MAGIC `MATKL`,
# MAGIC `PPEGUID`,
# MAGIC `MODEGUID`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn)
# MAGIC   VALUES 
# MAGIC (
# MAGIC S.`MANDT`,
# MAGIC S.`LEDNR`,
# MAGIC S.`BZOBJ`,
# MAGIC S.`KALNR`,
# MAGIC S.`KALKA`,
# MAGIC S.`KADKY`,
# MAGIC S.`TVERS`,
# MAGIC S.`BWVAR`,
# MAGIC S.`KKZMA`,
# MAGIC S.`POSNR`,
# MAGIC S.`STATP`,
# MAGIC S.`STRGP`,
# MAGIC S.`TYPPS`,
# MAGIC S.`KSTAR`,
# MAGIC S.`KSTAR_MANUAL`,
# MAGIC S.`HRKFT`,
# MAGIC S.`ELEMT`,
# MAGIC S.`ELEMTNS`,
# MAGIC S.`FVFLG`,
# MAGIC S.`OPCOD`,
# MAGIC S.`FRLNR`,
# MAGIC S.`BUKRS`,
# MAGIC S.`WERKS`,
# MAGIC S.`MATNR`,
# MAGIC S.`PSCHL`,
# MAGIC S.`KOKRS_HRK`,
# MAGIC S.`EXTNR`,
# MAGIC S.`GPREIS`,
# MAGIC S.`FPREIS`,
# MAGIC S.`PEINH`,
# MAGIC S.`PMEHT`,
# MAGIC S.`PRICE_MANUAL`,
# MAGIC S.`WERTB`,
# MAGIC S.`WERTN`,
# MAGIC S.`WRTFX`,
# MAGIC S.`WRTFW_KPF`,
# MAGIC S.`WRTFW_KFX`,
# MAGIC S.`FWAER_KPF`,
# MAGIC S.`WRTFW_POS`,
# MAGIC S.`WRTFW_PFX`,
# MAGIC S.`FWAER`,
# MAGIC S.`MKURS`,
# MAGIC S.`FWEHT`,
# MAGIC S.`MENGE`,
# MAGIC S.`MEEHT`,
# MAGIC S.`SUMM1`,
# MAGIC S.`SUMM2`,
# MAGIC S.`SUMM3`,
# MAGIC S.`DPREIS`,
# MAGIC S.`PREIS1`,
# MAGIC S.`PREIS2`,
# MAGIC S.`PREIS3`,
# MAGIC S.`PREIS4`,
# MAGIC S.`PREIS5`,
# MAGIC S.`PFKT1`,
# MAGIC S.`PFKT2`,
# MAGIC S.`PFKT3`,
# MAGIC S.`PFKT4`,
# MAGIC S.`PFKT5`,
# MAGIC S.`ZUABS`,
# MAGIC S.`ZUFKT`,
# MAGIC S.`PSKNZ`,
# MAGIC S.`SBDKZ`,
# MAGIC S.`VTKNZ`,
# MAGIC S.`LSTAR`,
# MAGIC S.`ARBID`,
# MAGIC S.`KOSTL`,
# MAGIC S.`INFNR`,
# MAGIC S.`EKORG`,
# MAGIC S.`ESOKZ`,
# MAGIC S.`LIFNR`,
# MAGIC S.`EBELN`,
# MAGIC S.`EBELP`,
# MAGIC S.`STEUS`,
# MAGIC S.`FXPRU`,
# MAGIC S.`STPOS`,
# MAGIC S.`AFAKT`,
# MAGIC S.`BWTAR`,
# MAGIC S.`MKALK`,
# MAGIC S.`BTYP`,
# MAGIC S.`KALNR_BA`,
# MAGIC S.`MISCH_VERH`,
# MAGIC S.`UMREZ`,
# MAGIC S.`UMREN`,
# MAGIC S.`AUSMG`,
# MAGIC S.`AUSMGKO`,
# MAGIC S.`AUSPROZ`,
# MAGIC S.`APLZL`,
# MAGIC S.`VORNR`,
# MAGIC S.`UVORN`,
# MAGIC S.`STEAS`,
# MAGIC S.`POSNR_EXT`,
# MAGIC S.`POINTER1`,
# MAGIC S.`POINTER2`,
# MAGIC S.`POINTER3`,
# MAGIC S.`OPREIS`,
# MAGIC S.`OPREIFX`,
# MAGIC S.`TPREIS`,
# MAGIC S.`TPREIFX`,
# MAGIC S.`PATNR`,
# MAGIC S.`VERWS`,
# MAGIC S.`PRSKZ`,
# MAGIC S.`RLDNR`,
# MAGIC S.`STRAT`,
# MAGIC S.`SUBSTRAT`,
# MAGIC S.`TKURS`,
# MAGIC S.`SELKZ`,
# MAGIC S.`VRGGRP`,
# MAGIC S.`KKZMM`,
# MAGIC S.`SSEDD`,
# MAGIC S.`FEHLKZ`,
# MAGIC S.`NO_CCSPLIT`,
# MAGIC S.`SCHKZ`,
# MAGIC S.`SCHKZNS`,
# MAGIC S.`UFIELD_MODE`,
# MAGIC S.`UKALN`,
# MAGIC S.`UKALKA`,
# MAGIC S.`UKADKY`,
# MAGIC S.`UTVERS`,
# MAGIC S.`UBWVAR`,
# MAGIC S.`HKMAT`,
# MAGIC S.`SPOSN`,
# MAGIC S.`USTRAT`,
# MAGIC S.`BAUGR`,
# MAGIC S.`ASNUM`,
# MAGIC S.`PEINH_2`,
# MAGIC S.`PEINH_3`,
# MAGIC S.`PRZNR`,
# MAGIC S.`NLAG`,
# MAGIC S.`PRCTR`,
# MAGIC S.`TPTYP`,
# MAGIC S.`SEGUNIT`,
# MAGIC S.`GSBER`,
# MAGIC S.`PSPNR`,
# MAGIC S.`PAROB`,
# MAGIC S.`KZANW`,
# MAGIC S.`KZLB`,
# MAGIC S.`SUBKEY`,
# MAGIC S.`KZWSO`,
# MAGIC S.`DISST`,
# MAGIC S.`CUOBJ`,
# MAGIC S.`GENTYP`,
# MAGIC S.`COMPONENT_ID`,
# MAGIC S.`/CWM/XCWMAT`,
# MAGIC S.`/CWM/MENGE_BEW`,
# MAGIC S.`/CWM/AUSMG_BEW`,
# MAGIC S.`/CWM/AUSMGKO_BEW`,
# MAGIC S.`RELATIONSHIP_IND`,
# MAGIC S.`MATKL`,
# MAGIC S.`PPEGUID`,
# MAGIC S.`MODEGUID`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'SAP',
# MAGIC now()
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path,True)

# COMMAND ----------


