# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'KEKO'
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
StructField('BZOBJ',StringType(),True),\
StructField('KALNR',IntegerType(),True),\
StructField('KALKA',IntegerType(),True),\
StructField('KADKY',StringType(),True),\
StructField('TVERS',IntegerType(),True),\
StructField('BWVAR',StringType(),True),\
StructField('KKZMA',StringType(),True),\
StructField('MATNR',StringType(),True),\
StructField('WERKS',IntegerType(),True),\
StructField('BWKEY',IntegerType(),True),\
StructField('BWTAR',StringType(),True),\
StructField('KOKRS',StringType(),True),\
StructField('KADAT',StringType(),True),\
StructField('BIDAT',StringType(),True),\
StructField('KADAM',StringType(),True),\
StructField('BIDAM',StringType(),True),\
StructField('BWDAT',StringType(),True),\
StructField('ALDAT',StringType(),True),\
StructField('BEDAT',StringType(),True),\
StructField('VERID',StringType(),True),\
StructField('STNUM',StringType(),True),\
StructField('STLAN',StringType(),True),\
StructField('STALT',StringType(),True),\
StructField('STCNT',IntegerType(),True),\
StructField('PLNNR',StringType(),True),\
StructField('PLNTY',StringType(),True),\
StructField('PLNAL',StringType(),True),\
StructField('PLNCT',IntegerType(),True),\
StructField('LOEKZ',StringType(),True),\
StructField('LOSGR',DoubleType(),True),\
StructField('MEINS',StringType(),True),\
StructField('ERFNM',StringType(),True),\
StructField('ERFMA',StringType(),True),\
StructField('CPUDT',StringType(),True),\
StructField('CPUDM',StringType(),True),\
StructField('CPUTIME',TimestampType(),True),\
StructField('FEH_ANZ',IntegerType(),True),\
StructField('FEH_K_ANZ',IntegerType(),True),\
StructField('FEH_STA',StringType(),True),\
StructField('MAXMSG',StringType(),True),\
StructField('FREIG',StringType(),True),\
StructField('MKALK',StringType(),True),\
StructField('BALTKZ',StringType(),True),\
StructField('KALNR_BA',IntegerType(),True),\
StructField('BTYP',StringType(),True),\
StructField('MISCH_VERH',DoubleType(),True),\
StructField('BWVAR_BA',StringType(),True),\
StructField('PLSCN',IntegerType(),True),\
StructField('PLMNG',DoubleType(),True),\
StructField('SOBSL',StringType(),True),\
StructField('SOBES',StringType(),True),\
StructField('SOWRK',StringType(),True),\
StructField('SOBWT',StringType(),True),\
StructField('SODIR',StringType(),True),\
StructField('SODUM',StringType(),True),\
StructField('KALSM',StringType(),True),\
StructField('AUFZA',StringType(),True),\
StructField('BWSMR',StringType(),True),\
StructField('SUBSTRAT',StringType(),True),\
StructField('RLDNR',StringType(),True),\
StructField('KLVAR',StringType(),True),\
StructField('KOSGR',StringType(),True),\
StructField('ZSCHL',StringType(),True),\
StructField('POPER',IntegerType(),True),\
StructField('BDATJ',IntegerType(),True),\
StructField('STKOZ',IntegerType(),True),\
StructField('ZAEHL',IntegerType(),True),\
StructField('TOPKA',StringType(),True),\
StructField('CMF_NR',IntegerType(),True),\
StructField('OCS_COUNT',IntegerType(),True),\
StructField('OBJNR',StringType(),True),\
StructField('ERZKA',StringType(),True),\
StructField('LOSAU',DoubleType(),True),\
StructField('AUSID',StringType(),True),\
StructField('AUSSS',DoubleType(),True),\
StructField('SAPRL',StringType(),True),\
StructField('KZROH',StringType(),True),\
StructField('AUFPL',IntegerType(),True),\
StructField('CUOBJ',IntegerType(),True),\
StructField('CUOBJID',StringType(),True),\
StructField('TECHS',StringType(),True),\
StructField('TYPE',StringType(),True),\
StructField('WRKLT',StringType(),True),\
StructField('VORMDAT',StringType(),True),\
StructField('VORMUSR',StringType(),True),\
StructField('FREIDAT',StringType(),True),\
StructField('FREIUSR',StringType(),True),\
StructField('UEBID',StringType(),True),\
StructField('PROZESS',StringType(),True),\
StructField('PR_VERID',StringType(),True),\
StructField('CSPLIT',StringType(),True),\
StructField('KZKUP',StringType(),True),\
StructField('FXPRU',StringType(),True),\
StructField('CFXPR',StringType(),True),\
StructField('ZIFFR',IntegerType(),True),\
StructField('SUMZIFFR',IntegerType(),True),\
StructField('AFAKT',IntegerType(),True),\
StructField('VBELN',StringType(),True),\
StructField('POSNR',IntegerType(),True),\
StructField('PSPNR',IntegerType(),True),\
StructField('SBDKZ',StringType(),True),\
StructField('MLMAA',StringType(),True),\
StructField('BESKZ',StringType(),True),\
StructField('DISST',StringType(),True),\
StructField('KALST',IntegerType(),True),\
StructField('TEMPLATE',StringType(),True),\
StructField('PATNR',IntegerType(),True),\
StructField('PART_VRSN',IntegerType(),True),\
StructField('ELEHK',StringType(),True),\
StructField('ELEHKNS',StringType(),True),\
StructField('VOCNT',IntegerType(),True),\
StructField('GSBER',StringType(),True),\
StructField('PRCTR',StringType(),True),\
StructField('SEGUNIT',StringType(),True),\
StructField('TPVAR',StringType(),True),\
StructField('KURST',StringType(),True),\
StructField('MGTYP',StringType(),True),\
StructField('HWAER',StringType(),True),\
StructField('FWAER_KPF',StringType(),True),\
StructField('REFID',IntegerType(),True),\
StructField('MEINH_WS',StringType(),True),\
StructField('KZWSO',StringType(),True),\
StructField('ASL',StringType(),True),\
StructField('KALAID',StringType(),True),\
StructField('KALADAT',StringType(),True),\
StructField('OTYP',StringType(),True),\
StructField('BVC_SOBSL_USE',StringType(),True),\
StructField('BAPI_CREATED',StringType(),True),\
StructField('SGT_SCAT',StringType(),True),\
StructField('LOGSYSTEM_SENDER',StringType(),True),\
StructField('BZOBJ_SENDER',StringType(),True),\
StructField('KALNR_SENDER',IntegerType(),True),\
StructField('KALKA_SENDER',StringType(),True),\
StructField('KADKY_SENDER',StringType(),True),\
StructField('TVERS_SENDER',IntegerType(),True),\
StructField('BWVAR_SENDER',StringType(),True),\
StructField('KKZMA_SENDER',StringType(),True),\
StructField('/CWM/XCWMAT',StringType(),True),\
StructField('/CWM/LOSGR_BAS',DoubleType(),True),\
StructField('/CWM/MEINH_BAS',StringType(),True),\
StructField('/CWM/LOSGR_BEW',DoubleType(),True),\
StructField('/CWM/MEINH_BEW',StringType(),True),\
StructField('CRULE',StringType(),True),\
StructField('PACKNO',IntegerType(),True),\
StructField('INTROW',IntegerType(),True),\
StructField('BOSFSHT',StringType(),True),\
StructField('BOSDVERSION',IntegerType(),True),\
StructField('GRC_COSTING',StringType(),True),\
StructField('CLINT',IntegerType(),True),\
StructField('PPEGUID',IntegerType(),True),\
StructField('OTYPE',StringType(),True),\
StructField('APPLOBJ_TYPE',StringType(),True),\
StructField('AENNR',StringType(),True),\
StructField('IPPE_COSTING',StringType(),True),\
StructField('CMPEXT_NONCOL',StringType(),True),\
StructField('CCODE',StringType(),True),\
StructField('NONCOL_VERID',StringType(),True),\
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
.withColumn( "KADKY",to_date(regexp_replace(df_add_column.KADKY,'\.','-'))) \
.withColumn( "ALDAT",to_date(regexp_replace(df_add_column.ALDAT,'\.','-'))) \
.withColumn( "BEDAT",to_date(regexp_replace(df_add_column.BEDAT,'\.','-'))) \
.withColumn( "BIDAM",to_date(regexp_replace(df_add_column.BIDAM,'\.','-'))) \
.withColumn( "BIDAT",to_date(regexp_replace(df_add_column.BIDAT,'\.','-'))) \
.withColumn( "BWDAT",to_date(regexp_replace(df_add_column.BWDAT,'\.','-'))) \
.withColumn( "CPUDM",to_date(regexp_replace(df_add_column.CPUDM,'\.','-'))) \
.withColumn( "CPUDT",to_date(regexp_replace(df_add_column.CPUDT,'\.','-'))) \
.withColumn( "FREIDAT",to_date(regexp_replace(df_add_column.FREIDAT,'\.','-'))) \
.withColumn( "KADAM",to_date(regexp_replace(df_add_column.KADAM,'\.','-'))) \
.withColumn( "KADAT",to_date(regexp_replace(df_add_column.KADAT,'\.','-'))) \
.withColumn( "KADKY_SENDER",to_date(regexp_replace(df_add_column.KADKY_SENDER,'\.','-'))) \
.withColumn( "KALADAT",to_date(regexp_replace(df_add_column.KALADAT,'\.','-'))) \
.withColumn( "VORMDAT",to_date(regexp_replace(df_add_column.VORMDAT,'\.','-'))) \
.withColumn("CPUTIME", to_timestamp(regexp_replace(df_add_column.CPUTIME,'\.','-'))) \
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
# MAGIC MERGE INTO S42.KEKO as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY MANDT,BWVAR,BZOBJ,KADKY,KALKA,KALNR,KKZMA,TVERS ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_KEKO)A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and
# MAGIC T.BWVAR = S.BWVAR and
# MAGIC T.BZOBJ = S.BZOBJ and
# MAGIC T.KADKY = S.KADKY and
# MAGIC T.KALKA = S.KALKA and
# MAGIC T.KALNR = S.KALNR and
# MAGIC T.KKZMA = S.KKZMA and
# MAGIC T.TVERS = S.TVERS 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`BZOBJ` =  S.`BZOBJ`,
# MAGIC T.`KALNR` =  S.`KALNR`,
# MAGIC T.`KALKA` =  S.`KALKA`,
# MAGIC T.`KADKY` =  S.`KADKY`,
# MAGIC T.`TVERS` =  S.`TVERS`,
# MAGIC T.`BWVAR` =  S.`BWVAR`,
# MAGIC T.`KKZMA` =  S.`KKZMA`,
# MAGIC T.`MATNR` =  S.`MATNR`,
# MAGIC T.`WERKS` =  S.`WERKS`,
# MAGIC T.`BWKEY` =  S.`BWKEY`,
# MAGIC T.`BWTAR` =  S.`BWTAR`,
# MAGIC T.`KOKRS` =  S.`KOKRS`,
# MAGIC T.`KADAT` =  S.`KADAT`,
# MAGIC T.`BIDAT` =  S.`BIDAT`,
# MAGIC T.`KADAM` =  S.`KADAM`,
# MAGIC T.`BIDAM` =  S.`BIDAM`,
# MAGIC T.`BWDAT` =  S.`BWDAT`,
# MAGIC T.`ALDAT` =  S.`ALDAT`,
# MAGIC T.`BEDAT` =  S.`BEDAT`,
# MAGIC T.`VERID` =  S.`VERID`,
# MAGIC T.`STNUM` =  S.`STNUM`,
# MAGIC T.`STLAN` =  S.`STLAN`,
# MAGIC T.`STALT` =  S.`STALT`,
# MAGIC T.`STCNT` =  S.`STCNT`,
# MAGIC T.`PLNNR` =  S.`PLNNR`,
# MAGIC T.`PLNTY` =  S.`PLNTY`,
# MAGIC T.`PLNAL` =  S.`PLNAL`,
# MAGIC T.`PLNCT` =  S.`PLNCT`,
# MAGIC T.`LOEKZ` =  S.`LOEKZ`,
# MAGIC T.`LOSGR` =  S.`LOSGR`,
# MAGIC T.`MEINS` =  S.`MEINS`,
# MAGIC T.`ERFNM` =  S.`ERFNM`,
# MAGIC T.`ERFMA` =  S.`ERFMA`,
# MAGIC T.`CPUDT` =  S.`CPUDT`,
# MAGIC T.`CPUDM` =  S.`CPUDM`,
# MAGIC T.`CPUTIME` =  S.`CPUTIME`,
# MAGIC T.`FEH_ANZ` =  S.`FEH_ANZ`,
# MAGIC T.`FEH_K_ANZ` =  S.`FEH_K_ANZ`,
# MAGIC T.`FEH_STA` =  S.`FEH_STA`,
# MAGIC T.`MAXMSG` =  S.`MAXMSG`,
# MAGIC T.`FREIG` =  S.`FREIG`,
# MAGIC T.`MKALK` =  S.`MKALK`,
# MAGIC T.`BALTKZ` =  S.`BALTKZ`,
# MAGIC T.`KALNR_BA` =  S.`KALNR_BA`,
# MAGIC T.`BTYP` =  S.`BTYP`,
# MAGIC T.`MISCH_VERH` =  S.`MISCH_VERH`,
# MAGIC T.`BWVAR_BA` =  S.`BWVAR_BA`,
# MAGIC T.`PLSCN` =  S.`PLSCN`,
# MAGIC T.`PLMNG` =  S.`PLMNG`,
# MAGIC T.`SOBSL` =  S.`SOBSL`,
# MAGIC T.`SOBES` =  S.`SOBES`,
# MAGIC T.`SOWRK` =  S.`SOWRK`,
# MAGIC T.`SOBWT` =  S.`SOBWT`,
# MAGIC T.`SODIR` =  S.`SODIR`,
# MAGIC T.`SODUM` =  S.`SODUM`,
# MAGIC T.`KALSM` =  S.`KALSM`,
# MAGIC T.`AUFZA` =  S.`AUFZA`,
# MAGIC T.`BWSMR` =  S.`BWSMR`,
# MAGIC T.`SUBSTRAT` =  S.`SUBSTRAT`,
# MAGIC T.`RLDNR` =  S.`RLDNR`,
# MAGIC T.`KLVAR` =  S.`KLVAR`,
# MAGIC T.`KOSGR` =  S.`KOSGR`,
# MAGIC T.`ZSCHL` =  S.`ZSCHL`,
# MAGIC T.`POPER` =  S.`POPER`,
# MAGIC T.`BDATJ` =  S.`BDATJ`,
# MAGIC T.`STKOZ` =  S.`STKOZ`,
# MAGIC T.`ZAEHL` =  S.`ZAEHL`,
# MAGIC T.`TOPKA` =  S.`TOPKA`,
# MAGIC T.`CMF_NR` =  S.`CMF_NR`,
# MAGIC T.`OCS_COUNT` =  S.`OCS_COUNT`,
# MAGIC T.`OBJNR` =  S.`OBJNR`,
# MAGIC T.`ERZKA` =  S.`ERZKA`,
# MAGIC T.`LOSAU` =  S.`LOSAU`,
# MAGIC T.`AUSID` =  S.`AUSID`,
# MAGIC T.`AUSSS` =  S.`AUSSS`,
# MAGIC T.`SAPRL` =  S.`SAPRL`,
# MAGIC T.`KZROH` =  S.`KZROH`,
# MAGIC T.`AUFPL` =  S.`AUFPL`,
# MAGIC T.`CUOBJ` =  S.`CUOBJ`,
# MAGIC T.`CUOBJID` =  S.`CUOBJID`,
# MAGIC T.`TECHS` =  S.`TECHS`,
# MAGIC T.`TYPE` =  S.`TYPE`,
# MAGIC T.`WRKLT` =  S.`WRKLT`,
# MAGIC T.`VORMDAT` =  S.`VORMDAT`,
# MAGIC T.`VORMUSR` =  S.`VORMUSR`,
# MAGIC T.`FREIDAT` =  S.`FREIDAT`,
# MAGIC T.`FREIUSR` =  S.`FREIUSR`,
# MAGIC T.`UEBID` =  S.`UEBID`,
# MAGIC T.`PROZESS` =  S.`PROZESS`,
# MAGIC T.`PR_VERID` =  S.`PR_VERID`,
# MAGIC T.`CSPLIT` =  S.`CSPLIT`,
# MAGIC T.`KZKUP` =  S.`KZKUP`,
# MAGIC T.`FXPRU` =  S.`FXPRU`,
# MAGIC T.`CFXPR` =  S.`CFXPR`,
# MAGIC T.`ZIFFR` =  S.`ZIFFR`,
# MAGIC T.`SUMZIFFR` =  S.`SUMZIFFR`,
# MAGIC T.`AFAKT` =  S.`AFAKT`,
# MAGIC T.`VBELN` =  S.`VBELN`,
# MAGIC T.`POSNR` =  S.`POSNR`,
# MAGIC T.`PSPNR` =  S.`PSPNR`,
# MAGIC T.`SBDKZ` =  S.`SBDKZ`,
# MAGIC T.`MLMAA` =  S.`MLMAA`,
# MAGIC T.`BESKZ` =  S.`BESKZ`,
# MAGIC T.`DISST` =  S.`DISST`,
# MAGIC T.`KALST` =  S.`KALST`,
# MAGIC T.`TEMPLATE` =  S.`TEMPLATE`,
# MAGIC T.`PATNR` =  S.`PATNR`,
# MAGIC T.`PART_VRSN` =  S.`PART_VRSN`,
# MAGIC T.`ELEHK` =  S.`ELEHK`,
# MAGIC T.`ELEHKNS` =  S.`ELEHKNS`,
# MAGIC T.`VOCNT` =  S.`VOCNT`,
# MAGIC T.`GSBER` =  S.`GSBER`,
# MAGIC T.`PRCTR` =  S.`PRCTR`,
# MAGIC T.`SEGUNIT` =  S.`SEGUNIT`,
# MAGIC T.`TPVAR` =  S.`TPVAR`,
# MAGIC T.`KURST` =  S.`KURST`,
# MAGIC T.`MGTYP` =  S.`MGTYP`,
# MAGIC T.`HWAER` =  S.`HWAER`,
# MAGIC T.`FWAER_KPF` =  S.`FWAER_KPF`,
# MAGIC T.`REFID` =  S.`REFID`,
# MAGIC T.`MEINH_WS` =  S.`MEINH_WS`,
# MAGIC T.`KZWSO` =  S.`KZWSO`,
# MAGIC T.`ASL` =  S.`ASL`,
# MAGIC T.`KALAID` =  S.`KALAID`,
# MAGIC T.`KALADAT` =  S.`KALADAT`,
# MAGIC T.`OTYP` =  S.`OTYP`,
# MAGIC T.`BVC_SOBSL_USE` =  S.`BVC_SOBSL_USE`,
# MAGIC T.`BAPI_CREATED` =  S.`BAPI_CREATED`,
# MAGIC T.`SGT_SCAT` =  S.`SGT_SCAT`,
# MAGIC T.`LOGSYSTEM_SENDER` =  S.`LOGSYSTEM_SENDER`,
# MAGIC T.`BZOBJ_SENDER` =  S.`BZOBJ_SENDER`,
# MAGIC T.`KALNR_SENDER` =  S.`KALNR_SENDER`,
# MAGIC T.`KALKA_SENDER` =  S.`KALKA_SENDER`,
# MAGIC T.`KADKY_SENDER` =  S.`KADKY_SENDER`,
# MAGIC T.`TVERS_SENDER` =  S.`TVERS_SENDER`,
# MAGIC T.`BWVAR_SENDER` =  S.`BWVAR_SENDER`,
# MAGIC T.`KKZMA_SENDER` =  S.`KKZMA_SENDER`,
# MAGIC T.`/CWM/XCWMAT` =  S.`/CWM/XCWMAT`,
# MAGIC T.`/CWM/LOSGR_BAS` =  S.`/CWM/LOSGR_BAS`,
# MAGIC T.`/CWM/MEINH_BAS` =  S.`/CWM/MEINH_BAS`,
# MAGIC T.`/CWM/LOSGR_BEW` =  S.`/CWM/LOSGR_BEW`,
# MAGIC T.`/CWM/MEINH_BEW` =  S.`/CWM/MEINH_BEW`,
# MAGIC T.`CRULE` =  S.`CRULE`,
# MAGIC T.`PACKNO` =  S.`PACKNO`,
# MAGIC T.`INTROW` =  S.`INTROW`,
# MAGIC T.`BOSFSHT` =  S.`BOSFSHT`,
# MAGIC T.`BOSDVERSION` =  S.`BOSDVERSION`,
# MAGIC T.`GRC_COSTING` =  S.`GRC_COSTING`,
# MAGIC T.`CLINT` =  S.`CLINT`,
# MAGIC T.`PPEGUID` =  S.`PPEGUID`,
# MAGIC T.`OTYPE` =  S.`OTYPE`,
# MAGIC T.`APPLOBJ_TYPE` =  S.`APPLOBJ_TYPE`,
# MAGIC T.`AENNR` =  S.`AENNR`,
# MAGIC T.`IPPE_COSTING` =  S.`IPPE_COSTING`,
# MAGIC T.`CMPEXT_NONCOL` =  S.`CMPEXT_NONCOL`,
# MAGIC T.`CCODE` =  S.`CCODE`,
# MAGIC T.`NONCOL_VERID` =  S.`NONCOL_VERID`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `BZOBJ`,
# MAGIC `KALNR`,
# MAGIC `KALKA`,
# MAGIC `KADKY`,
# MAGIC `TVERS`,
# MAGIC `BWVAR`,
# MAGIC `KKZMA`,
# MAGIC `MATNR`,
# MAGIC `WERKS`,
# MAGIC `BWKEY`,
# MAGIC `BWTAR`,
# MAGIC `KOKRS`,
# MAGIC `KADAT`,
# MAGIC `BIDAT`,
# MAGIC `KADAM`,
# MAGIC `BIDAM`,
# MAGIC `BWDAT`,
# MAGIC `ALDAT`,
# MAGIC `BEDAT`,
# MAGIC `VERID`,
# MAGIC `STNUM`,
# MAGIC `STLAN`,
# MAGIC `STALT`,
# MAGIC `STCNT`,
# MAGIC `PLNNR`,
# MAGIC `PLNTY`,
# MAGIC `PLNAL`,
# MAGIC `PLNCT`,
# MAGIC `LOEKZ`,
# MAGIC `LOSGR`,
# MAGIC `MEINS`,
# MAGIC `ERFNM`,
# MAGIC `ERFMA`,
# MAGIC `CPUDT`,
# MAGIC `CPUDM`,
# MAGIC `CPUTIME`,
# MAGIC `FEH_ANZ`,
# MAGIC `FEH_K_ANZ`,
# MAGIC `FEH_STA`,
# MAGIC `MAXMSG`,
# MAGIC `FREIG`,
# MAGIC `MKALK`,
# MAGIC `BALTKZ`,
# MAGIC `KALNR_BA`,
# MAGIC `BTYP`,
# MAGIC `MISCH_VERH`,
# MAGIC `BWVAR_BA`,
# MAGIC `PLSCN`,
# MAGIC `PLMNG`,
# MAGIC `SOBSL`,
# MAGIC `SOBES`,
# MAGIC `SOWRK`,
# MAGIC `SOBWT`,
# MAGIC `SODIR`,
# MAGIC `SODUM`,
# MAGIC `KALSM`,
# MAGIC `AUFZA`,
# MAGIC `BWSMR`,
# MAGIC `SUBSTRAT`,
# MAGIC `RLDNR`,
# MAGIC `KLVAR`,
# MAGIC `KOSGR`,
# MAGIC `ZSCHL`,
# MAGIC `POPER`,
# MAGIC `BDATJ`,
# MAGIC `STKOZ`,
# MAGIC `ZAEHL`,
# MAGIC `TOPKA`,
# MAGIC `CMF_NR`,
# MAGIC `OCS_COUNT`,
# MAGIC `OBJNR`,
# MAGIC `ERZKA`,
# MAGIC `LOSAU`,
# MAGIC `AUSID`,
# MAGIC `AUSSS`,
# MAGIC `SAPRL`,
# MAGIC `KZROH`,
# MAGIC `AUFPL`,
# MAGIC `CUOBJ`,
# MAGIC `CUOBJID`,
# MAGIC `TECHS`,
# MAGIC `TYPE`,
# MAGIC `WRKLT`,
# MAGIC `VORMDAT`,
# MAGIC `VORMUSR`,
# MAGIC `FREIDAT`,
# MAGIC `FREIUSR`,
# MAGIC `UEBID`,
# MAGIC `PROZESS`,
# MAGIC `PR_VERID`,
# MAGIC `CSPLIT`,
# MAGIC `KZKUP`,
# MAGIC `FXPRU`,
# MAGIC `CFXPR`,
# MAGIC `ZIFFR`,
# MAGIC `SUMZIFFR`,
# MAGIC `AFAKT`,
# MAGIC `VBELN`,
# MAGIC `POSNR`,
# MAGIC `PSPNR`,
# MAGIC `SBDKZ`,
# MAGIC `MLMAA`,
# MAGIC `BESKZ`,
# MAGIC `DISST`,
# MAGIC `KALST`,
# MAGIC `TEMPLATE`,
# MAGIC `PATNR`,
# MAGIC `PART_VRSN`,
# MAGIC `ELEHK`,
# MAGIC `ELEHKNS`,
# MAGIC `VOCNT`,
# MAGIC `GSBER`,
# MAGIC `PRCTR`,
# MAGIC `SEGUNIT`,
# MAGIC `TPVAR`,
# MAGIC `KURST`,
# MAGIC `MGTYP`,
# MAGIC `HWAER`,
# MAGIC `FWAER_KPF`,
# MAGIC `REFID`,
# MAGIC `MEINH_WS`,
# MAGIC `KZWSO`,
# MAGIC `ASL`,
# MAGIC `KALAID`,
# MAGIC `KALADAT`,
# MAGIC `OTYP`,
# MAGIC `BVC_SOBSL_USE`,
# MAGIC `BAPI_CREATED`,
# MAGIC `SGT_SCAT`,
# MAGIC `LOGSYSTEM_SENDER`,
# MAGIC `BZOBJ_SENDER`,
# MAGIC `KALNR_SENDER`,
# MAGIC `KALKA_SENDER`,
# MAGIC `KADKY_SENDER`,
# MAGIC `TVERS_SENDER`,
# MAGIC `BWVAR_SENDER`,
# MAGIC `KKZMA_SENDER`,
# MAGIC `/CWM/XCWMAT`,
# MAGIC `/CWM/LOSGR_BAS`,
# MAGIC `/CWM/MEINH_BAS`,
# MAGIC `/CWM/LOSGR_BEW`,
# MAGIC `/CWM/MEINH_BEW`,
# MAGIC `CRULE`,
# MAGIC `PACKNO`,
# MAGIC `INTROW`,
# MAGIC `BOSFSHT`,
# MAGIC `BOSDVERSION`,
# MAGIC `GRC_COSTING`,
# MAGIC `CLINT`,
# MAGIC `PPEGUID`,
# MAGIC `OTYPE`,
# MAGIC `APPLOBJ_TYPE`,
# MAGIC `AENNR`,
# MAGIC `IPPE_COSTING`,
# MAGIC `CMPEXT_NONCOL`,
# MAGIC `CCODE`,
# MAGIC `NONCOL_VERID`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC VALUES
# MAGIC (S.`MANDT`,
# MAGIC S.`BZOBJ`,
# MAGIC S.`KALNR`,
# MAGIC S.`KALKA`,
# MAGIC S.`KADKY`,
# MAGIC S.`TVERS`,
# MAGIC S.`BWVAR`,
# MAGIC S.`KKZMA`,
# MAGIC S.`MATNR`,
# MAGIC S.`WERKS`,
# MAGIC S.`BWKEY`,
# MAGIC S.`BWTAR`,
# MAGIC S.`KOKRS`,
# MAGIC S.`KADAT`,
# MAGIC S.`BIDAT`,
# MAGIC S.`KADAM`,
# MAGIC S.`BIDAM`,
# MAGIC S.`BWDAT`,
# MAGIC S.`ALDAT`,
# MAGIC S.`BEDAT`,
# MAGIC S.`VERID`,
# MAGIC S.`STNUM`,
# MAGIC S.`STLAN`,
# MAGIC S.`STALT`,
# MAGIC S.`STCNT`,
# MAGIC S.`PLNNR`,
# MAGIC S.`PLNTY`,
# MAGIC S.`PLNAL`,
# MAGIC S.`PLNCT`,
# MAGIC S.`LOEKZ`,
# MAGIC S.`LOSGR`,
# MAGIC S.`MEINS`,
# MAGIC S.`ERFNM`,
# MAGIC S.`ERFMA`,
# MAGIC S.`CPUDT`,
# MAGIC S.`CPUDM`,
# MAGIC S.`CPUTIME`,
# MAGIC S.`FEH_ANZ`,
# MAGIC S.`FEH_K_ANZ`,
# MAGIC S.`FEH_STA`,
# MAGIC S.`MAXMSG`,
# MAGIC S.`FREIG`,
# MAGIC S.`MKALK`,
# MAGIC S.`BALTKZ`,
# MAGIC S.`KALNR_BA`,
# MAGIC S.`BTYP`,
# MAGIC S.`MISCH_VERH`,
# MAGIC S.`BWVAR_BA`,
# MAGIC S.`PLSCN`,
# MAGIC S.`PLMNG`,
# MAGIC S.`SOBSL`,
# MAGIC S.`SOBES`,
# MAGIC S.`SOWRK`,
# MAGIC S.`SOBWT`,
# MAGIC S.`SODIR`,
# MAGIC S.`SODUM`,
# MAGIC S.`KALSM`,
# MAGIC S.`AUFZA`,
# MAGIC S.`BWSMR`,
# MAGIC S.`SUBSTRAT`,
# MAGIC S.`RLDNR`,
# MAGIC S.`KLVAR`,
# MAGIC S.`KOSGR`,
# MAGIC S.`ZSCHL`,
# MAGIC S.`POPER`,
# MAGIC S.`BDATJ`,
# MAGIC S.`STKOZ`,
# MAGIC S.`ZAEHL`,
# MAGIC S.`TOPKA`,
# MAGIC S.`CMF_NR`,
# MAGIC S.`OCS_COUNT`,
# MAGIC S.`OBJNR`,
# MAGIC S.`ERZKA`,
# MAGIC S.`LOSAU`,
# MAGIC S.`AUSID`,
# MAGIC S.`AUSSS`,
# MAGIC S.`SAPRL`,
# MAGIC S.`KZROH`,
# MAGIC S.`AUFPL`,
# MAGIC S.`CUOBJ`,
# MAGIC S.`CUOBJID`,
# MAGIC S.`TECHS`,
# MAGIC S.`TYPE`,
# MAGIC S.`WRKLT`,
# MAGIC S.`VORMDAT`,
# MAGIC S.`VORMUSR`,
# MAGIC S.`FREIDAT`,
# MAGIC S.`FREIUSR`,
# MAGIC S.`UEBID`,
# MAGIC S.`PROZESS`,
# MAGIC S.`PR_VERID`,
# MAGIC S.`CSPLIT`,
# MAGIC S.`KZKUP`,
# MAGIC S.`FXPRU`,
# MAGIC S.`CFXPR`,
# MAGIC S.`ZIFFR`,
# MAGIC S.`SUMZIFFR`,
# MAGIC S.`AFAKT`,
# MAGIC S.`VBELN`,
# MAGIC S.`POSNR`,
# MAGIC S.`PSPNR`,
# MAGIC S.`SBDKZ`,
# MAGIC S.`MLMAA`,
# MAGIC S.`BESKZ`,
# MAGIC S.`DISST`,
# MAGIC S.`KALST`,
# MAGIC S.`TEMPLATE`,
# MAGIC S.`PATNR`,
# MAGIC S.`PART_VRSN`,
# MAGIC S.`ELEHK`,
# MAGIC S.`ELEHKNS`,
# MAGIC S.`VOCNT`,
# MAGIC S.`GSBER`,
# MAGIC S.`PRCTR`,
# MAGIC S.`SEGUNIT`,
# MAGIC S.`TPVAR`,
# MAGIC S.`KURST`,
# MAGIC S.`MGTYP`,
# MAGIC S.`HWAER`,
# MAGIC S.`FWAER_KPF`,
# MAGIC S.`REFID`,
# MAGIC S.`MEINH_WS`,
# MAGIC S.`KZWSO`,
# MAGIC S.`ASL`,
# MAGIC S.`KALAID`,
# MAGIC S.`KALADAT`,
# MAGIC S.`OTYP`,
# MAGIC S.`BVC_SOBSL_USE`,
# MAGIC S.`BAPI_CREATED`,
# MAGIC S.`SGT_SCAT`,
# MAGIC S.`LOGSYSTEM_SENDER`,
# MAGIC S.`BZOBJ_SENDER`,
# MAGIC S.`KALNR_SENDER`,
# MAGIC S.`KALKA_SENDER`,
# MAGIC S.`KADKY_SENDER`,
# MAGIC S.`TVERS_SENDER`,
# MAGIC S.`BWVAR_SENDER`,
# MAGIC S.`KKZMA_SENDER`,
# MAGIC S.`/CWM/XCWMAT`,
# MAGIC S.`/CWM/LOSGR_BAS`,
# MAGIC S.`/CWM/MEINH_BAS`,
# MAGIC S.`/CWM/LOSGR_BEW`,
# MAGIC S.`/CWM/MEINH_BEW`,
# MAGIC S.`CRULE`,
# MAGIC S.`PACKNO`,
# MAGIC S.`INTROW`,
# MAGIC S.`BOSFSHT`,
# MAGIC S.`BOSDVERSION`,
# MAGIC S.`GRC_COSTING`,
# MAGIC S.`CLINT`,
# MAGIC S.`PPEGUID`,
# MAGIC S.`OTYPE`,
# MAGIC S.`APPLOBJ_TYPE`,
# MAGIC S.`AENNR`,
# MAGIC S.`IPPE_COSTING`,
# MAGIC S.`CMPEXT_NONCOL`,
# MAGIC S.`CCODE`,
# MAGIC S.`NONCOL_VERID`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path,True)

# COMMAND ----------


