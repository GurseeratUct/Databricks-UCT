# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

schema = StructType([ \
                     	StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
	StructField('DI_OPERATION_TYPE',StringType(),True),\
	StructField('MANDT',IntegerType(),True),\
	StructField('BUKRS',StringType(),True),\
	StructField('BUTXT',StringType(),True),\
	StructField('ORT01',StringType(),True),\
	StructField('LAND1',StringType(),True),\
	StructField('WAERS',StringType(),True),\
	StructField('SPRAS',StringType(),True),\
	StructField('KTOPL',StringType(),True),\
	StructField('WAABW',StringType(),True),\
	StructField('PERIV',StringType(),True),\
	StructField('KOKFI',StringType(),True),\
	StructField('RCOMP',StringType(),True),\
	StructField('ADRNR',StringType(),True),\
	StructField('STCEG',StringType(),True),\
	StructField('FIKRS',StringType(),True),\
	StructField('XFMCO',StringType(),True),\
	StructField('XFMCB',StringType(),True),\
	StructField('XFMCA',StringType(),True),\
	StructField('TXJCD',StringType(),True),\
	StructField('FMHRDATE',StringType(),True),\
	StructField('XTEMPLT',StringType(),True),\
	StructField('BUVAR',StringType(),True),\
	StructField('FDBUK',StringType(),True),\
	StructField('XFDIS',StringType(),True),\
	StructField('XVALV',StringType(),True),\
	StructField('XSKFN',StringType(),True),\
	StructField('KKBER',StringType(),True),\
	StructField('XMWSN',StringType(),True),\
	StructField('MREGL',StringType(),True),\
	StructField('XGSBE',StringType(),True),\
	StructField('XGJRV',StringType(),True),\
	StructField('XKDFT',StringType(),True),\
	StructField('XPROD',StringType(),True),\
	StructField('XEINK',StringType(),True),\
	StructField('XJVAA',StringType(),True),\
	StructField('XVVWA',StringType(),True),\
	StructField('XSLTA',StringType(),True),\
	StructField('XFDMM',StringType(),True),\
	StructField('XFDSD',StringType(),True),\
	StructField('XEXTB',StringType(),True),\
	StructField('EBUKR',StringType(),True),\
	StructField('KTOP2',StringType(),True),\
	StructField('UMKRS',StringType(),True),\
	StructField('BUKRS_GLOB',StringType(),True),\
	StructField('FSTVA',StringType(),True),\
	StructField('OPVAR',StringType(),True),\
	StructField('XCOVR',StringType(),True),\
	StructField('TXKRS',StringType(),True),\
	StructField('WFVAR',StringType(),True),\
	StructField('XBBBF',StringType(),True),\
	StructField('XBBBE',StringType(),True),\
	StructField('XBBBA',StringType(),True),\
	StructField('XBBKO',StringType(),True),\
	StructField('XSTDT',StringType(),True),\
	StructField('MWSKV',StringType(),True),\
	StructField('MWSKA',StringType(),True),\
	StructField('IMPDA',StringType(),True),\
	StructField('XNEGP',StringType(),True),\
	StructField('XKKBI',StringType(),True),\
	StructField('WT_NEWWT',StringType(),True),\
	StructField('PP_PDATE',StringType(),True),\
	StructField('INFMT',StringType(),True),\
	StructField('FSTVARE',StringType(),True),\
	StructField('KOPIM',StringType(),True),\
	StructField('DKWEG',StringType(),True),\
	StructField('OFFSACCT',IntegerType(),True),\
	StructField('BAPOVAR',StringType(),True),\
	StructField('XCOS',StringType(),True),\
	StructField('XCESSION',StringType(),True),\
	StructField('XSPLT',StringType(),True),\
	StructField('SURCCM',StringType(),True),\
	StructField('DTPROV',StringType(),True),\
	StructField('DTAMTC',StringType(),True),\
	StructField('DTTAXC',StringType(),True),\
	StructField('DTTDSP',StringType(),True),\
	StructField('DTAXR',StringType(),True),\
	StructField('XVATDATE',StringType(),True),\
	StructField('PST_PER_VAR',StringType(),True),\
	StructField('XBBSC',StringType(),True),\
	StructField('F_OBSOLETE',StringType(),True),\
	StructField('FM_DERIVE_ACC',StringType(),True),\
	StructField('ODQ_CHANGEMODE',StringType(),True),\
	StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
	StructField('LandingFileTimeStamp',StringType(),True)\
                    ])

# COMMAND ----------

table_name = 'T001'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

df_T001 = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter","^") \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

#df_rename = df_T001.withColumnRenamed('TimeStamp','LandingTimeStamp') #column rename LandingFileTimeStamp
df_add_column = df_T001.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

# COMMAND ----------

df_transform = df_add_column.withColumn("FMHRDATE", regexp_replace(df_add_column.FMHRDATE, '\.', '-')) \
                            .withColumn("FMHRDATE", to_date(col("FMHRDATE"))) \
                            .withColumn("LandingFileTimeStamp", regexp_replace(regexp_replace(df_add_column.LandingFileTimeStamp, 'T001/T001_', ''),'-','')) \
                            
                           

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.T001 
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,BUKRS ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T001 where MANDT = '100')A where A.rn = 1 ) as stg 
# MAGIC ON T001.MANDT = stg.MANDT and 
# MAGIC T001.BUKRS = stg.BUKRS
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC   BUTXT = stg.BUTXT,
# MAGIC   ORT01 = stg.ORT01,
# MAGIC   LAND1 = stg.LAND1,
# MAGIC   WAERS = stg.WAERS,
# MAGIC   SPRAS = stg.SPRAS,
# MAGIC   KTOPL = stg.KTOPL,
# MAGIC   WAABW = stg.WAABW,
# MAGIC   PERIV = stg.PERIV,
# MAGIC   KOKFI = stg.KOKFI,
# MAGIC   RCOMP = stg.RCOMP,
# MAGIC   ADRNR = stg.ADRNR,
# MAGIC   STCEG = stg.STCEG,
# MAGIC   FIKRS = stg.FIKRS,
# MAGIC   XFMCO = stg.XFMCO,
# MAGIC   XFMCB = stg.XFMCB,
# MAGIC   XFMCA = stg.XFMCA,
# MAGIC   TXJCD = stg.TXJCD,
# MAGIC   FMHRDATE = stg.FMHRDATE,
# MAGIC   XTEMPLT = stg.XTEMPLT,
# MAGIC   BUVAR = stg.BUVAR,
# MAGIC   FDBUK = stg.FDBUK,
# MAGIC   XFDIS = stg.XFDIS,
# MAGIC   XVALV = stg.XVALV,
# MAGIC   XSKFN = stg.XSKFN,
# MAGIC   KKBER = stg.KKBER,
# MAGIC   XMWSN = stg.XMWSN,
# MAGIC   MREGL = stg.MREGL,
# MAGIC   XGSBE = stg.XGSBE,
# MAGIC   XGJRV = stg.XGJRV,
# MAGIC   XKDFT = stg.XKDFT,
# MAGIC   XPROD = stg.XPROD,
# MAGIC   XEINK = stg.XEINK,
# MAGIC   XJVAA = stg.XJVAA,
# MAGIC   XVVWA = stg.XVVWA,
# MAGIC   XSLTA = stg.XSLTA,
# MAGIC   XFDMM = stg.XFDMM,
# MAGIC   XFDSD = stg.XFDSD,
# MAGIC   XEXTB = stg.XEXTB,
# MAGIC   EBUKR = stg.EBUKR,
# MAGIC   KTOP2 = stg.KTOP2,
# MAGIC   UMKRS = stg.UMKRS,
# MAGIC   BUKRS_GLOB = stg.BUKRS_GLOB,
# MAGIC   FSTVA = stg.FSTVA,
# MAGIC   OPVAR = stg.OPVAR,
# MAGIC   XCOVR = stg.XCOVR,
# MAGIC   TXKRS = stg.TXKRS,
# MAGIC   WFVAR = stg.WFVAR,
# MAGIC   XBBBF = stg.XBBBF,
# MAGIC   XBBBE = stg.XBBBE,
# MAGIC   XBBBA = stg.XBBBA,
# MAGIC   XBBKO = stg.XBBKO,
# MAGIC   XSTDT = stg.XSTDT,
# MAGIC   MWSKV = stg.MWSKV,
# MAGIC   MWSKA = stg.MWSKA,
# MAGIC   IMPDA = stg.IMPDA,
# MAGIC   XNEGP = stg.XNEGP,
# MAGIC   XKKBI = stg.XKKBI,
# MAGIC   WT_NEWWT = stg.WT_NEWWT,
# MAGIC   PP_PDATE = stg.PP_PDATE,
# MAGIC   INFMT = stg.INFMT,
# MAGIC   FSTVARE = stg.FSTVARE,
# MAGIC   KOPIM = stg.KOPIM,
# MAGIC   DKWEG = stg.DKWEG,
# MAGIC   OFFSACCT = stg.OFFSACCT,
# MAGIC   BAPOVAR = stg.BAPOVAR,
# MAGIC   XCOS = stg.XCOS,
# MAGIC   XCESSION = stg.XCESSION,
# MAGIC   XSPLT = stg.XSPLT,
# MAGIC   SURCCM = stg.SURCCM,
# MAGIC   DTPROV = stg.DTPROV,
# MAGIC   DTAMTC = stg.DTAMTC,
# MAGIC   DTTAXC = stg.DTTAXC,
# MAGIC   DTTDSP = stg.DTTDSP,
# MAGIC   DTAXR = stg.DTAXR,
# MAGIC   XVATDATE = stg.XVATDATE,
# MAGIC   PST_PER_VAR = stg.PST_PER_VAR,
# MAGIC   XBBSC = stg.XBBSC,
# MAGIC   F_OBSOLETE = stg.F_OBSOLETE,
# MAGIC   FM_DERIVE_ACC = stg.FM_DERIVE_ACC,
# MAGIC   ODQ_CHANGEMODE = stg.ODQ_CHANGEMODE,
# MAGIC   ODQ_ENTITYCNTR = stg.ODQ_ENTITYCNTR,
# MAGIC   LandingFileTimeStamp = stg.LandingFileTimeStamp,
# MAGIC   UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC   MANDT,
# MAGIC   BUKRS,
# MAGIC   BUTXT,
# MAGIC   ORT01,
# MAGIC   LAND1,
# MAGIC   WAERS,
# MAGIC   SPRAS,
# MAGIC   KTOPL,
# MAGIC   WAABW,
# MAGIC   PERIV,
# MAGIC   KOKFI,
# MAGIC   RCOMP,
# MAGIC   ADRNR,
# MAGIC   STCEG,
# MAGIC   FIKRS,
# MAGIC   XFMCO,
# MAGIC   XFMCB,
# MAGIC   XFMCA,
# MAGIC   TXJCD,
# MAGIC   FMHRDATE,
# MAGIC   XTEMPLT,
# MAGIC   BUVAR,
# MAGIC   FDBUK,
# MAGIC   XFDIS,
# MAGIC   XVALV,
# MAGIC   XSKFN,
# MAGIC   KKBER,
# MAGIC   XMWSN,
# MAGIC   MREGL,
# MAGIC   XGSBE,
# MAGIC   XGJRV,
# MAGIC   XKDFT,
# MAGIC   XPROD,
# MAGIC   XEINK,
# MAGIC   XJVAA,
# MAGIC   XVVWA,
# MAGIC   XSLTA,
# MAGIC   XFDMM,
# MAGIC   XFDSD,
# MAGIC   XEXTB,
# MAGIC   EBUKR,
# MAGIC   KTOP2,
# MAGIC   UMKRS,
# MAGIC   BUKRS_GLOB,
# MAGIC   FSTVA,
# MAGIC   OPVAR,
# MAGIC   XCOVR,
# MAGIC   TXKRS,
# MAGIC   WFVAR,
# MAGIC   XBBBF,
# MAGIC   XBBBE,
# MAGIC   XBBBA,
# MAGIC   XBBKO,
# MAGIC   XSTDT,
# MAGIC   MWSKV,
# MAGIC   MWSKA,
# MAGIC   IMPDA,
# MAGIC   XNEGP,
# MAGIC   XKKBI,
# MAGIC   WT_NEWWT,
# MAGIC   PP_PDATE,
# MAGIC   INFMT,
# MAGIC   FSTVARE,
# MAGIC   KOPIM,
# MAGIC   DKWEG,
# MAGIC   OFFSACCT,
# MAGIC   BAPOVAR,
# MAGIC   XCOS,
# MAGIC   XCESSION,
# MAGIC   XSPLT,
# MAGIC   SURCCM,
# MAGIC   DTPROV,
# MAGIC   DTAMTC,
# MAGIC   DTTAXC,
# MAGIC   DTTDSP,
# MAGIC   DTAXR,
# MAGIC   XVATDATE,
# MAGIC   PST_PER_VAR,
# MAGIC   XBBSC,
# MAGIC   F_OBSOLETE,
# MAGIC   FM_DERIVE_ACC,
# MAGIC   ODQ_CHANGEMODE,
# MAGIC   ODQ_ENTITYCNTR,
# MAGIC   LandingFileTimeStamp,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC   )
# MAGIC   VALUES (
# MAGIC   stg.MANDT,
# MAGIC   stg.BUKRS,
# MAGIC   stg.BUTXT,
# MAGIC   stg.ORT01,
# MAGIC   stg.LAND1,
# MAGIC   stg.WAERS,
# MAGIC   stg.SPRAS,
# MAGIC   stg.KTOPL,
# MAGIC   stg.WAABW,
# MAGIC   stg.PERIV,
# MAGIC   stg.KOKFI,
# MAGIC   stg.RCOMP,
# MAGIC   stg.ADRNR,
# MAGIC   stg.STCEG,
# MAGIC   stg.FIKRS,
# MAGIC   stg.XFMCO,
# MAGIC   stg.XFMCB,
# MAGIC   stg.XFMCA,
# MAGIC   stg.TXJCD,
# MAGIC   stg.FMHRDATE,
# MAGIC   stg.XTEMPLT,
# MAGIC   stg.BUVAR,
# MAGIC   stg.FDBUK,
# MAGIC   stg.XFDIS,
# MAGIC   stg.XVALV,
# MAGIC   stg.XSKFN,
# MAGIC   stg.KKBER,
# MAGIC   stg.XMWSN,
# MAGIC   stg.MREGL,
# MAGIC   stg.XGSBE,
# MAGIC   stg.XGJRV,
# MAGIC   stg.XKDFT,
# MAGIC   stg.XPROD,
# MAGIC   stg.XEINK,
# MAGIC   stg.XJVAA,
# MAGIC   stg.XVVWA,
# MAGIC   stg.XSLTA,
# MAGIC   stg.XFDMM,
# MAGIC   stg.XFDSD,
# MAGIC   stg.XEXTB,
# MAGIC   stg.EBUKR,
# MAGIC   stg.KTOP2,
# MAGIC   stg.UMKRS,
# MAGIC   stg.BUKRS_GLOB,
# MAGIC   stg.FSTVA,
# MAGIC   stg.OPVAR,
# MAGIC   stg.XCOVR,
# MAGIC   stg.TXKRS,
# MAGIC   stg.WFVAR,
# MAGIC   stg.XBBBF,
# MAGIC   stg.XBBBE,
# MAGIC   stg.XBBBA,
# MAGIC   stg.XBBKO,
# MAGIC   stg.XSTDT,
# MAGIC   stg.MWSKV,
# MAGIC   stg.MWSKA,
# MAGIC   stg.IMPDA,
# MAGIC   stg.XNEGP,
# MAGIC   stg.XKKBI,
# MAGIC   stg.WT_NEWWT,
# MAGIC   stg.PP_PDATE,
# MAGIC   stg.INFMT,
# MAGIC   stg.FSTVARE,
# MAGIC   stg.KOPIM,
# MAGIC   stg.DKWEG,
# MAGIC   stg.OFFSACCT,
# MAGIC   stg.BAPOVAR,
# MAGIC   stg.XCOS,
# MAGIC   stg.XCESSION,
# MAGIC   stg.XSPLT,
# MAGIC   stg.SURCCM,
# MAGIC   stg.DTPROV,
# MAGIC   stg.DTAMTC,
# MAGIC   stg.DTTAXC,
# MAGIC   stg.DTTDSP,
# MAGIC   stg.DTAXR,
# MAGIC   stg.XVATDATE,
# MAGIC   stg.PST_PER_VAR,
# MAGIC   stg.XBBSC,
# MAGIC   stg.F_OBSOLETE,
# MAGIC   stg.FM_DERIVE_ACC,
# MAGIC   stg.ODQ_CHANGEMODE,
# MAGIC   stg.ODQ_ENTITYCNTR,
# MAGIC   stg.LandingFileTimeStamp,
# MAGIC   'SAP',
# MAGIC   now()
# MAGIC   )
# MAGIC          

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


