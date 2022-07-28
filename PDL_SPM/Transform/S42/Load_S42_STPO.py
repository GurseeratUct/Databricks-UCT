# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'STPO'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.options(header='True', inferShema='True', delimiter='^').csv(read_path)

# COMMAND ----------

schema = StructType([ \
StructField('DI_SEQUENCE_NUMBER',StringType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',StringType(),True),\
StructField('STLTY',StringType(),True),\
StructField('STLNR',StringType(),True),\
StructField('STLKN',StringType(),True),\
StructField('STPOZ',StringType(),True),\
StructField('DATUV',StringType(),True),\
StructField('TECHV',StringType(),True),\
StructField('AENNR',StringType(),True),\
StructField('LKENZ',StringType(),True),\
StructField('VGKNT',StringType(),True),\
StructField('VGPZL',StringType(),True),\
StructField('ANDAT',StringType(),True),\
StructField('ANNAM',StringType(),True),\
StructField('AEDAT',StringType(),True),\
StructField('AENAM',StringType(),True),\
StructField('IDNRK',StringType(),True),\
StructField('PSWRK',StringType(),True),\
StructField('POSTP',StringType(),True),\
StructField('POSNR',StringType(),True),\
StructField('SORTF',StringType(),True),\
StructField('MEINS',StringType(),True),\
StructField('MENGE',StringType(),True),\
StructField('FMENG',StringType(),True),\
StructField('AUSCH',StringType(),True),\
StructField('AVOAU',StringType(),True),\
StructField('NETAU',StringType(),True),\
StructField('SCHGT',StringType(),True),\
StructField('BEIKZ',StringType(),True),\
StructField('ERSKZ',StringType(),True),\
StructField('RVREL',StringType(),True),\
StructField('SANFE',StringType(),True),\
StructField('SANIN',StringType(),True),\
StructField('SANKA',StringType(),True),\
StructField('SANKO',StringType(),True),\
StructField('SANVS',StringType(),True),\
StructField('STKKZ',StringType(),True),\
StructField('REKRI',StringType(),True),\
StructField('REKRS',StringType(),True),\
StructField('CADPO',StringType(),True),\
StructField('NFMAT',StringType(),True),\
StructField('NLFZT',StringType(),True),\
StructField('VERTI',StringType(),True),\
StructField('ALPOS',StringType(),True),\
StructField('EWAHR',StringType(),True),\
StructField('EKGRP',StringType(),True),\
StructField('LIFZT',StringType(),True),\
StructField('LIFNR',StringType(),True),\
StructField('PREIS',StringType(),True),\
StructField('PEINH',StringType(),True),\
StructField('WAERS',StringType(),True),\
StructField('SAKTO',StringType(),True),\
StructField('ROANZ',StringType(),True),\
StructField('ROMS1',StringType(),True),\
StructField('ROMS2',StringType(),True),\
StructField('ROMS3',StringType(),True),\
StructField('ROMEI',StringType(),True),\
StructField('ROMEN',StringType(),True),\
StructField('RFORM',StringType(),True),\
StructField('UPSKZ',StringType(),True),\
StructField('VALKZ',StringType(),True),\
StructField('LTXSP',StringType(),True),\
StructField('POTX1',StringType(),True),\
StructField('POTX2',StringType(),True),\
StructField('OBJTY',StringType(),True),\
StructField('MATKL',StringType(),True),\
StructField('WEBAZ',StringType(),True),\
StructField('DOKAR',StringType(),True),\
StructField('DOKNR',StringType(),True),\
StructField('DOKVR',StringType(),True),\
StructField('DOKTL',StringType(),True),\
StructField('CSSTR',StringType(),True),\
StructField('CLASS',StringType(),True),\
StructField('KLART',StringType(),True),\
StructField('POTPR',StringType(),True),\
StructField('AWAKZ',StringType(),True),\
StructField('INSKZ',StringType(),True),\
StructField('VCEKZ',StringType(),True),\
StructField('VSTKZ',StringType(),True),\
StructField('VACKZ',StringType(),True),\
StructField('EKORG',StringType(),True),\
StructField('CLOBK',StringType(),True),\
StructField('CLMUL',StringType(),True),\
StructField('CLALT',StringType(),True),\
StructField('CVIEW',StringType(),True),\
StructField('KNOBJ',StringType(),True),\
StructField('LGORT',StringType(),True),\
StructField('KZKUP',StringType(),True),\
StructField('INTRM',StringType(),True),\
StructField('TPEKZ',StringType(),True),\
StructField('STVKN',StringType(),True),\
StructField('DVDAT',StringType(),True),\
StructField('DVNAM',StringType(),True),\
StructField('DSPST',StringType(),True),\
StructField('ALPST',StringType(),True),\
StructField('ALPRF',StringType(),True),\
StructField('ALPGR',StringType(),True),\
StructField('KZNFP',StringType(),True),\
StructField('NFGRP',StringType(),True),\
StructField('NFEAG',StringType(),True),\
StructField('KNDVB',StringType(),True),\
StructField('KNDBZ',StringType(),True),\
StructField('KSTTY',StringType(),True),\
StructField('KSTNR',StringType(),True),\
StructField('KSTKN',StringType(),True),\
StructField('KSTPZ',StringType(),True),\
StructField('CLSZU',StringType(),True),\
StructField('KZCLB',StringType(),True),\
StructField('AEHLP',StringType(),True),\
StructField('PRVBE',StringType(),True),\
StructField('NLFZV',StringType(),True),\
StructField('NLFMV',StringType(),True),\
StructField('IDPOS',StringType(),True),\
StructField('IDHIS',StringType(),True),\
StructField('IDVAR',StringType(),True),\
StructField('ALEKZ',StringType(),True),\
StructField('ITMID',StringType(),True),\
StructField('GUID',StringType(),True),\
StructField('ITSOB',StringType(),True),\
StructField('RFPNT',StringType(),True),\
StructField('GUIDX',StringType(),True),\
StructField('SGT_CMKZ',StringType(),True),\
StructField('SGT_CATV',StringType(),True),\
StructField('VALID_TO',StringType(),True),\
StructField('VALID_TO_RKEY',StringType(),True),\
StructField('ECN_TO',StringType(),True),\
StructField('ECN_TO_RKEY',StringType(),True),\
StructField('ABLAD',StringType(),True),\
StructField('WEMPF',StringType(),True),\
StructField('STVKN_VERSN',StringType(),True),\
StructField('LASTCHANGEDATETIME',StringType(),True),\
StructField('SFWIND',StringType(),True),\
StructField('DUMMY_STPO_INCL_EEW_PS',StringType(),True),\
StructField('CUFACTOR',StringType(),True),\
StructField('/SAPMP/MET_LRCH',StringType(),True),\
StructField('/SAPMP/MAX_FERTL',StringType(),True),\
StructField('/SAPMP/FIX_AS_J',StringType(),True),\
StructField('/SAPMP/FIX_AS_E',StringType(),True),\
StructField('/SAPMP/FIX_AS_L',StringType(),True),\
StructField('/SAPMP/ABL_ZAHL',StringType(),True),\
StructField('/SAPMP/RUND_FAKT',StringType(),True),\
StructField('FSH_VMKZ',StringType(),True),\
StructField('FSH_PGQR',StringType(),True),\
StructField('FSH_PGQRRF',StringType(),True),\
StructField('FSH_CRITICAL_COMP',StringType(),True),\
StructField('FSH_CRITICAL_LEVEL',StringType(),True),\
StructField('FUNCID',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',StringType(),True),\
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
.withColumn( "DATUV",to_date(regexp_replace(df_add_column.DATUV,'\.','-'))) \
.withColumn( "ANDAT",to_date(regexp_replace(df_add_column.ANDAT,'\.','-'))) \
.withColumn( "AEDAT",to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
.withColumn( "DVDAT",to_date(regexp_replace(df_add_column.DVDAT,'\.','-'))) \
.withColumn( "VALID_TO",to_date(regexp_replace(df_add_column.VALID_TO,'\.','-'))) \
.withColumn( "VALID_TO_RKEY",to_date(regexp_replace(df_add_column.VALID_TO_RKEY,'\.','-'))) \
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
# MAGIC MERGE INTO S42.STPO as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY MANDT,STLTY,STLNR,STLKN,STPOZ ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_STPO)A where A.rn = 1 ) as S 
# MAGIC ON T.`MANDT`	 = S.`MANDT`
# MAGIC and T.`STLTY`	 = S.`STLTY`
# MAGIC and T.`STLNR`	 = S.`STLNR`
# MAGIC and T.`STLKN`	 = S.`STLKN`
# MAGIC and T.`STPOZ`	 = S.`STPOZ`
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`MANDT`	 = S.`MANDT`,
# MAGIC T.`STLTY`	 = S.`STLTY`,
# MAGIC T.`STLNR`	 = S.`STLNR`,
# MAGIC T.`STLKN`	 = S.`STLKN`,
# MAGIC T.`STPOZ`	 = S.`STPOZ`,
# MAGIC T.`DATUV`	 = S.`DATUV`,
# MAGIC T.`TECHV`	 = S.`TECHV`,
# MAGIC T.`AENNR`	 = S.`AENNR`,
# MAGIC T.`LKENZ`	 = S.`LKENZ`,
# MAGIC T.`VGKNT`	 = S.`VGKNT`,
# MAGIC T.`VGPZL`	 = S.`VGPZL`,
# MAGIC T.`ANDAT`	 = S.`ANDAT`,
# MAGIC T.`ANNAM`	 = S.`ANNAM`,
# MAGIC T.`AEDAT`	 = S.`AEDAT`,
# MAGIC T.`AENAM`	 = S.`AENAM`,
# MAGIC T.`IDNRK`	 = S.`IDNRK`,
# MAGIC T.`PSWRK`	 = S.`PSWRK`,
# MAGIC T.`POSTP`	 = S.`POSTP`,
# MAGIC T.`POSNR`	 = S.`POSNR`,
# MAGIC T.`SORTF`	 = S.`SORTF`,
# MAGIC T.`MEINS`	 = S.`MEINS`,
# MAGIC T.`MENGE`	 = S.`MENGE`,
# MAGIC T.`FMENG`	 = S.`FMENG`,
# MAGIC T.`AUSCH`	 = S.`AUSCH`,
# MAGIC T.`AVOAU`	 = S.`AVOAU`,
# MAGIC T.`NETAU`	 = S.`NETAU`,
# MAGIC T.`SCHGT`	 = S.`SCHGT`,
# MAGIC T.`BEIKZ`	 = S.`BEIKZ`,
# MAGIC T.`ERSKZ`	 = S.`ERSKZ`,
# MAGIC T.`RVREL`	 = S.`RVREL`,
# MAGIC T.`SANFE`	 = S.`SANFE`,
# MAGIC T.`SANIN`	 = S.`SANIN`,
# MAGIC T.`SANKA`	 = S.`SANKA`,
# MAGIC T.`SANKO`	 = S.`SANKO`,
# MAGIC T.`SANVS`	 = S.`SANVS`,
# MAGIC T.`STKKZ`	 = S.`STKKZ`,
# MAGIC T.`REKRI`	 = S.`REKRI`,
# MAGIC T.`REKRS`	 = S.`REKRS`,
# MAGIC T.`CADPO`	 = S.`CADPO`,
# MAGIC T.`NFMAT`	 = S.`NFMAT`,
# MAGIC T.`NLFZT`	 = S.`NLFZT`,
# MAGIC T.`VERTI`	 = S.`VERTI`,
# MAGIC T.`ALPOS`	 = S.`ALPOS`,
# MAGIC T.`EWAHR`	 = S.`EWAHR`,
# MAGIC T.`EKGRP`	 = S.`EKGRP`,
# MAGIC T.`LIFZT`	 = S.`LIFZT`,
# MAGIC T.`LIFNR`	 = S.`LIFNR`,
# MAGIC T.`PREIS`	 = S.`PREIS`,
# MAGIC T.`PEINH`	 = S.`PEINH`,
# MAGIC T.`WAERS`	 = S.`WAERS`,
# MAGIC T.`SAKTO`	 = S.`SAKTO`,
# MAGIC T.`ROANZ`	 = S.`ROANZ`,
# MAGIC T.`ROMS1`	 = S.`ROMS1`,
# MAGIC T.`ROMS2`	 = S.`ROMS2`,
# MAGIC T.`ROMS3`	 = S.`ROMS3`,
# MAGIC T.`ROMEI`	 = S.`ROMEI`,
# MAGIC T.`ROMEN`	 = S.`ROMEN`,
# MAGIC T.`RFORM`	 = S.`RFORM`,
# MAGIC T.`UPSKZ`	 = S.`UPSKZ`,
# MAGIC T.`VALKZ`	 = S.`VALKZ`,
# MAGIC T.`LTXSP`	 = S.`LTXSP`,
# MAGIC T.`POTX1`	 = S.`POTX1`,
# MAGIC T.`POTX2`	 = S.`POTX2`,
# MAGIC T.`OBJTY`	 = S.`OBJTY`,
# MAGIC T.`MATKL`	 = S.`MATKL`,
# MAGIC T.`WEBAZ`	 = S.`WEBAZ`,
# MAGIC T.`DOKAR`	 = S.`DOKAR`,
# MAGIC T.`DOKNR`	 = S.`DOKNR`,
# MAGIC T.`DOKVR`	 = S.`DOKVR`,
# MAGIC T.`DOKTL`	 = S.`DOKTL`,
# MAGIC T.`CSSTR`	 = S.`CSSTR`,
# MAGIC T.`CLASS`	 = S.`CLASS`,
# MAGIC T.`KLART`	 = S.`KLART`,
# MAGIC T.`POTPR`	 = S.`POTPR`,
# MAGIC T.`AWAKZ`	 = S.`AWAKZ`,
# MAGIC T.`INSKZ`	 = S.`INSKZ`,
# MAGIC T.`VCEKZ`	 = S.`VCEKZ`,
# MAGIC T.`VSTKZ`	 = S.`VSTKZ`,
# MAGIC T.`VACKZ`	 = S.`VACKZ`,
# MAGIC T.`EKORG`	 = S.`EKORG`,
# MAGIC T.`CLOBK`	 = S.`CLOBK`,
# MAGIC T.`CLMUL`	 = S.`CLMUL`,
# MAGIC T.`CLALT`	 = S.`CLALT`,
# MAGIC T.`CVIEW`	 = S.`CVIEW`,
# MAGIC T.`KNOBJ`	 = S.`KNOBJ`,
# MAGIC T.`LGORT`	 = S.`LGORT`,
# MAGIC T.`KZKUP`	 = S.`KZKUP`,
# MAGIC T.`INTRM`	 = S.`INTRM`,
# MAGIC T.`TPEKZ`	 = S.`TPEKZ`,
# MAGIC T.`STVKN`	 = S.`STVKN`,
# MAGIC T.`DVDAT`	 = S.`DVDAT`,
# MAGIC T.`DVNAM`	 = S.`DVNAM`,
# MAGIC T.`DSPST`	 = S.`DSPST`,
# MAGIC T.`ALPST`	 = S.`ALPST`,
# MAGIC T.`ALPRF`	 = S.`ALPRF`,
# MAGIC T.`ALPGR`	 = S.`ALPGR`,
# MAGIC T.`KZNFP`	 = S.`KZNFP`,
# MAGIC T.`NFGRP`	 = S.`NFGRP`,
# MAGIC T.`NFEAG`	 = S.`NFEAG`,
# MAGIC T.`KNDVB`	 = S.`KNDVB`,
# MAGIC T.`KNDBZ`	 = S.`KNDBZ`,
# MAGIC T.`KSTTY`	 = S.`KSTTY`,
# MAGIC T.`KSTNR`	 = S.`KSTNR`,
# MAGIC T.`KSTKN`	 = S.`KSTKN`,
# MAGIC T.`KSTPZ`	 = S.`KSTPZ`,
# MAGIC T.`CLSZU`	 = S.`CLSZU`,
# MAGIC T.`KZCLB`	 = S.`KZCLB`,
# MAGIC T.`AEHLP`	 = S.`AEHLP`,
# MAGIC T.`PRVBE`	 = S.`PRVBE`,
# MAGIC T.`NLFZV`	 = S.`NLFZV`,
# MAGIC T.`NLFMV`	 = S.`NLFMV`,
# MAGIC T.`IDPOS`	 = S.`IDPOS`,
# MAGIC T.`IDHIS`	 = S.`IDHIS`,
# MAGIC T.`IDVAR`	 = S.`IDVAR`,
# MAGIC T.`ALEKZ`	 = S.`ALEKZ`,
# MAGIC T.`ITMID`	 = S.`ITMID`,
# MAGIC T.`GUID`	 = S.`GUID`,
# MAGIC T.`ITSOB`	 = S.`ITSOB`,
# MAGIC T.`RFPNT`	 = S.`RFPNT`,
# MAGIC T.`GUIDX`	 = S.`GUIDX`,
# MAGIC T.`SGT_CMKZ`	 = S.`SGT_CMKZ`,
# MAGIC T.`SGT_CATV`	 = S.`SGT_CATV`,
# MAGIC T.`VALID_TO`	 = S.`VALID_TO`,
# MAGIC T.`VALID_TO_RKEY`	 = S.`VALID_TO_RKEY`,
# MAGIC T.`ECN_TO`	 = S.`ECN_TO`,
# MAGIC T.`ECN_TO_RKEY`	 = S.`ECN_TO_RKEY`,
# MAGIC T.`ABLAD`	 = S.`ABLAD`,
# MAGIC T.`WEMPF`	 = S.`WEMPF`,
# MAGIC T.`STVKN_VERSN`	 = S.`STVKN_VERSN`,
# MAGIC T.`LASTCHANGEDATETIME`	 = S.`LASTCHANGEDATETIME`,
# MAGIC T.`SFWIND`	 = S.`SFWIND`,
# MAGIC T.`DUMMY_STPO_INCL_EEW_PS`	 = S.`DUMMY_STPO_INCL_EEW_PS`,
# MAGIC T.`CUFACTOR`	 = S.`CUFACTOR`,
# MAGIC T.`/SAPMP/MET_LRCH`	 = S.`/SAPMP/MET_LRCH`,
# MAGIC T.`/SAPMP/MAX_FERTL`	 = S.`/SAPMP/MAX_FERTL`,
# MAGIC T.`/SAPMP/FIX_AS_J`	 = S.`/SAPMP/FIX_AS_J`,
# MAGIC T.`/SAPMP/FIX_AS_E`	 = S.`/SAPMP/FIX_AS_E`,
# MAGIC T.`/SAPMP/FIX_AS_L`	 = S.`/SAPMP/FIX_AS_L`,
# MAGIC T.`/SAPMP/ABL_ZAHL`	 = S.`/SAPMP/ABL_ZAHL`,
# MAGIC T.`/SAPMP/RUND_FAKT`	 = S.`/SAPMP/RUND_FAKT`,
# MAGIC T.`FSH_VMKZ`	 = S.`FSH_VMKZ`,
# MAGIC T.`FSH_PGQR`	 = S.`FSH_PGQR`,
# MAGIC T.`FSH_PGQRRF`	 = S.`FSH_PGQRRF`,
# MAGIC T.`FSH_CRITICAL_COMP`	 = S.`FSH_CRITICAL_COMP`,
# MAGIC T.`FSH_CRITICAL_LEVEL`	 = S.`FSH_CRITICAL_LEVEL`,
# MAGIC T.`FUNCID`	 = S.`FUNCID`,
# MAGIC T.`ODQ_CHANGEMODE`	 = S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR`	 = S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp`	 = S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `STLTY`,
# MAGIC `STLNR`,
# MAGIC `STLKN`,
# MAGIC `STPOZ`,
# MAGIC `DATUV`,
# MAGIC `TECHV`,
# MAGIC `AENNR`,
# MAGIC `LKENZ`,
# MAGIC `VGKNT`,
# MAGIC `VGPZL`,
# MAGIC `ANDAT`,
# MAGIC `ANNAM`,
# MAGIC `AEDAT`,
# MAGIC `AENAM`,
# MAGIC `IDNRK`,
# MAGIC `PSWRK`,
# MAGIC `POSTP`,
# MAGIC `POSNR`,
# MAGIC `SORTF`,
# MAGIC `MEINS`,
# MAGIC `MENGE`,
# MAGIC `FMENG`,
# MAGIC `AUSCH`,
# MAGIC `AVOAU`,
# MAGIC `NETAU`,
# MAGIC `SCHGT`,
# MAGIC `BEIKZ`,
# MAGIC `ERSKZ`,
# MAGIC `RVREL`,
# MAGIC `SANFE`,
# MAGIC `SANIN`,
# MAGIC `SANKA`,
# MAGIC `SANKO`,
# MAGIC `SANVS`,
# MAGIC `STKKZ`,
# MAGIC `REKRI`,
# MAGIC `REKRS`,
# MAGIC `CADPO`,
# MAGIC `NFMAT`,
# MAGIC `NLFZT`,
# MAGIC `VERTI`,
# MAGIC `ALPOS`,
# MAGIC `EWAHR`,
# MAGIC `EKGRP`,
# MAGIC `LIFZT`,
# MAGIC `LIFNR`,
# MAGIC `PREIS`,
# MAGIC `PEINH`,
# MAGIC `WAERS`,
# MAGIC `SAKTO`,
# MAGIC `ROANZ`,
# MAGIC `ROMS1`,
# MAGIC `ROMS2`,
# MAGIC `ROMS3`,
# MAGIC `ROMEI`,
# MAGIC `ROMEN`,
# MAGIC `RFORM`,
# MAGIC `UPSKZ`,
# MAGIC `VALKZ`,
# MAGIC `LTXSP`,
# MAGIC `POTX1`,
# MAGIC `POTX2`,
# MAGIC `OBJTY`,
# MAGIC `MATKL`,
# MAGIC `WEBAZ`,
# MAGIC `DOKAR`,
# MAGIC `DOKNR`,
# MAGIC `DOKVR`,
# MAGIC `DOKTL`,
# MAGIC `CSSTR`,
# MAGIC `CLASS`,
# MAGIC `KLART`,
# MAGIC `POTPR`,
# MAGIC `AWAKZ`,
# MAGIC `INSKZ`,
# MAGIC `VCEKZ`,
# MAGIC `VSTKZ`,
# MAGIC `VACKZ`,
# MAGIC `EKORG`,
# MAGIC `CLOBK`,
# MAGIC `CLMUL`,
# MAGIC `CLALT`,
# MAGIC `CVIEW`,
# MAGIC `KNOBJ`,
# MAGIC `LGORT`,
# MAGIC `KZKUP`,
# MAGIC `INTRM`,
# MAGIC `TPEKZ`,
# MAGIC `STVKN`,
# MAGIC `DVDAT`,
# MAGIC `DVNAM`,
# MAGIC `DSPST`,
# MAGIC `ALPST`,
# MAGIC `ALPRF`,
# MAGIC `ALPGR`,
# MAGIC `KZNFP`,
# MAGIC `NFGRP`,
# MAGIC `NFEAG`,
# MAGIC `KNDVB`,
# MAGIC `KNDBZ`,
# MAGIC `KSTTY`,
# MAGIC `KSTNR`,
# MAGIC `KSTKN`,
# MAGIC `KSTPZ`,
# MAGIC `CLSZU`,
# MAGIC `KZCLB`,
# MAGIC `AEHLP`,
# MAGIC `PRVBE`,
# MAGIC `NLFZV`,
# MAGIC `NLFMV`,
# MAGIC `IDPOS`,
# MAGIC `IDHIS`,
# MAGIC `IDVAR`,
# MAGIC `ALEKZ`,
# MAGIC `ITMID`,
# MAGIC `GUID`,
# MAGIC `ITSOB`,
# MAGIC `RFPNT`,
# MAGIC `GUIDX`,
# MAGIC `SGT_CMKZ`,
# MAGIC `SGT_CATV`,
# MAGIC `VALID_TO`,
# MAGIC `VALID_TO_RKEY`,
# MAGIC `ECN_TO`,
# MAGIC `ECN_TO_RKEY`,
# MAGIC `ABLAD`,
# MAGIC `WEMPF`,
# MAGIC `STVKN_VERSN`,
# MAGIC `LASTCHANGEDATETIME`,
# MAGIC `SFWIND`,
# MAGIC `DUMMY_STPO_INCL_EEW_PS`,
# MAGIC `CUFACTOR`,
# MAGIC `/SAPMP/MET_LRCH`,
# MAGIC `/SAPMP/MAX_FERTL`,
# MAGIC `/SAPMP/FIX_AS_J`,
# MAGIC `/SAPMP/FIX_AS_E`,
# MAGIC `/SAPMP/FIX_AS_L`,
# MAGIC `/SAPMP/ABL_ZAHL`,
# MAGIC `/SAPMP/RUND_FAKT`,
# MAGIC `FSH_VMKZ`,
# MAGIC `FSH_PGQR`,
# MAGIC `FSH_PGQRRF`,
# MAGIC `FSH_CRITICAL_COMP`,
# MAGIC `FSH_CRITICAL_LEVEL`,
# MAGIC `FUNCID`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC VALUES
# MAGIC (S.`MANDT`,
# MAGIC S.`STLTY`,
# MAGIC S.`STLNR`,
# MAGIC S.`STLKN`,
# MAGIC S.`STPOZ`,
# MAGIC S.`DATUV`,
# MAGIC S.`TECHV`,
# MAGIC S.`AENNR`,
# MAGIC S.`LKENZ`,
# MAGIC S.`VGKNT`,
# MAGIC S.`VGPZL`,
# MAGIC S.`ANDAT`,
# MAGIC S.`ANNAM`,
# MAGIC S.`AEDAT`,
# MAGIC S.`AENAM`,
# MAGIC S.`IDNRK`,
# MAGIC S.`PSWRK`,
# MAGIC S.`POSTP`,
# MAGIC S.`POSNR`,
# MAGIC S.`SORTF`,
# MAGIC S.`MEINS`,
# MAGIC S.`MENGE`,
# MAGIC S.`FMENG`,
# MAGIC S.`AUSCH`,
# MAGIC S.`AVOAU`,
# MAGIC S.`NETAU`,
# MAGIC S.`SCHGT`,
# MAGIC S.`BEIKZ`,
# MAGIC S.`ERSKZ`,
# MAGIC S.`RVREL`,
# MAGIC S.`SANFE`,
# MAGIC S.`SANIN`,
# MAGIC S.`SANKA`,
# MAGIC S.`SANKO`,
# MAGIC S.`SANVS`,
# MAGIC S.`STKKZ`,
# MAGIC S.`REKRI`,
# MAGIC S.`REKRS`,
# MAGIC S.`CADPO`,
# MAGIC S.`NFMAT`,
# MAGIC S.`NLFZT`,
# MAGIC S.`VERTI`,
# MAGIC S.`ALPOS`,
# MAGIC S.`EWAHR`,
# MAGIC S.`EKGRP`,
# MAGIC S.`LIFZT`,
# MAGIC S.`LIFNR`,
# MAGIC S.`PREIS`,
# MAGIC S.`PEINH`,
# MAGIC S.`WAERS`,
# MAGIC S.`SAKTO`,
# MAGIC S.`ROANZ`,
# MAGIC S.`ROMS1`,
# MAGIC S.`ROMS2`,
# MAGIC S.`ROMS3`,
# MAGIC S.`ROMEI`,
# MAGIC S.`ROMEN`,
# MAGIC S.`RFORM`,
# MAGIC S.`UPSKZ`,
# MAGIC S.`VALKZ`,
# MAGIC S.`LTXSP`,
# MAGIC S.`POTX1`,
# MAGIC S.`POTX2`,
# MAGIC S.`OBJTY`,
# MAGIC S.`MATKL`,
# MAGIC S.`WEBAZ`,
# MAGIC S.`DOKAR`,
# MAGIC S.`DOKNR`,
# MAGIC S.`DOKVR`,
# MAGIC S.`DOKTL`,
# MAGIC S.`CSSTR`,
# MAGIC S.`CLASS`,
# MAGIC S.`KLART`,
# MAGIC S.`POTPR`,
# MAGIC S.`AWAKZ`,
# MAGIC S.`INSKZ`,
# MAGIC S.`VCEKZ`,
# MAGIC S.`VSTKZ`,
# MAGIC S.`VACKZ`,
# MAGIC S.`EKORG`,
# MAGIC S.`CLOBK`,
# MAGIC S.`CLMUL`,
# MAGIC S.`CLALT`,
# MAGIC S.`CVIEW`,
# MAGIC S.`KNOBJ`,
# MAGIC S.`LGORT`,
# MAGIC S.`KZKUP`,
# MAGIC S.`INTRM`,
# MAGIC S.`TPEKZ`,
# MAGIC S.`STVKN`,
# MAGIC S.`DVDAT`,
# MAGIC S.`DVNAM`,
# MAGIC S.`DSPST`,
# MAGIC S.`ALPST`,
# MAGIC S.`ALPRF`,
# MAGIC S.`ALPGR`,
# MAGIC S.`KZNFP`,
# MAGIC S.`NFGRP`,
# MAGIC S.`NFEAG`,
# MAGIC S.`KNDVB`,
# MAGIC S.`KNDBZ`,
# MAGIC S.`KSTTY`,
# MAGIC S.`KSTNR`,
# MAGIC S.`KSTKN`,
# MAGIC S.`KSTPZ`,
# MAGIC S.`CLSZU`,
# MAGIC S.`KZCLB`,
# MAGIC S.`AEHLP`,
# MAGIC S.`PRVBE`,
# MAGIC S.`NLFZV`,
# MAGIC S.`NLFMV`,
# MAGIC S.`IDPOS`,
# MAGIC S.`IDHIS`,
# MAGIC S.`IDVAR`,
# MAGIC S.`ALEKZ`,
# MAGIC S.`ITMID`,
# MAGIC S.`GUID`,
# MAGIC S.`ITSOB`,
# MAGIC S.`RFPNT`,
# MAGIC S.`GUIDX`,
# MAGIC S.`SGT_CMKZ`,
# MAGIC S.`SGT_CATV`,
# MAGIC S.`VALID_TO`,
# MAGIC S.`VALID_TO_RKEY`,
# MAGIC S.`ECN_TO`,
# MAGIC S.`ECN_TO_RKEY`,
# MAGIC S.`ABLAD`,
# MAGIC S.`WEMPF`,
# MAGIC S.`STVKN_VERSN`,
# MAGIC S.`LASTCHANGEDATETIME`,
# MAGIC S.`SFWIND`,
# MAGIC S.`DUMMY_STPO_INCL_EEW_PS`,
# MAGIC S.`CUFACTOR`,
# MAGIC S.`/SAPMP/MET_LRCH`,
# MAGIC S.`/SAPMP/MAX_FERTL`,
# MAGIC S.`/SAPMP/FIX_AS_J`,
# MAGIC S.`/SAPMP/FIX_AS_E`,
# MAGIC S.`/SAPMP/FIX_AS_L`,
# MAGIC S.`/SAPMP/ABL_ZAHL`,
# MAGIC S.`/SAPMP/RUND_FAKT`,
# MAGIC S.`FSH_VMKZ`,
# MAGIC S.`FSH_PGQR`,
# MAGIC S.`FSH_PGQRRF`,
# MAGIC S.`FSH_CRITICAL_COMP`,
# MAGIC S.`FSH_CRITICAL_LEVEL`,
# MAGIC S.`FUNCID`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)
