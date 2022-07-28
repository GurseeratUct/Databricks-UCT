# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'EKKO'
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

schema = StructType([StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
                     StructField('DI_OPERATION_TYPE',StringType(),True),\
                     StructField('MANDT',IntegerType(),True),\
StructField('EBELN',LongType(),True),\
StructField('BUKRS',StringType(),True),\
StructField('BSTYP',StringType(),True),\
StructField('BSART',StringType(),True),\
StructField('BSAKZ',StringType(),True),\
StructField('LOEKZ',StringType(),True),\
StructField('STATU',StringType(),True),\
StructField('AEDAT',StringType(),True),\
StructField('ERNAM',StringType(),True),\
StructField('LASTCHANGEDATETIME',DoubleType(),True),\
StructField('PINCR',IntegerType(),True),\
StructField('LPONR',IntegerType(),True),\
StructField('LIFNR',StringType(),True),\
StructField('SPRAS',StringType(),True),\
StructField('ZTERM',StringType(),True),\
StructField('ZBD1T',IntegerType(),True),\
StructField('ZBD2T',IntegerType(),True),\
StructField('ZBD3T',IntegerType(),True),\
StructField('ZBD1P',DoubleType(),True),\
StructField('ZBD2P',DoubleType(),True),\
StructField('EKORG',StringType(),True),\
StructField('EKGRP',StringType(),True),\
StructField('WAERS',StringType(),True),\
StructField('WKURS',DoubleType(),True),\
StructField('KUFIX',StringType(),True),\
StructField('BEDAT',StringType(),True),\
StructField('KDATB',StringType(),True),\
StructField('KDATE',StringType(),True),\
StructField('BWBDT',StringType(),True),\
StructField('ANGDT',StringType(),True),\
StructField('BNDDT',StringType(),True),\
StructField('GWLDT',StringType(),True),\
StructField('AUSNR',StringType(),True),\
StructField('ANGNR',StringType(),True),\
StructField('IHRAN',StringType(),True),\
StructField('IHREZ',StringType(),True),\
StructField('VERKF',StringType(),True),\
StructField('TELF1',StringType(),True),\
StructField('LLIEF',StringType(),True),\
StructField('KUNNR',StringType(),True),\
StructField('ACTIVE_ID',StringType(),True),\
StructField('KONNR',StringType(),True),\
StructField('ABGRU',StringType(),True),\
StructField('AUTLF',StringType(),True),\
StructField('WEAKT',StringType(),True),\
StructField('RESWK',StringType(),True),\
StructField('LBLIF',StringType(),True),\
StructField('INCO1',StringType(),True),\
StructField('INCO2',StringType(),True),\
StructField('KTWRT',DoubleType(),True),\
StructField('DISTRIBUTIONTYPE',StringType(),True),\
StructField('SUBMI',StringType(),True),\
StructField('KNUMV',StringType(),True),\
StructField('KALSM',StringType(),True),\
StructField('STAFO',StringType(),True),\
StructField('LIFRE',StringType(),True),\
StructField('EXNUM',StringType(),True),\
StructField('UNSEZ',StringType(),True),\
StructField('LOGSY',StringType(),True),\
StructField('UPINC',IntegerType(),True),\
StructField('STAKO',StringType(),True),\
StructField('FRGGR',StringType(),True),\
StructField('FRGSX',StringType(),True),\
StructField('FRGKE',StringType(),True),\
StructField('FRGZU',StringType(),True),\
StructField('FRGRL',StringType(),True),\
StructField('LANDS',StringType(),True),\
StructField('LPHIS',StringType(),True),\
StructField('ADRNR',StringType(),True),\
StructField('STCEG_L',StringType(),True),\
StructField('STCEG',StringType(),True),\
StructField('ABSGR',IntegerType(),True),\
StructField('ADDNR',StringType(),True),\
StructField('KORNR',StringType(),True),\
StructField('MEMORY',StringType(),True),\
StructField('PROCSTAT',IntegerType(),True),\
StructField('PROCESS_INDICATOR',StringType(),True),\
StructField('RLWRT',DoubleType(),True),\
StructField('CR_STAT',StringType(),True),\
StructField('REVNO',StringType(),True),\
StructField('SCMPROC',StringType(),True),\
StructField('REASON_CODE',StringType(),True),\
StructField('MEMORYTYPE',StringType(),True),\
StructField('RETTP',StringType(),True),\
StructField('RETPC',DoubleType(),True),\
StructField('DPTYP',StringType(),True),\
StructField('DPPCT',DoubleType(),True),\
StructField('DPAMT',DoubleType(),True),\
StructField('DPDAT',StringType(),True),\
StructField('MSR_ID',StringType(),True),\
StructField('HIERARCHY_EXISTS',StringType(),True),\
StructField('GROUPING_ID',StringType(),True),\
StructField('PARENT_ID',StringType(),True),\
StructField('THRESHOLD_EXISTS',StringType(),True),\
StructField('LEGAL_CONTRACT',StringType(),True),\
StructField('DESCRIPTION',StringType(),True),\
StructField('RELEASE_DATE',StringType(),True),\
StructField('VSART',StringType(),True),\
StructField('HANDOVERLOC',StringType(),True),\
StructField('SHIPCOND',StringType(),True),\
StructField('INCOV',StringType(),True),\
StructField('INCO2_L',StringType(),True),\
StructField('INCO3_L',StringType(),True),\
StructField('GRWCU',StringType(),True),\
StructField('INTRA_REL',StringType(),True),\
StructField('INTRA_EXCL',StringType(),True),\
StructField('QTN_ERLST_SUBMSN_DATE',StringType(),True),\
StructField('FOLLOWON_DOC_CAT',StringType(),True),\
StructField('FOLLOWON_DOC_TYPE',StringType(),True),\
StructField('DUMMY_EKKO_INCL_EEW_PS',StringType(),True),\
StructField('EXTERNALSYSTEM',StringType(),True),\
StructField('EXTERNALREFERENCEID',StringType(),True),\
StructField('EXT_REV_TMSTMP',DoubleType(),True),\
StructField('ISEOPBLOCKED',StringType(),True),\
StructField('ISAGED',StringType(),True),\
StructField('FORCE_ID',StringType(),True),\
StructField('FORCE_CNT',IntegerType(),True),\
StructField('RELOC_ID',StringType(),True),\
StructField('RELOC_SEQ_ID',StringType(),True),\
StructField('SOURCE_LOGSYS',StringType(),True),\
StructField('FSH_TRANSACTION',StringType(),True),\
StructField('FSH_ITEM_GROUP',IntegerType(),True),\
StructField('FSH_VAS_LAST_ITEM',IntegerType(),True),\
StructField('FSH_OS_STG_CHANGE',StringType(),True),\
StructField('TMS_REF_UUID',StringType(),True),\
StructField('ZAPCGK',IntegerType(),True),\
StructField('APCGK_EXTEND',IntegerType(),True),\
StructField('ZBAS_DATE',StringType(),True),\
StructField('ZADATTYP',StringType(),True),\
StructField('ZSTART_DAT',StringType(),True),\
StructField('Z_DEV',DoubleType(),True),\
StructField('ZINDANX',StringType(),True),\
StructField('ZLIMIT_DAT',StringType(),True),\
StructField('NUMERATOR',StringType(),True),\
StructField('HASHCAL_BDAT',StringType(),True),\
StructField('HASHCAL',StringType(),True),\
StructField('NEGATIVE',StringType(),True),\
StructField('HASHCAL_EXISTS',StringType(),True),\
StructField('KNOWN_INDEX',StringType(),True),\
StructField('POSTAT',StringType(),True),\
StructField('VZSKZ',StringType(),True),\
StructField('FSH_SNST_STATUS',StringType(),True),\
StructField('PROCE',IntegerType(),True),\
StructField('CONC',StringType(),True),\
StructField('CONT',StringType(),True),\
StructField('COMP',StringType(),True),\
StructField('OUTR',StringType(),True),\
StructField('DESP',StringType(),True),\
StructField('DESP_DAT',StringType(),True),\
StructField('DESP_CARGO',StringType(),True),\
StructField('PARE',StringType(),True),\
StructField('PARE_DAT',StringType(),True),\
StructField('PARE_CARGO',StringType(),True),\
StructField('PFM_CONTRACT',IntegerType(),True),\
StructField('POHF_TYPE',StringType(),True),\
StructField('EQ_EINDT',StringType(),True),\
StructField('EQ_WERKS',StringType(),True),\
StructField('FIXPO',StringType(),True),\
StructField('EKGRP_ALLOW',StringType(),True),\
StructField('WERKS_ALLOW',StringType(),True),\
StructField('CONTRACT_ALLOW',StringType(),True),\
StructField('PSTYP_ALLOW',StringType(),True),\
StructField('FIXPO_ALLOW',StringType(),True),\
StructField('KEY_ID_ALLOW',StringType(),True),\
StructField('AUREL_ALLOW',StringType(),True),\
StructField('DELPER_ALLOW',StringType(),True),\
StructField('EINDT_ALLOW',StringType(),True),\
StructField('LTSNR_ALLOW',StringType(),True),\
StructField('OTB_LEVEL',StringType(),True),\
StructField('OTB_COND_TYPE',StringType(),True),\
StructField('KEY_ID',IntegerType(),True),\
StructField('OTB_VALUE',DoubleType(),True),\
StructField('OTB_CURR',StringType(),True),\
StructField('OTB_RES_VALUE',DoubleType(),True),\
StructField('OTB_SPEC_VALUE',DoubleType(),True),\
StructField('SPR_RSN_PROFILE',StringType(),True),\
StructField('BUDG_TYPE',StringType(),True),\
StructField('OTB_STATUS',StringType(),True),\
StructField('OTB_REASON',StringType(),True),\
StructField('CHECK_TYPE',StringType(),True),\
StructField('CON_OTB_REQ',StringType(),True),\
StructField('CON_PREBOOK_LEV',StringType(),True),\
StructField('CON_DISTR_LEV',StringType(),True),\
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
.withColumn("AEDAT",to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
.withColumn("BEDAT",to_date(regexp_replace(df_add_column.BEDAT,'\.','-'))) \
.withColumn("KDATB",to_date(regexp_replace(df_add_column.KDATB,'\.','-'))) \
.withColumn("KDATE",to_date(regexp_replace(df_add_column.KDATE,'\.','-'))) \
.withColumn("BWBDT",to_date(regexp_replace(df_add_column.BWBDT,'\.','-'))) \
.withColumn("ANGDT",to_date(regexp_replace(df_add_column.ANGDT,'\.','-'))) \
.withColumn("BNDDT",to_date(regexp_replace(df_add_column.BNDDT,'\.','-'))) \
.withColumn("GWLDT",to_date(regexp_replace(df_add_column.GWLDT,'\.','-'))) \
.withColumn("IHRAN",to_date(regexp_replace(df_add_column.IHRAN,'\.','-'))) \
.withColumn("DPDAT",to_date(regexp_replace(df_add_column.DPDAT,'\.','-'))) \
.withColumn("RELEASE_DATE",to_date(regexp_replace(df_add_column.RELEASE_DATE,'\.','-'))) \
.withColumn("QTN_ERLST_SUBMSN_DATE",to_date(regexp_replace(df_add_column.QTN_ERLST_SUBMSN_DATE,'\.','-'))) \
.withColumn("ZBAS_DATE",to_date(regexp_replace(df_add_column.ZBAS_DATE,'\.','-'))) \
.withColumn("ZSTART_DAT",to_date(regexp_replace(df_add_column.ZSTART_DAT,'\.','-'))) \
.withColumn("ZLIMIT_DAT",to_date(regexp_replace(df_add_column.ZLIMIT_DAT,'\.','-'))) \
.withColumn("HASHCAL_BDAT",to_date(regexp_replace(df_add_column.HASHCAL_BDAT,'\.','-'))) \
.withColumn("DESP_DAT",to_date(regexp_replace(df_add_column.DESP_DAT,'\.','-'))) \
.withColumn("PARE_DAT",to_date(regexp_replace(df_add_column.PARE_DAT,'\.','-'))) \
.withColumn("EQ_EINDT",to_date(regexp_replace(df_add_column.EQ_EINDT,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.EKKO as T
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,EBELN ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_EKKO where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and 
# MAGIC T.EBELN = S.EBELN 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`MANDT` =  S.`MANDT`,
# MAGIC T.`EBELN` =  S.`EBELN`,
# MAGIC T.`BUKRS` =  S.`BUKRS`,
# MAGIC T.`BSTYP` =  S.`BSTYP`,
# MAGIC T.`BSART` =  S.`BSART`,
# MAGIC T.`BSAKZ` =  S.`BSAKZ`,
# MAGIC T.`LOEKZ` =  S.`LOEKZ`,
# MAGIC T.`STATU` =  S.`STATU`,
# MAGIC T.`AEDAT` =  S.`AEDAT`,
# MAGIC T.`ERNAM` =  S.`ERNAM`,
# MAGIC T.`LASTCHANGEDATETIME` =  S.`LASTCHANGEDATETIME`,
# MAGIC T.`PINCR` =  S.`PINCR`,
# MAGIC T.`LPONR` =  S.`LPONR`,
# MAGIC T.`LIFNR` =  S.`LIFNR`,
# MAGIC T.`SPRAS` =  S.`SPRAS`,
# MAGIC T.`ZTERM` =  S.`ZTERM`,
# MAGIC T.`ZBD1T` =  S.`ZBD1T`,
# MAGIC T.`ZBD2T` =  S.`ZBD2T`,
# MAGIC T.`ZBD3T` =  S.`ZBD3T`,
# MAGIC T.`ZBD1P` =  S.`ZBD1P`,
# MAGIC T.`ZBD2P` =  S.`ZBD2P`,
# MAGIC T.`EKORG` =  S.`EKORG`,
# MAGIC T.`EKGRP` =  S.`EKGRP`,
# MAGIC T.`WAERS` =  S.`WAERS`,
# MAGIC T.`WKURS` =  S.`WKURS`,
# MAGIC T.`KUFIX` =  S.`KUFIX`,
# MAGIC T.`BEDAT` =  S.`BEDAT`,
# MAGIC T.`KDATB` =  S.`KDATB`,
# MAGIC T.`KDATE` =  S.`KDATE`,
# MAGIC T.`BWBDT` =  S.`BWBDT`,
# MAGIC T.`ANGDT` =  S.`ANGDT`,
# MAGIC T.`BNDDT` =  S.`BNDDT`,
# MAGIC T.`GWLDT` =  S.`GWLDT`,
# MAGIC T.`AUSNR` =  S.`AUSNR`,
# MAGIC T.`ANGNR` =  S.`ANGNR`,
# MAGIC T.`IHRAN` =  S.`IHRAN`,
# MAGIC T.`IHREZ` =  S.`IHREZ`,
# MAGIC T.`VERKF` =  S.`VERKF`,
# MAGIC T.`TELF1` =  S.`TELF1`,
# MAGIC T.`LLIEF` =  S.`LLIEF`,
# MAGIC T.`KUNNR` =  S.`KUNNR`,
# MAGIC T.`ACTIVE_ID` =  S.`ACTIVE_ID`,
# MAGIC T.`KONNR` =  S.`KONNR`,
# MAGIC T.`ABGRU` =  S.`ABGRU`,
# MAGIC T.`AUTLF` =  S.`AUTLF`,
# MAGIC T.`WEAKT` =  S.`WEAKT`,
# MAGIC T.`RESWK` =  S.`RESWK`,
# MAGIC T.`LBLIF` =  S.`LBLIF`,
# MAGIC T.`INCO1` =  S.`INCO1`,
# MAGIC T.`INCO2` =  S.`INCO2`,
# MAGIC T.`KTWRT` =  S.`KTWRT`,
# MAGIC T.`DISTRIBUTIONTYPE` =  S.`DISTRIBUTIONTYPE`,
# MAGIC T.`SUBMI` =  S.`SUBMI`,
# MAGIC T.`KNUMV` =  S.`KNUMV`,
# MAGIC T.`KALSM` =  S.`KALSM`,
# MAGIC T.`STAFO` =  S.`STAFO`,
# MAGIC T.`LIFRE` =  S.`LIFRE`,
# MAGIC T.`EXNUM` =  S.`EXNUM`,
# MAGIC T.`UNSEZ` =  S.`UNSEZ`,
# MAGIC T.`LOGSY` =  S.`LOGSY`,
# MAGIC T.`UPINC` =  S.`UPINC`,
# MAGIC T.`STAKO` =  S.`STAKO`,
# MAGIC T.`FRGGR` =  S.`FRGGR`,
# MAGIC T.`FRGSX` =  S.`FRGSX`,
# MAGIC T.`FRGKE` =  S.`FRGKE`,
# MAGIC T.`FRGZU` =  S.`FRGZU`,
# MAGIC T.`FRGRL` =  S.`FRGRL`,
# MAGIC T.`LANDS` =  S.`LANDS`,
# MAGIC T.`LPHIS` =  S.`LPHIS`,
# MAGIC T.`ADRNR` =  S.`ADRNR`,
# MAGIC T.`STCEG_L` =  S.`STCEG_L`,
# MAGIC T.`STCEG` =  S.`STCEG`,
# MAGIC T.`ABSGR` =  S.`ABSGR`,
# MAGIC T.`ADDNR` =  S.`ADDNR`,
# MAGIC T.`KORNR` =  S.`KORNR`,
# MAGIC T.`MEMORY` =  S.`MEMORY`,
# MAGIC T.`PROCSTAT` =  S.`PROCSTAT`,
# MAGIC T.`PROCESS_INDICATOR` =  S.`PROCESS_INDICATOR`,
# MAGIC T.`RLWRT` =  S.`RLWRT`,
# MAGIC T.`CR_STAT` =  S.`CR_STAT`,
# MAGIC T.`REVNO` =  S.`REVNO`,
# MAGIC T.`SCMPROC` =  S.`SCMPROC`,
# MAGIC T.`REASON_CODE` =  S.`REASON_CODE`,
# MAGIC T.`MEMORYTYPE` =  S.`MEMORYTYPE`,
# MAGIC T.`RETTP` =  S.`RETTP`,
# MAGIC T.`RETPC` =  S.`RETPC`,
# MAGIC T.`DPTYP` =  S.`DPTYP`,
# MAGIC T.`DPPCT` =  S.`DPPCT`,
# MAGIC T.`DPAMT` =  S.`DPAMT`,
# MAGIC T.`DPDAT` =  S.`DPDAT`,
# MAGIC T.`MSR_ID` =  S.`MSR_ID`,
# MAGIC T.`HIERARCHY_EXISTS` =  S.`HIERARCHY_EXISTS`,
# MAGIC T.`GROUPING_ID` =  S.`GROUPING_ID`,
# MAGIC T.`PARENT_ID` =  S.`PARENT_ID`,
# MAGIC T.`THRESHOLD_EXISTS` =  S.`THRESHOLD_EXISTS`,
# MAGIC T.`LEGAL_CONTRACT` =  S.`LEGAL_CONTRACT`,
# MAGIC T.`DESCRIPTION` =  S.`DESCRIPTION`,
# MAGIC T.`RELEASE_DATE` =  S.`RELEASE_DATE`,
# MAGIC T.`VSART` =  S.`VSART`,
# MAGIC T.`HANDOVERLOC` =  S.`HANDOVERLOC`,
# MAGIC T.`SHIPCOND` =  S.`SHIPCOND`,
# MAGIC T.`INCOV` =  S.`INCOV`,
# MAGIC T.`INCO2_L` =  S.`INCO2_L`,
# MAGIC T.`INCO3_L` =  S.`INCO3_L`,
# MAGIC T.`GRWCU` =  S.`GRWCU`,
# MAGIC T.`INTRA_REL` =  S.`INTRA_REL`,
# MAGIC T.`INTRA_EXCL` =  S.`INTRA_EXCL`,
# MAGIC T.`QTN_ERLST_SUBMSN_DATE` =  S.`QTN_ERLST_SUBMSN_DATE`,
# MAGIC T.`FOLLOWON_DOC_CAT` =  S.`FOLLOWON_DOC_CAT`,
# MAGIC T.`FOLLOWON_DOC_TYPE` =  S.`FOLLOWON_DOC_TYPE`,
# MAGIC T.`DUMMY_EKKO_INCL_EEW_PS` =  S.`DUMMY_EKKO_INCL_EEW_PS`,
# MAGIC T.`EXTERNALSYSTEM` =  S.`EXTERNALSYSTEM`,
# MAGIC T.`EXTERNALREFERENCEID` =  S.`EXTERNALREFERENCEID`,
# MAGIC T.`EXT_REV_TMSTMP` =  S.`EXT_REV_TMSTMP`,
# MAGIC T.`ISEOPBLOCKED` =  S.`ISEOPBLOCKED`,
# MAGIC T.`ISAGED` =  S.`ISAGED`,
# MAGIC T.`FORCE_ID` =  S.`FORCE_ID`,
# MAGIC T.`FORCE_CNT` =  S.`FORCE_CNT`,
# MAGIC T.`RELOC_ID` =  S.`RELOC_ID`,
# MAGIC T.`RELOC_SEQ_ID` =  S.`RELOC_SEQ_ID`,
# MAGIC T.`SOURCE_LOGSYS` =  S.`SOURCE_LOGSYS`,
# MAGIC T.`FSH_TRANSACTION` =  S.`FSH_TRANSACTION`,
# MAGIC T.`FSH_ITEM_GROUP` =  S.`FSH_ITEM_GROUP`,
# MAGIC T.`FSH_VAS_LAST_ITEM` =  S.`FSH_VAS_LAST_ITEM`,
# MAGIC T.`FSH_OS_STG_CHANGE` =  S.`FSH_OS_STG_CHANGE`,
# MAGIC T.`TMS_REF_UUID` =  S.`TMS_REF_UUID`,
# MAGIC T.`ZAPCGK` =  S.`ZAPCGK`,
# MAGIC T.`APCGK_EXTEND` =  S.`APCGK_EXTEND`,
# MAGIC T.`ZBAS_DATE` =  S.`ZBAS_DATE`,
# MAGIC T.`ZADATTYP` =  S.`ZADATTYP`,
# MAGIC T.`ZSTART_DAT` =  S.`ZSTART_DAT`,
# MAGIC T.`Z_DEV` =  S.`Z_DEV`,
# MAGIC T.`ZINDANX` =  S.`ZINDANX`,
# MAGIC T.`ZLIMIT_DAT` =  S.`ZLIMIT_DAT`,
# MAGIC T.`NUMERATOR` =  S.`NUMERATOR`,
# MAGIC T.`HASHCAL_BDAT` =  S.`HASHCAL_BDAT`,
# MAGIC T.`HASHCAL` =  S.`HASHCAL`,
# MAGIC T.`NEGATIVE` =  S.`NEGATIVE`,
# MAGIC T.`HASHCAL_EXISTS` =  S.`HASHCAL_EXISTS`,
# MAGIC T.`KNOWN_INDEX` =  S.`KNOWN_INDEX`,
# MAGIC T.`POSTAT` =  S.`POSTAT`,
# MAGIC T.`VZSKZ` =  S.`VZSKZ`,
# MAGIC T.`FSH_SNST_STATUS` =  S.`FSH_SNST_STATUS`,
# MAGIC T.`PROCE` =  S.`PROCE`,
# MAGIC T.`CONC` =  S.`CONC`,
# MAGIC T.`CONT` =  S.`CONT`,
# MAGIC T.`COMP` =  S.`COMP`,
# MAGIC T.`OUTR` =  S.`OUTR`,
# MAGIC T.`DESP` =  S.`DESP`,
# MAGIC T.`DESP_DAT` =  S.`DESP_DAT`,
# MAGIC T.`DESP_CARGO` =  S.`DESP_CARGO`,
# MAGIC T.`PARE` =  S.`PARE`,
# MAGIC T.`PARE_DAT` =  S.`PARE_DAT`,
# MAGIC T.`PARE_CARGO` =  S.`PARE_CARGO`,
# MAGIC T.`PFM_CONTRACT` =  S.`PFM_CONTRACT`,
# MAGIC T.`POHF_TYPE` =  S.`POHF_TYPE`,
# MAGIC T.`EQ_EINDT` =  S.`EQ_EINDT`,
# MAGIC T.`EQ_WERKS` =  S.`EQ_WERKS`,
# MAGIC T.`FIXPO` =  S.`FIXPO`,
# MAGIC T.`EKGRP_ALLOW` =  S.`EKGRP_ALLOW`,
# MAGIC T.`WERKS_ALLOW` =  S.`WERKS_ALLOW`,
# MAGIC T.`CONTRACT_ALLOW` =  S.`CONTRACT_ALLOW`,
# MAGIC T.`PSTYP_ALLOW` =  S.`PSTYP_ALLOW`,
# MAGIC T.`FIXPO_ALLOW` =  S.`FIXPO_ALLOW`,
# MAGIC T.`KEY_ID_ALLOW` =  S.`KEY_ID_ALLOW`,
# MAGIC T.`AUREL_ALLOW` =  S.`AUREL_ALLOW`,
# MAGIC T.`DELPER_ALLOW` =  S.`DELPER_ALLOW`,
# MAGIC T.`EINDT_ALLOW` =  S.`EINDT_ALLOW`,
# MAGIC T.`LTSNR_ALLOW` =  S.`LTSNR_ALLOW`,
# MAGIC T.`OTB_LEVEL` =  S.`OTB_LEVEL`,
# MAGIC T.`OTB_COND_TYPE` =  S.`OTB_COND_TYPE`,
# MAGIC T.`KEY_ID` =  S.`KEY_ID`,
# MAGIC T.`OTB_VALUE` =  S.`OTB_VALUE`,
# MAGIC T.`OTB_CURR` =  S.`OTB_CURR`,
# MAGIC T.`OTB_RES_VALUE` =  S.`OTB_RES_VALUE`,
# MAGIC T.`OTB_SPEC_VALUE` =  S.`OTB_SPEC_VALUE`,
# MAGIC T.`SPR_RSN_PROFILE` =  S.`SPR_RSN_PROFILE`,
# MAGIC T.`BUDG_TYPE` =  S.`BUDG_TYPE`,
# MAGIC T.`OTB_STATUS` =  S.`OTB_STATUS`,
# MAGIC T.`OTB_REASON` =  S.`OTB_REASON`,
# MAGIC T.`CHECK_TYPE` =  S.`CHECK_TYPE`,
# MAGIC T.`CON_OTB_REQ` =  S.`CON_OTB_REQ`,
# MAGIC T.`CON_PREBOOK_LEV` =  S.`CON_PREBOOK_LEV`,
# MAGIC T.`CON_DISTR_LEV` =  S.`CON_DISTR_LEV`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `EBELN`,
# MAGIC `BUKRS`,
# MAGIC `BSTYP`,
# MAGIC `BSART`,
# MAGIC `BSAKZ`,
# MAGIC `LOEKZ`,
# MAGIC `STATU`,
# MAGIC `AEDAT`,
# MAGIC `ERNAM`,
# MAGIC `LASTCHANGEDATETIME`,
# MAGIC `PINCR`,
# MAGIC `LPONR`,
# MAGIC `LIFNR`,
# MAGIC `SPRAS`,
# MAGIC `ZTERM`,
# MAGIC `ZBD1T`,
# MAGIC `ZBD2T`,
# MAGIC `ZBD3T`,
# MAGIC `ZBD1P`,
# MAGIC `ZBD2P`,
# MAGIC `EKORG`,
# MAGIC `EKGRP`,
# MAGIC `WAERS`,
# MAGIC `WKURS`,
# MAGIC `KUFIX`,
# MAGIC `BEDAT`,
# MAGIC `KDATB`,
# MAGIC `KDATE`,
# MAGIC `BWBDT`,
# MAGIC `ANGDT`,
# MAGIC `BNDDT`,
# MAGIC `GWLDT`,
# MAGIC `AUSNR`,
# MAGIC `ANGNR`,
# MAGIC `IHRAN`,
# MAGIC `IHREZ`,
# MAGIC `VERKF`,
# MAGIC `TELF1`,
# MAGIC `LLIEF`,
# MAGIC `KUNNR`,
# MAGIC `ACTIVE_ID`,
# MAGIC `KONNR`,
# MAGIC `ABGRU`,
# MAGIC `AUTLF`,
# MAGIC `WEAKT`,
# MAGIC `RESWK`,
# MAGIC `LBLIF`,
# MAGIC `INCO1`,
# MAGIC `INCO2`,
# MAGIC `KTWRT`,
# MAGIC `DISTRIBUTIONTYPE`,
# MAGIC `SUBMI`,
# MAGIC `KNUMV`,
# MAGIC `KALSM`,
# MAGIC `STAFO`,
# MAGIC `LIFRE`,
# MAGIC `EXNUM`,
# MAGIC `UNSEZ`,
# MAGIC `LOGSY`,
# MAGIC `UPINC`,
# MAGIC `STAKO`,
# MAGIC `FRGGR`,
# MAGIC `FRGSX`,
# MAGIC `FRGKE`,
# MAGIC `FRGZU`,
# MAGIC `FRGRL`,
# MAGIC `LANDS`,
# MAGIC `LPHIS`,
# MAGIC `ADRNR`,
# MAGIC `STCEG_L`,
# MAGIC `STCEG`,
# MAGIC `ABSGR`,
# MAGIC `ADDNR`,
# MAGIC `KORNR`,
# MAGIC `MEMORY`,
# MAGIC `PROCSTAT`,
# MAGIC `PROCESS_INDICATOR`,
# MAGIC `RLWRT`,
# MAGIC `CR_STAT`,
# MAGIC `REVNO`,
# MAGIC `SCMPROC`,
# MAGIC `REASON_CODE`,
# MAGIC `MEMORYTYPE`,
# MAGIC `RETTP`,
# MAGIC `RETPC`,
# MAGIC `DPTYP`,
# MAGIC `DPPCT`,
# MAGIC `DPAMT`,
# MAGIC `DPDAT`,
# MAGIC `MSR_ID`,
# MAGIC `HIERARCHY_EXISTS`,
# MAGIC `GROUPING_ID`,
# MAGIC `PARENT_ID`,
# MAGIC `THRESHOLD_EXISTS`,
# MAGIC `LEGAL_CONTRACT`,
# MAGIC `DESCRIPTION`,
# MAGIC `RELEASE_DATE`,
# MAGIC `VSART`,
# MAGIC `HANDOVERLOC`,
# MAGIC `SHIPCOND`,
# MAGIC `INCOV`,
# MAGIC `INCO2_L`,
# MAGIC `INCO3_L`,
# MAGIC `GRWCU`,
# MAGIC `INTRA_REL`,
# MAGIC `INTRA_EXCL`,
# MAGIC `QTN_ERLST_SUBMSN_DATE`,
# MAGIC `FOLLOWON_DOC_CAT`,
# MAGIC `FOLLOWON_DOC_TYPE`,
# MAGIC `DUMMY_EKKO_INCL_EEW_PS`,
# MAGIC `EXTERNALSYSTEM`,
# MAGIC `EXTERNALREFERENCEID`,
# MAGIC `EXT_REV_TMSTMP`,
# MAGIC `ISEOPBLOCKED`,
# MAGIC `ISAGED`,
# MAGIC `FORCE_ID`,
# MAGIC `FORCE_CNT`,
# MAGIC `RELOC_ID`,
# MAGIC `RELOC_SEQ_ID`,
# MAGIC `SOURCE_LOGSYS`,
# MAGIC `FSH_TRANSACTION`,
# MAGIC `FSH_ITEM_GROUP`,
# MAGIC `FSH_VAS_LAST_ITEM`,
# MAGIC `FSH_OS_STG_CHANGE`,
# MAGIC `TMS_REF_UUID`,
# MAGIC `ZAPCGK`,
# MAGIC `APCGK_EXTEND`,
# MAGIC `ZBAS_DATE`,
# MAGIC `ZADATTYP`,
# MAGIC `ZSTART_DAT`,
# MAGIC `Z_DEV`,
# MAGIC `ZINDANX`,
# MAGIC `ZLIMIT_DAT`,
# MAGIC `NUMERATOR`,
# MAGIC `HASHCAL_BDAT`,
# MAGIC `HASHCAL`,
# MAGIC `NEGATIVE`,
# MAGIC `HASHCAL_EXISTS`,
# MAGIC `KNOWN_INDEX`,
# MAGIC `POSTAT`,
# MAGIC `VZSKZ`,
# MAGIC `FSH_SNST_STATUS`,
# MAGIC `PROCE`,
# MAGIC `CONC`,
# MAGIC `CONT`,
# MAGIC `COMP`,
# MAGIC `OUTR`,
# MAGIC `DESP`,
# MAGIC `DESP_DAT`,
# MAGIC `DESP_CARGO`,
# MAGIC `PARE`,
# MAGIC `PARE_DAT`,
# MAGIC `PARE_CARGO`,
# MAGIC `PFM_CONTRACT`,
# MAGIC `POHF_TYPE`,
# MAGIC `EQ_EINDT`,
# MAGIC `EQ_WERKS`,
# MAGIC `FIXPO`,
# MAGIC `EKGRP_ALLOW`,
# MAGIC `WERKS_ALLOW`,
# MAGIC `CONTRACT_ALLOW`,
# MAGIC `PSTYP_ALLOW`,
# MAGIC `FIXPO_ALLOW`,
# MAGIC `KEY_ID_ALLOW`,
# MAGIC `AUREL_ALLOW`,
# MAGIC `DELPER_ALLOW`,
# MAGIC `EINDT_ALLOW`,
# MAGIC `LTSNR_ALLOW`,
# MAGIC `OTB_LEVEL`,
# MAGIC `OTB_COND_TYPE`,
# MAGIC `KEY_ID`,
# MAGIC `OTB_VALUE`,
# MAGIC `OTB_CURR`,
# MAGIC `OTB_RES_VALUE`,
# MAGIC `OTB_SPEC_VALUE`,
# MAGIC `SPR_RSN_PROFILE`,
# MAGIC `BUDG_TYPE`,
# MAGIC `OTB_STATUS`,
# MAGIC `OTB_REASON`,
# MAGIC `CHECK_TYPE`,
# MAGIC `CON_OTB_REQ`,
# MAGIC `CON_PREBOOK_LEV`,
# MAGIC `CON_DISTR_LEV`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC  DataSource,
# MAGIC   UpdatedOn)
# MAGIC   VALUES 
# MAGIC (
# MAGIC S.`MANDT`,
# MAGIC S.`EBELN`,
# MAGIC S.`BUKRS`,
# MAGIC S.`BSTYP`,
# MAGIC S.`BSART`,
# MAGIC S.`BSAKZ`,
# MAGIC S.`LOEKZ`,
# MAGIC S.`STATU`,
# MAGIC S.`AEDAT`,
# MAGIC S.`ERNAM`,
# MAGIC S.`LASTCHANGEDATETIME`,
# MAGIC S.`PINCR`,
# MAGIC S.`LPONR`,
# MAGIC S.`LIFNR`,
# MAGIC S.`SPRAS`,
# MAGIC S.`ZTERM`,
# MAGIC S.`ZBD1T`,
# MAGIC S.`ZBD2T`,
# MAGIC S.`ZBD3T`,
# MAGIC S.`ZBD1P`,
# MAGIC S.`ZBD2P`,
# MAGIC S.`EKORG`,
# MAGIC S.`EKGRP`,
# MAGIC S.`WAERS`,
# MAGIC S.`WKURS`,
# MAGIC S.`KUFIX`,
# MAGIC S.`BEDAT`,
# MAGIC S.`KDATB`,
# MAGIC S.`KDATE`,
# MAGIC S.`BWBDT`,
# MAGIC S.`ANGDT`,
# MAGIC S.`BNDDT`,
# MAGIC S.`GWLDT`,
# MAGIC S.`AUSNR`,
# MAGIC S.`ANGNR`,
# MAGIC S.`IHRAN`,
# MAGIC S.`IHREZ`,
# MAGIC S.`VERKF`,
# MAGIC S.`TELF1`,
# MAGIC S.`LLIEF`,
# MAGIC S.`KUNNR`,
# MAGIC S.`ACTIVE_ID`,
# MAGIC S.`KONNR`,
# MAGIC S.`ABGRU`,
# MAGIC S.`AUTLF`,
# MAGIC S.`WEAKT`,
# MAGIC S.`RESWK`,
# MAGIC S.`LBLIF`,
# MAGIC S.`INCO1`,
# MAGIC S.`INCO2`,
# MAGIC S.`KTWRT`,
# MAGIC S.`DISTRIBUTIONTYPE`,
# MAGIC S.`SUBMI`,
# MAGIC S.`KNUMV`,
# MAGIC S.`KALSM`,
# MAGIC S.`STAFO`,
# MAGIC S.`LIFRE`,
# MAGIC S.`EXNUM`,
# MAGIC S.`UNSEZ`,
# MAGIC S.`LOGSY`,
# MAGIC S.`UPINC`,
# MAGIC S.`STAKO`,
# MAGIC S.`FRGGR`,
# MAGIC S.`FRGSX`,
# MAGIC S.`FRGKE`,
# MAGIC S.`FRGZU`,
# MAGIC S.`FRGRL`,
# MAGIC S.`LANDS`,
# MAGIC S.`LPHIS`,
# MAGIC S.`ADRNR`,
# MAGIC S.`STCEG_L`,
# MAGIC S.`STCEG`,
# MAGIC S.`ABSGR`,
# MAGIC S.`ADDNR`,
# MAGIC S.`KORNR`,
# MAGIC S.`MEMORY`,
# MAGIC S.`PROCSTAT`,
# MAGIC S.`PROCESS_INDICATOR`,
# MAGIC S.`RLWRT`,
# MAGIC S.`CR_STAT`,
# MAGIC S.`REVNO`,
# MAGIC S.`SCMPROC`,
# MAGIC S.`REASON_CODE`,
# MAGIC S.`MEMORYTYPE`,
# MAGIC S.`RETTP`,
# MAGIC S.`RETPC`,
# MAGIC S.`DPTYP`,
# MAGIC S.`DPPCT`,
# MAGIC S.`DPAMT`,
# MAGIC S.`DPDAT`,
# MAGIC S.`MSR_ID`,
# MAGIC S.`HIERARCHY_EXISTS`,
# MAGIC S.`GROUPING_ID`,
# MAGIC S.`PARENT_ID`,
# MAGIC S.`THRESHOLD_EXISTS`,
# MAGIC S.`LEGAL_CONTRACT`,
# MAGIC S.`DESCRIPTION`,
# MAGIC S.`RELEASE_DATE`,
# MAGIC S.`VSART`,
# MAGIC S.`HANDOVERLOC`,
# MAGIC S.`SHIPCOND`,
# MAGIC S.`INCOV`,
# MAGIC S.`INCO2_L`,
# MAGIC S.`INCO3_L`,
# MAGIC S.`GRWCU`,
# MAGIC S.`INTRA_REL`,
# MAGIC S.`INTRA_EXCL`,
# MAGIC S.`QTN_ERLST_SUBMSN_DATE`,
# MAGIC S.`FOLLOWON_DOC_CAT`,
# MAGIC S.`FOLLOWON_DOC_TYPE`,
# MAGIC S.`DUMMY_EKKO_INCL_EEW_PS`,
# MAGIC S.`EXTERNALSYSTEM`,
# MAGIC S.`EXTERNALREFERENCEID`,
# MAGIC S.`EXT_REV_TMSTMP`,
# MAGIC S.`ISEOPBLOCKED`,
# MAGIC S.`ISAGED`,
# MAGIC S.`FORCE_ID`,
# MAGIC S.`FORCE_CNT`,
# MAGIC S.`RELOC_ID`,
# MAGIC S.`RELOC_SEQ_ID`,
# MAGIC S.`SOURCE_LOGSYS`,
# MAGIC S.`FSH_TRANSACTION`,
# MAGIC S.`FSH_ITEM_GROUP`,
# MAGIC S.`FSH_VAS_LAST_ITEM`,
# MAGIC S.`FSH_OS_STG_CHANGE`,
# MAGIC S.`TMS_REF_UUID`,
# MAGIC S.`ZAPCGK`,
# MAGIC S.`APCGK_EXTEND`,
# MAGIC S.`ZBAS_DATE`,
# MAGIC S.`ZADATTYP`,
# MAGIC S.`ZSTART_DAT`,
# MAGIC S.`Z_DEV`,
# MAGIC S.`ZINDANX`,
# MAGIC S.`ZLIMIT_DAT`,
# MAGIC S.`NUMERATOR`,
# MAGIC S.`HASHCAL_BDAT`,
# MAGIC S.`HASHCAL`,
# MAGIC S.`NEGATIVE`,
# MAGIC S.`HASHCAL_EXISTS`,
# MAGIC S.`KNOWN_INDEX`,
# MAGIC S.`POSTAT`,
# MAGIC S.`VZSKZ`,
# MAGIC S.`FSH_SNST_STATUS`,
# MAGIC S.`PROCE`,
# MAGIC S.`CONC`,
# MAGIC S.`CONT`,
# MAGIC S.`COMP`,
# MAGIC S.`OUTR`,
# MAGIC S.`DESP`,
# MAGIC S.`DESP_DAT`,
# MAGIC S.`DESP_CARGO`,
# MAGIC S.`PARE`,
# MAGIC S.`PARE_DAT`,
# MAGIC S.`PARE_CARGO`,
# MAGIC S.`PFM_CONTRACT`,
# MAGIC S.`POHF_TYPE`,
# MAGIC S.`EQ_EINDT`,
# MAGIC S.`EQ_WERKS`,
# MAGIC S.`FIXPO`,
# MAGIC S.`EKGRP_ALLOW`,
# MAGIC S.`WERKS_ALLOW`,
# MAGIC S.`CONTRACT_ALLOW`,
# MAGIC S.`PSTYP_ALLOW`,
# MAGIC S.`FIXPO_ALLOW`,
# MAGIC S.`KEY_ID_ALLOW`,
# MAGIC S.`AUREL_ALLOW`,
# MAGIC S.`DELPER_ALLOW`,
# MAGIC S.`EINDT_ALLOW`,
# MAGIC S.`LTSNR_ALLOW`,
# MAGIC S.`OTB_LEVEL`,
# MAGIC S.`OTB_COND_TYPE`,
# MAGIC S.`KEY_ID`,
# MAGIC S.`OTB_VALUE`,
# MAGIC S.`OTB_CURR`,
# MAGIC S.`OTB_RES_VALUE`,
# MAGIC S.`OTB_SPEC_VALUE`,
# MAGIC S.`SPR_RSN_PROFILE`,
# MAGIC S.`BUDG_TYPE`,
# MAGIC S.`OTB_STATUS`,
# MAGIC S.`OTB_REASON`,
# MAGIC S.`CHECK_TYPE`,
# MAGIC S.`CON_OTB_REQ`,
# MAGIC S.`CON_PREBOOK_LEV`,
# MAGIC S.`CON_DISTR_LEV`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'SAP',
# MAGIC now()
# MAGIC  )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)
