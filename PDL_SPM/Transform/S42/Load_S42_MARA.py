# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

schema = StructType([ \
StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',IntegerType(),True),\
StructField('MATNR',StringType(),True),\
StructField('ERSDA',StringType(),True),\
StructField('CREATED_AT_TIME',TimestampType(),True),\
StructField('ERNAM',StringType(),True),\
StructField('LAEDA',StringType(),True),\
StructField('AENAM',StringType(),True),\
StructField('VPSTA',StringType(),True),\
StructField('PSTAT',StringType(),True),\
StructField('LVORM',StringType(),True),\
StructField('MTART',StringType(),True),\
StructField('MBRSH',StringType(),True),\
StructField('MATKL',StringType(),True),\
StructField('BISMT',StringType(),True),\
StructField('MEINS',StringType(),True),\
StructField('BSTME',StringType(),True),\
StructField('ZEINR',StringType(),True),\
StructField('ZEIAR',StringType(),True),\
StructField('ZEIVR',StringType(),True),\
StructField('ZEIFO',StringType(),True),\
StructField('AESZN',StringType(),True),\
StructField('BLATT',StringType(),True),\
StructField('BLANZ',IntegerType(),True),\
StructField('FERTH',StringType(),True),\
StructField('FORMT',StringType(),True),\
StructField('GROES',StringType(),True),\
StructField('WRKST',StringType(),True),\
StructField('NORMT',StringType(),True),\
StructField('LABOR',StringType(),True),\
StructField('EKWSL',StringType(),True),\
StructField('BRGEW',DoubleType(),True),\
StructField('NTGEW',DoubleType(),True),\
StructField('GEWEI',StringType(),True),\
StructField('VOLUM',DoubleType(),True),\
StructField('VOLEH',StringType(),True),\
StructField('BEHVO',StringType(),True),\
StructField('RAUBE',StringType(),True),\
StructField('TEMPB',StringType(),True),\
StructField('DISST',StringType(),True),\
StructField('TRAGR',StringType(),True),\
StructField('STOFF',StringType(),True),\
StructField('SPART',StringType(),True),\
StructField('KUNNR',StringType(),True),\
StructField('EANNR',StringType(),True),\
StructField('WESCH',DoubleType(),True),\
StructField('BWVOR',StringType(),True),\
StructField('BWSCL',StringType(),True),\
StructField('SAISO',StringType(),True),\
StructField('ETIAR',StringType(),True),\
StructField('ETIFO',StringType(),True),\
StructField('ENTAR',StringType(),True),\
StructField('EAN11',StringType(),True),\
StructField('NUMTP',StringType(),True),\
StructField('LAENG',DoubleType(),True),\
StructField('BREIT',DoubleType(),True),\
StructField('HOEHE',DoubleType(),True),\
StructField('MEABM',StringType(),True),\
StructField('PRDHA',StringType(),True),\
StructField('AEKLK',StringType(),True),\
StructField('CADKZ',StringType(),True),\
StructField('QMPUR',StringType(),True),\
StructField('ERGEW',DoubleType(),True),\
StructField('ERGEI',StringType(),True),\
StructField('ERVOL',DoubleType(),True),\
StructField('ERVOE',StringType(),True),\
StructField('GEWTO',DoubleType(),True),\
StructField('VOLTO',DoubleType(),True),\
StructField('VABME',StringType(),True),\
StructField('KZREV',StringType(),True),\
StructField('KZKFG',StringType(),True),\
StructField('XCHPF',StringType(),True),\
StructField('VHART',StringType(),True),\
StructField('FUELG',IntegerType(),True),\
StructField('STFAK',IntegerType(),True),\
StructField('MAGRV',StringType(),True),\
StructField('BEGRU',StringType(),True),\
StructField('DATAB',StringType(),True),\
StructField('LIQDT',StringType(),True),\
StructField('SAISJ',StringType(),True),\
StructField('PLGTP',StringType(),True),\
StructField('MLGUT',StringType(),True),\
StructField('EXTWG',StringType(),True),\
StructField('SATNR',StringType(),True),\
StructField('ATTYP',StringType(),True),\
StructField('KZKUP',StringType(),True),\
StructField('KZNFM',StringType(),True),\
StructField('PMATA',StringType(),True),\
StructField('MSTAE',StringType(),True),\
StructField('MSTAV',StringType(),True),\
StructField('MSTDE',StringType(),True),\
StructField('MSTDV',StringType(),True),\
StructField('TAKLV',StringType(),True),\
StructField('RBNRM',StringType(),True),\
StructField('MHDRZ',IntegerType(),True),\
StructField('MHDHB',IntegerType(),True),\
StructField('MHDLP',IntegerType(),True),\
StructField('INHME',StringType(),True),\
StructField('INHAL',DoubleType(),True),\
StructField('VPREH',IntegerType(),True),\
StructField('ETIAG',StringType(),True),\
StructField('INHBR',DoubleType(),True),\
StructField('CMETH',StringType(),True),\
StructField('CUOBF',IntegerType(),True),\
StructField('KZUMW',StringType(),True),\
StructField('KOSCH',StringType(),True),\
StructField('SPROF',StringType(),True),\
StructField('NRFHG',StringType(),True),\
StructField('MFRPN',StringType(),True),\
StructField('MFRNR',StringType(),True),\
StructField('BMATN',StringType(),True),\
StructField('MPROF',StringType(),True),\
StructField('KZWSM',StringType(),True),\
StructField('SAITY',StringType(),True),\
StructField('PROFL',StringType(),True),\
StructField('IHIVI',StringType(),True),\
StructField('ILOOS',StringType(),True),\
StructField('SERLV',StringType(),True),\
StructField('KZGVH',StringType(),True),\
StructField('XGCHP',StringType(),True),\
StructField('KZEFF',StringType(),True),\
StructField('COMPL',IntegerType(),True),\
StructField('IPRKZ',StringType(),True),\
StructField('RDMHD',StringType(),True),\
StructField('PRZUS',StringType(),True),\
StructField('MTPOS_MARA',StringType(),True),\
StructField('BFLME',StringType(),True),\
StructField('MATFI',StringType(),True),\
StructField('CMREL',StringType(),True),\
StructField('BBTYP',StringType(),True),\
StructField('SLED_BBD',StringType(),True),\
StructField('GTIN_VARIANT',StringType(),True),\
StructField('GENNR',StringType(),True),\
StructField('RMATP',StringType(),True),\
StructField('GDS_RELEVANT',StringType(),True),\
StructField('WEORA',StringType(),True),\
StructField('HUTYP_DFLT',StringType(),True),\
StructField('PILFERABLE',StringType(),True),\
StructField('WHSTC',StringType(),True),\
StructField('WHMATGR',StringType(),True),\
StructField('HNDLCODE',StringType(),True),\
StructField('HAZMAT',StringType(),True),\
StructField('HUTYP',StringType(),True),\
StructField('TARE_VAR',StringType(),True),\
StructField('MAXC',DoubleType(),True),\
StructField('MAXC_TOL',DoubleType(),True),\
StructField('MAXL',DoubleType(),True),\
StructField('MAXB',DoubleType(),True),\
StructField('MAXH',DoubleType(),True),\
StructField('MAXDIM_UOM',StringType(),True),\
StructField('HERKL',StringType(),True),\
StructField('MFRGR',StringType(),True),\
StructField('QQTIME',IntegerType(),True),\
StructField('QQTIMEUOM',StringType(),True),\
StructField('QGRP',StringType(),True),\
StructField('SERIAL',StringType(),True),\
StructField('PS_SMARTFORM',StringType(),True),\
StructField('LOGUNIT',StringType(),True),\
StructField('CWQREL',StringType(),True),\
StructField('CWQPROC',StringType(),True),\
StructField('CWQTOLGR',StringType(),True),\
StructField('ADPROF',StringType(),True),\
StructField('IPMIPPRODUCT',StringType(),True),\
StructField('ALLOW_PMAT_IGNO',StringType(),True),\
StructField('MEDIUM',StringType(),True),\
StructField('COMMODITY',StringType(),True),\
StructField('ANIMAL_ORIGIN',StringType(),True),\
StructField('TEXTILE_COMP_IND',StringType(),True),\
StructField('LAST_CHANGED_TIME',TimestampType(),True),\
StructField('MATNR_EXTERNAL',StringType(),True),\
StructField('CHML_CMPLNC_RLVNCE_IND',StringType(),True),\
StructField('LOGISTICAL_MAT_CATEGORY',StringType(),True),\
StructField('SALES_MATERIAL',StringType(),True),\
StructField('IDENTIFICATION_TAG_TYPE',StringType(),True),\
StructField('SGT_CSGR',StringType(),True),\
StructField('SGT_COVSA',StringType(),True),\
StructField('SGT_STAT',StringType(),True),\
StructField('SGT_SCOPE',StringType(),True),\
StructField('SGT_REL',StringType(),True),\
StructField('ANP',IntegerType(),True),\
StructField('PSM_CODE',StringType(),True),\
StructField('FSH_MG_AT1',StringType(),True),\
StructField('FSH_MG_AT2',StringType(),True),\
StructField('FSH_MG_AT3',StringType(),True),\
StructField('FSH_SEALV',StringType(),True),\
StructField('FSH_SEAIM',StringType(),True),\
StructField('FSH_SC_MID',StringType(),True),\
StructField('DUMMY_PRD_INCL_EEW_PS',StringType(),True),\
StructField('SCM_MATID_GUID16',StringType(),True),\
StructField('SCM_MATID_GUID22',StringType(),True),\
StructField('SCM_MATURITY_DUR',IntegerType(),True),\
StructField('SCM_SHLF_LFE_REQ_MIN',IntegerType(),True),\
StructField('SCM_SHLF_LFE_REQ_MAX',IntegerType(),True),\
StructField('SCM_PUOM',StringType(),True),\
StructField('RMATP_PB',StringType(),True),\
StructField('PROD_SHAPE',StringType(),True),\
StructField('MO_PROFILE_ID',StringType(),True),\
StructField('OVERHANG_TRESH',DoubleType(),True),\
StructField('BRIDGE_TRESH',DoubleType(),True),\
StructField('BRIDGE_MAX_SLOPE',DoubleType(),True),\
StructField('HEIGHT_NONFLAT',DoubleType(),True),\
StructField('HEIGHT_NONFLAT_UOM',StringType(),True),\
StructField('SCM_KITCOMP',StringType(),True),\
StructField('SCM_PROD_PAOOPT',StringType(),True),\
StructField('SCM_BOD_DEPLVL',StringType(),True),\
StructField('SCM_RESTRICT_INVBAL',StringType(),True),\
StructField('SCM_DRP_GL_STOCK',StringType(),True),\
StructField('SCM_EXCL_EXPEDITE',StringType(),True),\
StructField('/CWM/XCWMAT',StringType(),True),\
StructField('/CWM/VALUM',StringType(),True),\
StructField('/CWM/TOLGR',StringType(),True),\
StructField('/CWM/TARA',StringType(),True),\
StructField('/CWM/TARUM',StringType(),True),\
StructField('/BEV1/LULEINH',IntegerType(),True),\
StructField('/BEV1/LULDEGRP',StringType(),True),\
StructField('/BEV1/NESTRUCCAT',StringType(),True),\
StructField('/DSD/SL_TOLTYP',StringType(),True),\
StructField('/DSD/SV_CNT_GRP',StringType(),True),\
StructField('/DSD/VC_GROUP',StringType(),True),\
StructField('/SAPMP/KADU',DoubleType(),True),\
StructField('/SAPMP/ABMEIN',StringType(),True),\
StructField('/SAPMP/KADP',DoubleType(),True),\
StructField('/SAPMP/BRAD',IntegerType(),True),\
StructField('/SAPMP/SPBI',DoubleType(),True),\
StructField('/SAPMP/TRAD',DoubleType(),True),\
StructField('/SAPMP/KEDU',DoubleType(),True),\
StructField('/SAPMP/SPTR',DoubleType(),True),\
StructField('/SAPMP/FBDK',DoubleType(),True),\
StructField('/SAPMP/FBHK',DoubleType(),True),\
StructField('/SAPMP/RILI',StringType(),True),\
StructField('/SAPMP/FBAK',StringType(),True),\
StructField('/SAPMP/AHO',IntegerType(),True),\
StructField('/SAPMP/MIFRR',DoubleType(),True),\
StructField('/STTPEC/SERTYPE',IntegerType(),True),\
StructField('/STTPEC/SYNCACT',StringType(),True),\
StructField('/STTPEC/SYNCTIME',IntegerType(),True),\
StructField('/STTPEC/SYNCCHG',StringType(),True),\
StructField('/STTPEC/COUNTRY_REF',StringType(),True),\
StructField('/STTPEC/PRDCAT',StringType(),True),\
StructField('/VSO/R_TILT_IND',StringType(),True),\
StructField('/VSO/R_STACK_IND',StringType(),True),\
StructField('/VSO/R_BOT_IND',StringType(),True),\
StructField('/VSO/R_TOP_IND',StringType(),True),\
StructField('/VSO/R_STACK_NO',IntegerType(),True),\
StructField('/VSO/R_PAL_IND',StringType(),True),\
StructField('/VSO/R_PAL_OVR_D',DoubleType(),True),\
StructField('/VSO/R_PAL_OVR_W',DoubleType(),True),\
StructField('/VSO/R_PAL_B_HT',DoubleType(),True),\
StructField('/VSO/R_PAL_MIN_H',DoubleType(),True),\
StructField('/VSO/R_TOL_B_HT',DoubleType(),True),\
StructField('/VSO/R_NO_P_GVH',IntegerType(),True),\
StructField('/VSO/R_QUAN_UNIT',StringType(),True),\
StructField('/VSO/R_KZGVH_IND',StringType(),True),\
StructField('PACKCODE',StringType(),True),\
StructField('DG_PACK_STATUS',StringType(),True),\
StructField('SRV_DURA',DoubleType(),True),\
StructField('SRV_DURA_UOM',StringType(),True),\
StructField('SRV_SERWI',StringType(),True),\
StructField('SRV_ESCAL',StringType(),True),\
StructField('SOM_CYCLE',StringType(),True),\
StructField('SOM_CYCLE_RULE',StringType(),True),\
StructField('SOM_TC_SCHEMA',StringType(),True),\
StructField('SOM_CTR_AUTORENEWAL',StringType(),True),\
StructField('MCOND',StringType(),True),\
StructField('RETDELC',StringType(),True),\
StructField('LOGLEV_RETO',StringType(),True),\
StructField('NSNID',StringType(),True),\
StructField('ICFA',StringType(),True),\
StructField('RIC_ID',IntegerType(),True),\
StructField('DFS_SENSITIVITY_KEY',StringType(),True),\
StructField('DFS_MFRP2',StringType(),True),\
StructField('OVLPN',StringType(),True),\
StructField('ADSPC_SPC',IntegerType(),True),\
StructField('VARID',IntegerType(),True),\
StructField('MSBOOKPARTNO',StringType(),True),\
StructField('TOLERANCE_TYPE',StringType(),True),\
StructField('DPCBT',StringType(),True),\
StructField('XGRDT',StringType(),True),\
StructField('IMATN',StringType(),True),\
StructField('PICNUM',StringType(),True),\
StructField('BSTAT',StringType(),True),\
StructField('COLOR_ATINN',IntegerType(),True),\
StructField('SIZE1_ATINN',IntegerType(),True),\
StructField('SIZE2_ATINN',IntegerType(),True),\
StructField('COLOR',StringType(),True),\
StructField('SIZE1',StringType(),True),\
StructField('SIZE2',StringType(),True),\
StructField('FREE_CHAR',StringType(),True),\
StructField('CARE_CODE',StringType(),True),\
StructField('BRAND_ID',StringType(),True),\
StructField('FIBER_CODE1',StringType(),True),\
StructField('FIBER_PART1',IntegerType(),True),\
StructField('FIBER_CODE2',StringType(),True),\
StructField('FIBER_PART2',IntegerType(),True),\
StructField('FIBER_CODE3',StringType(),True),\
StructField('FIBER_PART3',IntegerType(),True),\
StructField('FIBER_CODE4',StringType(),True),\
StructField('FIBER_PART4',IntegerType(),True),\
StructField('FIBER_CODE5',StringType(),True),\
StructField('FIBER_PART5',IntegerType(),True),\
StructField('FASHGRD',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
                    ])

# COMMAND ----------

table_name = 'MARA'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'


read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.options(header='True', inferSchema='True', delimiter='^').csv(read_path)

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
                            .withColumn("ERSDA", to_date(regexp_replace(df_add_column.ERSDA,'\.','-'))) \
                            .withColumn("LAEDA", to_date(regexp_replace(df_add_column.LAEDA,'\.','-'))) \
                            .withColumn("DATAB", to_date(regexp_replace(df_add_column.DATAB,'\.','-'))) \
                            .withColumn("LIQDT", to_date(regexp_replace(df_add_column.LIQDT,'\.','-'))) \
                            .withColumn("MSTDE", to_date(regexp_replace(df_add_column.MSTDE,'\.','-'))) \
                            .withColumn("MSTDV", to_date(regexp_replace(df_add_column.MSTDV,'\.','-'))) \
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
# MAGIC MERGE INTO S42.MARA as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,MATNR ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_MARA where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and 
# MAGIC T.MATNR = S.MATNR
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`DI_SEQUENCE_NUMBER` = S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` = S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` = S.`MANDT`,
# MAGIC T.`MATNR` = S.`MATNR`,
# MAGIC T.`ERSDA` = S.`ERSDA`,
# MAGIC T.`CREATED_AT_TIME` = S.`CREATED_AT_TIME`,
# MAGIC T.`ERNAM` = S.`ERNAM`,
# MAGIC T.`LAEDA` = S.`LAEDA`,
# MAGIC T.`AENAM` = S.`AENAM`,
# MAGIC T.`VPSTA` = S.`VPSTA`,
# MAGIC T.`PSTAT` = S.`PSTAT`,
# MAGIC T.`LVORM` = S.`LVORM`,
# MAGIC T.`MTART` = S.`MTART`,
# MAGIC T.`MBRSH` = S.`MBRSH`,
# MAGIC T.`MATKL` = S.`MATKL`,
# MAGIC T.`BISMT` = S.`BISMT`,
# MAGIC T.`MEINS` = S.`MEINS`,
# MAGIC T.`BSTME` = S.`BSTME`,
# MAGIC T.`ZEINR` = S.`ZEINR`,
# MAGIC T.`ZEIAR` = S.`ZEIAR`,
# MAGIC T.`ZEIVR` = S.`ZEIVR`,
# MAGIC T.`ZEIFO` = S.`ZEIFO`,
# MAGIC T.`AESZN` = S.`AESZN`,
# MAGIC T.`BLATT` = S.`BLATT`,
# MAGIC T.`BLANZ` = S.`BLANZ`,
# MAGIC T.`FERTH` = S.`FERTH`,
# MAGIC T.`FORMT` = S.`FORMT`,
# MAGIC T.`GROES` = S.`GROES`,
# MAGIC T.`WRKST` = S.`WRKST`,
# MAGIC T.`NORMT` = S.`NORMT`,
# MAGIC T.`LABOR` = S.`LABOR`,
# MAGIC T.`EKWSL` = S.`EKWSL`,
# MAGIC T.`BRGEW` = S.`BRGEW`,
# MAGIC T.`NTGEW` = S.`NTGEW`,
# MAGIC T.`GEWEI` = S.`GEWEI`,
# MAGIC T.`VOLUM` = S.`VOLUM`,
# MAGIC T.`VOLEH` = S.`VOLEH`,
# MAGIC T.`BEHVO` = S.`BEHVO`,
# MAGIC T.`RAUBE` = S.`RAUBE`,
# MAGIC T.`TEMPB` = S.`TEMPB`,
# MAGIC T.`DISST` = S.`DISST`,
# MAGIC T.`TRAGR` = S.`TRAGR`,
# MAGIC T.`STOFF` = S.`STOFF`,
# MAGIC T.`SPART` = S.`SPART`,
# MAGIC T.`KUNNR` = S.`KUNNR`,
# MAGIC T.`EANNR` = S.`EANNR`,
# MAGIC T.`WESCH` = S.`WESCH`,
# MAGIC T.`BWVOR` = S.`BWVOR`,
# MAGIC T.`BWSCL` = S.`BWSCL`,
# MAGIC T.`SAISO` = S.`SAISO`,
# MAGIC T.`ETIAR` = S.`ETIAR`,
# MAGIC T.`ETIFO` = S.`ETIFO`,
# MAGIC T.`ENTAR` = S.`ENTAR`,
# MAGIC T.`EAN11` = S.`EAN11`,
# MAGIC T.`NUMTP` = S.`NUMTP`,
# MAGIC T.`LAENG` = S.`LAENG`,
# MAGIC T.`BREIT` = S.`BREIT`,
# MAGIC T.`HOEHE` = S.`HOEHE`,
# MAGIC T.`MEABM` = S.`MEABM`,
# MAGIC T.`PRDHA` = S.`PRDHA`,
# MAGIC T.`AEKLK` = S.`AEKLK`,
# MAGIC T.`CADKZ` = S.`CADKZ`,
# MAGIC T.`QMPUR` = S.`QMPUR`,
# MAGIC T.`ERGEW` = S.`ERGEW`,
# MAGIC T.`ERGEI` = S.`ERGEI`,
# MAGIC T.`ERVOL` = S.`ERVOL`,
# MAGIC T.`ERVOE` = S.`ERVOE`,
# MAGIC T.`GEWTO` = S.`GEWTO`,
# MAGIC T.`VOLTO` = S.`VOLTO`,
# MAGIC T.`VABME` = S.`VABME`,
# MAGIC T.`KZREV` = S.`KZREV`,
# MAGIC T.`KZKFG` = S.`KZKFG`,
# MAGIC T.`XCHPF` = S.`XCHPF`,
# MAGIC T.`VHART` = S.`VHART`,
# MAGIC T.`FUELG` = S.`FUELG`,
# MAGIC T.`STFAK` = S.`STFAK`,
# MAGIC T.`MAGRV` = S.`MAGRV`,
# MAGIC T.`BEGRU` = S.`BEGRU`,
# MAGIC T.`DATAB` = S.`DATAB`,
# MAGIC T.`LIQDT` = S.`LIQDT`,
# MAGIC T.`SAISJ` = S.`SAISJ`,
# MAGIC T.`PLGTP` = S.`PLGTP`,
# MAGIC T.`MLGUT` = S.`MLGUT`,
# MAGIC T.`EXTWG` = S.`EXTWG`,
# MAGIC T.`SATNR` = S.`SATNR`,
# MAGIC T.`ATTYP` = S.`ATTYP`,
# MAGIC T.`KZKUP` = S.`KZKUP`,
# MAGIC T.`KZNFM` = S.`KZNFM`,
# MAGIC T.`PMATA` = S.`PMATA`,
# MAGIC T.`MSTAE` = S.`MSTAE`,
# MAGIC T.`MSTAV` = S.`MSTAV`,
# MAGIC T.`MSTDE` = S.`MSTDE`,
# MAGIC T.`MSTDV` = S.`MSTDV`,
# MAGIC T.`TAKLV` = S.`TAKLV`,
# MAGIC T.`RBNRM` = S.`RBNRM`,
# MAGIC T.`MHDRZ` = S.`MHDRZ`,
# MAGIC T.`MHDHB` = S.`MHDHB`,
# MAGIC T.`MHDLP` = S.`MHDLP`,
# MAGIC T.`INHME` = S.`INHME`,
# MAGIC T.`INHAL` = S.`INHAL`,
# MAGIC T.`VPREH` = S.`VPREH`,
# MAGIC T.`ETIAG` = S.`ETIAG`,
# MAGIC T.`INHBR` = S.`INHBR`,
# MAGIC T.`CMETH` = S.`CMETH`,
# MAGIC T.`CUOBF` = S.`CUOBF`,
# MAGIC T.`KZUMW` = S.`KZUMW`,
# MAGIC T.`KOSCH` = S.`KOSCH`,
# MAGIC T.`SPROF` = S.`SPROF`,
# MAGIC T.`NRFHG` = S.`NRFHG`,
# MAGIC T.`MFRPN` = S.`MFRPN`,
# MAGIC T.`MFRNR` = S.`MFRNR`,
# MAGIC T.`BMATN` = S.`BMATN`,
# MAGIC T.`MPROF` = S.`MPROF`,
# MAGIC T.`KZWSM` = S.`KZWSM`,
# MAGIC T.`SAITY` = S.`SAITY`,
# MAGIC T.`PROFL` = S.`PROFL`,
# MAGIC T.`IHIVI` = S.`IHIVI`,
# MAGIC T.`ILOOS` = S.`ILOOS`,
# MAGIC T.`SERLV` = S.`SERLV`,
# MAGIC T.`KZGVH` = S.`KZGVH`,
# MAGIC T.`XGCHP` = S.`XGCHP`,
# MAGIC T.`KZEFF` = S.`KZEFF`,
# MAGIC T.`COMPL` = S.`COMPL`,
# MAGIC T.`IPRKZ` = S.`IPRKZ`,
# MAGIC T.`RDMHD` = S.`RDMHD`,
# MAGIC T.`PRZUS` = S.`PRZUS`,
# MAGIC T.`MTPOS_MARA` = S.`MTPOS_MARA`,
# MAGIC T.`BFLME` = S.`BFLME`,
# MAGIC T.`MATFI` = S.`MATFI`,
# MAGIC T.`CMREL` = S.`CMREL`,
# MAGIC T.`BBTYP` = S.`BBTYP`,
# MAGIC T.`SLED_BBD` = S.`SLED_BBD`,
# MAGIC T.`GTIN_VARIANT` = S.`GTIN_VARIANT`,
# MAGIC T.`GENNR` = S.`GENNR`,
# MAGIC T.`RMATP` = S.`RMATP`,
# MAGIC T.`GDS_RELEVANT` = S.`GDS_RELEVANT`,
# MAGIC T.`WEORA` = S.`WEORA`,
# MAGIC T.`HUTYP_DFLT` = S.`HUTYP_DFLT`,
# MAGIC T.`PILFERABLE` = S.`PILFERABLE`,
# MAGIC T.`WHSTC` = S.`WHSTC`,
# MAGIC T.`WHMATGR` = S.`WHMATGR`,
# MAGIC T.`HNDLCODE` = S.`HNDLCODE`,
# MAGIC T.`HAZMAT` = S.`HAZMAT`,
# MAGIC T.`HUTYP` = S.`HUTYP`,
# MAGIC T.`TARE_VAR` = S.`TARE_VAR`,
# MAGIC T.`MAXC` = S.`MAXC`,
# MAGIC T.`MAXC_TOL` = S.`MAXC_TOL`,
# MAGIC T.`MAXL` = S.`MAXL`,
# MAGIC T.`MAXB` = S.`MAXB`,
# MAGIC T.`MAXH` = S.`MAXH`,
# MAGIC T.`MAXDIM_UOM` = S.`MAXDIM_UOM`,
# MAGIC T.`HERKL` = S.`HERKL`,
# MAGIC T.`MFRGR` = S.`MFRGR`,
# MAGIC T.`QQTIME` = S.`QQTIME`,
# MAGIC T.`QQTIMEUOM` = S.`QQTIMEUOM`,
# MAGIC T.`QGRP` = S.`QGRP`,
# MAGIC T.`SERIAL` = S.`SERIAL`,
# MAGIC T.`PS_SMARTFORM` = S.`PS_SMARTFORM`,
# MAGIC T.`LOGUNIT` = S.`LOGUNIT`,
# MAGIC T.`CWQREL` = S.`CWQREL`,
# MAGIC T.`CWQPROC` = S.`CWQPROC`,
# MAGIC T.`CWQTOLGR` = S.`CWQTOLGR`,
# MAGIC T.`ADPROF` = S.`ADPROF`,
# MAGIC T.`IPMIPPRODUCT` = S.`IPMIPPRODUCT`,
# MAGIC T.`ALLOW_PMAT_IGNO` = S.`ALLOW_PMAT_IGNO`,
# MAGIC T.`MEDIUM` = S.`MEDIUM`,
# MAGIC T.`COMMODITY` = S.`COMMODITY`,
# MAGIC T.`ANIMAL_ORIGIN` = S.`ANIMAL_ORIGIN`,
# MAGIC T.`TEXTILE_COMP_IND` = S.`TEXTILE_COMP_IND`,
# MAGIC T.`LAST_CHANGED_TIME` = S.`LAST_CHANGED_TIME`,
# MAGIC T.`MATNR_EXTERNAL` = S.`MATNR_EXTERNAL`,
# MAGIC T.`CHML_CMPLNC_RLVNCE_IND` = S.`CHML_CMPLNC_RLVNCE_IND`,
# MAGIC T.`LOGISTICAL_MAT_CATEGORY` = S.`LOGISTICAL_MAT_CATEGORY`,
# MAGIC T.`SALES_MATERIAL` = S.`SALES_MATERIAL`,
# MAGIC T.`IDENTIFICATION_TAG_TYPE` = S.`IDENTIFICATION_TAG_TYPE`,
# MAGIC T.`SGT_CSGR` = S.`SGT_CSGR`,
# MAGIC T.`SGT_COVSA` = S.`SGT_COVSA`,
# MAGIC T.`SGT_STAT` = S.`SGT_STAT`,
# MAGIC T.`SGT_SCOPE` = S.`SGT_SCOPE`,
# MAGIC T.`SGT_REL` = S.`SGT_REL`,
# MAGIC T.`ANP` = S.`ANP`,
# MAGIC T.`PSM_CODE` = S.`PSM_CODE`,
# MAGIC T.`FSH_MG_AT1` = S.`FSH_MG_AT1`,
# MAGIC T.`FSH_MG_AT2` = S.`FSH_MG_AT2`,
# MAGIC T.`FSH_MG_AT3` = S.`FSH_MG_AT3`,
# MAGIC T.`FSH_SEALV` = S.`FSH_SEALV`,
# MAGIC T.`FSH_SEAIM` = S.`FSH_SEAIM`,
# MAGIC T.`FSH_SC_MID` = S.`FSH_SC_MID`,
# MAGIC T.`DUMMY_PRD_INCL_EEW_PS` = S.`DUMMY_PRD_INCL_EEW_PS`,
# MAGIC T.`SCM_MATID_GUID16` = S.`SCM_MATID_GUID16`,
# MAGIC T.`SCM_MATID_GUID22` = S.`SCM_MATID_GUID22`,
# MAGIC T.`SCM_MATURITY_DUR` = S.`SCM_MATURITY_DUR`,
# MAGIC T.`SCM_SHLF_LFE_REQ_MIN` = S.`SCM_SHLF_LFE_REQ_MIN`,
# MAGIC T.`SCM_SHLF_LFE_REQ_MAX` = S.`SCM_SHLF_LFE_REQ_MAX`,
# MAGIC T.`SCM_PUOM` = S.`SCM_PUOM`,
# MAGIC T.`RMATP_PB` = S.`RMATP_PB`,
# MAGIC T.`PROD_SHAPE` = S.`PROD_SHAPE`,
# MAGIC T.`MO_PROFILE_ID` = S.`MO_PROFILE_ID`,
# MAGIC T.`OVERHANG_TRESH` = S.`OVERHANG_TRESH`,
# MAGIC T.`BRIDGE_TRESH` = S.`BRIDGE_TRESH`,
# MAGIC T.`BRIDGE_MAX_SLOPE` = S.`BRIDGE_MAX_SLOPE`,
# MAGIC T.`HEIGHT_NONFLAT` = S.`HEIGHT_NONFLAT`,
# MAGIC T.`HEIGHT_NONFLAT_UOM` = S.`HEIGHT_NONFLAT_UOM`,
# MAGIC T.`SCM_KITCOMP` = S.`SCM_KITCOMP`,
# MAGIC T.`SCM_PROD_PAOOPT` = S.`SCM_PROD_PAOOPT`,
# MAGIC T.`SCM_BOD_DEPLVL` = S.`SCM_BOD_DEPLVL`,
# MAGIC T.`SCM_RESTRICT_INVBAL` = S.`SCM_RESTRICT_INVBAL`,
# MAGIC T.`SCM_DRP_GL_STOCK` = S.`SCM_DRP_GL_STOCK`,
# MAGIC T.`SCM_EXCL_EXPEDITE` = S.`SCM_EXCL_EXPEDITE`,
# MAGIC T.`/CWM/XCWMAT` = S.`/CWM/XCWMAT`,
# MAGIC T.`/CWM/VALUM` = S.`/CWM/VALUM`,
# MAGIC T.`/CWM/TOLGR` = S.`/CWM/TOLGR`,
# MAGIC T.`/CWM/TARA` = S.`/CWM/TARA`,
# MAGIC T.`/CWM/TARUM` = S.`/CWM/TARUM`,
# MAGIC T.`/BEV1/LULEINH` = S.`/BEV1/LULEINH`,
# MAGIC T.`/BEV1/LULDEGRP` = S.`/BEV1/LULDEGRP`,
# MAGIC T.`/BEV1/NESTRUCCAT` = S.`/BEV1/NESTRUCCAT`,
# MAGIC T.`/DSD/SL_TOLTYP` = S.`/DSD/SL_TOLTYP`,
# MAGIC T.`/DSD/SV_CNT_GRP` = S.`/DSD/SV_CNT_GRP`,
# MAGIC T.`/DSD/VC_GROUP` = S.`/DSD/VC_GROUP`,
# MAGIC T.`/SAPMP/KADU` = S.`/SAPMP/KADU`,
# MAGIC T.`/SAPMP/ABMEIN` = S.`/SAPMP/ABMEIN`,
# MAGIC T.`/SAPMP/KADP` = S.`/SAPMP/KADP`,
# MAGIC T.`/SAPMP/BRAD` = S.`/SAPMP/BRAD`,
# MAGIC T.`/SAPMP/SPBI` = S.`/SAPMP/SPBI`,
# MAGIC T.`/SAPMP/TRAD` = S.`/SAPMP/TRAD`,
# MAGIC T.`/SAPMP/KEDU` = S.`/SAPMP/KEDU`,
# MAGIC T.`/SAPMP/SPTR` = S.`/SAPMP/SPTR`,
# MAGIC T.`/SAPMP/FBDK` = S.`/SAPMP/FBDK`,
# MAGIC T.`/SAPMP/FBHK` = S.`/SAPMP/FBHK`,
# MAGIC T.`/SAPMP/RILI` = S.`/SAPMP/RILI`,
# MAGIC T.`/SAPMP/FBAK` = S.`/SAPMP/FBAK`,
# MAGIC T.`/SAPMP/AHO` = S.`/SAPMP/AHO`,
# MAGIC T.`/SAPMP/MIFRR` = S.`/SAPMP/MIFRR`,
# MAGIC T.`/STTPEC/SERTYPE` = S.`/STTPEC/SERTYPE`,
# MAGIC T.`/STTPEC/SYNCACT` = S.`/STTPEC/SYNCACT`,
# MAGIC T.`/STTPEC/SYNCTIME` = S.`/STTPEC/SYNCTIME`,
# MAGIC T.`/STTPEC/SYNCCHG` = S.`/STTPEC/SYNCCHG`,
# MAGIC T.`/STTPEC/COUNTRY_REF` = S.`/STTPEC/COUNTRY_REF`,
# MAGIC T.`/STTPEC/PRDCAT` = S.`/STTPEC/PRDCAT`,
# MAGIC T.`/VSO/R_TILT_IND` = S.`/VSO/R_TILT_IND`,
# MAGIC T.`/VSO/R_STACK_IND` = S.`/VSO/R_STACK_IND`,
# MAGIC T.`/VSO/R_BOT_IND` = S.`/VSO/R_BOT_IND`,
# MAGIC T.`/VSO/R_TOP_IND` = S.`/VSO/R_TOP_IND`,
# MAGIC T.`/VSO/R_STACK_NO` = S.`/VSO/R_STACK_NO`,
# MAGIC T.`/VSO/R_PAL_IND` = S.`/VSO/R_PAL_IND`,
# MAGIC T.`/VSO/R_PAL_OVR_D` = S.`/VSO/R_PAL_OVR_D`,
# MAGIC T.`/VSO/R_PAL_OVR_W` = S.`/VSO/R_PAL_OVR_W`,
# MAGIC T.`/VSO/R_PAL_B_HT` = S.`/VSO/R_PAL_B_HT`,
# MAGIC T.`/VSO/R_PAL_MIN_H` = S.`/VSO/R_PAL_MIN_H`,
# MAGIC T.`/VSO/R_TOL_B_HT` = S.`/VSO/R_TOL_B_HT`,
# MAGIC T.`/VSO/R_NO_P_GVH` = S.`/VSO/R_NO_P_GVH`,
# MAGIC T.`/VSO/R_QUAN_UNIT` = S.`/VSO/R_QUAN_UNIT`,
# MAGIC T.`/VSO/R_KZGVH_IND` = S.`/VSO/R_KZGVH_IND`,
# MAGIC T.`PACKCODE` = S.`PACKCODE`,
# MAGIC T.`DG_PACK_STATUS` = S.`DG_PACK_STATUS`,
# MAGIC T.`SRV_DURA` = S.`SRV_DURA`,
# MAGIC T.`SRV_DURA_UOM` = S.`SRV_DURA_UOM`,
# MAGIC T.`SRV_SERWI` = S.`SRV_SERWI`,
# MAGIC T.`SRV_ESCAL` = S.`SRV_ESCAL`,
# MAGIC T.`SOM_CYCLE` = S.`SOM_CYCLE`,
# MAGIC T.`SOM_CYCLE_RULE` = S.`SOM_CYCLE_RULE`,
# MAGIC T.`SOM_TC_SCHEMA` = S.`SOM_TC_SCHEMA`,
# MAGIC T.`SOM_CTR_AUTORENEWAL` = S.`SOM_CTR_AUTORENEWAL`,
# MAGIC T.`MCOND` = S.`MCOND`,
# MAGIC T.`RETDELC` = S.`RETDELC`,
# MAGIC T.`LOGLEV_RETO` = S.`LOGLEV_RETO`,
# MAGIC T.`NSNID` = S.`NSNID`,
# MAGIC T.`ICFA` = S.`ICFA`,
# MAGIC T.`RIC_ID` = S.`RIC_ID`,
# MAGIC T.`DFS_SENSITIVITY_KEY` = S.`DFS_SENSITIVITY_KEY`,
# MAGIC T.`DFS_MFRP2` = S.`DFS_MFRP2`,
# MAGIC T.`OVLPN` = S.`OVLPN`,
# MAGIC T.`ADSPC_SPC` = S.`ADSPC_SPC`,
# MAGIC T.`VARID` = S.`VARID`,
# MAGIC T.`MSBOOKPARTNO` = S.`MSBOOKPARTNO`,
# MAGIC T.`TOLERANCE_TYPE` = S.`TOLERANCE_TYPE`,
# MAGIC T.`DPCBT` = S.`DPCBT`,
# MAGIC T.`XGRDT` = S.`XGRDT`,
# MAGIC T.`IMATN` = S.`IMATN`,
# MAGIC T.`PICNUM` = S.`PICNUM`,
# MAGIC T.`BSTAT` = S.`BSTAT`,
# MAGIC T.`COLOR_ATINN` = S.`COLOR_ATINN`,
# MAGIC T.`SIZE1_ATINN` = S.`SIZE1_ATINN`,
# MAGIC T.`SIZE2_ATINN` = S.`SIZE2_ATINN`,
# MAGIC T.`COLOR` = S.`COLOR`,
# MAGIC T.`SIZE1` = S.`SIZE1`,
# MAGIC T.`SIZE2` = S.`SIZE2`,
# MAGIC T.`FREE_CHAR` = S.`FREE_CHAR`,
# MAGIC T.`CARE_CODE` = S.`CARE_CODE`,
# MAGIC T.`BRAND_ID` = S.`BRAND_ID`,
# MAGIC T.`FIBER_CODE1` = S.`FIBER_CODE1`,
# MAGIC T.`FIBER_PART1` = S.`FIBER_PART1`,
# MAGIC T.`FIBER_CODE2` = S.`FIBER_CODE2`,
# MAGIC T.`FIBER_PART2` = S.`FIBER_PART2`,
# MAGIC T.`FIBER_CODE3` = S.`FIBER_CODE3`,
# MAGIC T.`FIBER_PART3` = S.`FIBER_PART3`,
# MAGIC T.`FIBER_CODE4` = S.`FIBER_CODE4`,
# MAGIC T.`FIBER_PART4` = S.`FIBER_PART4`,
# MAGIC T.`FIBER_CODE5` = S.`FIBER_CODE5`,
# MAGIC T.`FIBER_PART5` = S.`FIBER_PART5`,
# MAGIC T.`FASHGRD` = S.`FASHGRD`,
# MAGIC T.`ODQ_CHANGEMODE` = S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` = S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` = S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC  `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `MATNR`,
# MAGIC `ERSDA`,
# MAGIC `CREATED_AT_TIME`,
# MAGIC `ERNAM`,
# MAGIC `LAEDA`,
# MAGIC `AENAM`,
# MAGIC `VPSTA`,
# MAGIC `PSTAT`,
# MAGIC `LVORM`,
# MAGIC `MTART`,
# MAGIC `MBRSH`,
# MAGIC `MATKL`,
# MAGIC `BISMT`,
# MAGIC `MEINS`,
# MAGIC `BSTME`,
# MAGIC `ZEINR`,
# MAGIC `ZEIAR`,
# MAGIC `ZEIVR`,
# MAGIC `ZEIFO`,
# MAGIC `AESZN`,
# MAGIC `BLATT`,
# MAGIC `BLANZ`,
# MAGIC `FERTH`,
# MAGIC `FORMT`,
# MAGIC `GROES`,
# MAGIC `WRKST`,
# MAGIC `NORMT`,
# MAGIC `LABOR`,
# MAGIC `EKWSL`,
# MAGIC `BRGEW`,
# MAGIC `NTGEW`,
# MAGIC `GEWEI`,
# MAGIC `VOLUM`,
# MAGIC `VOLEH`,
# MAGIC `BEHVO`,
# MAGIC `RAUBE`,
# MAGIC `TEMPB`,
# MAGIC `DISST`,
# MAGIC `TRAGR`,
# MAGIC `STOFF`,
# MAGIC `SPART`,
# MAGIC `KUNNR`,
# MAGIC `EANNR`,
# MAGIC `WESCH`,
# MAGIC `BWVOR`,
# MAGIC `BWSCL`,
# MAGIC `SAISO`,
# MAGIC `ETIAR`,
# MAGIC `ETIFO`,
# MAGIC `ENTAR`,
# MAGIC `EAN11`,
# MAGIC `NUMTP`,
# MAGIC `LAENG`,
# MAGIC `BREIT`,
# MAGIC `HOEHE`,
# MAGIC `MEABM`,
# MAGIC `PRDHA`,
# MAGIC `AEKLK`,
# MAGIC `CADKZ`,
# MAGIC `QMPUR`,
# MAGIC `ERGEW`,
# MAGIC `ERGEI`,
# MAGIC `ERVOL`,
# MAGIC `ERVOE`,
# MAGIC `GEWTO`,
# MAGIC `VOLTO`,
# MAGIC `VABME`,
# MAGIC `KZREV`,
# MAGIC `KZKFG`,
# MAGIC `XCHPF`,
# MAGIC `VHART`,
# MAGIC `FUELG`,
# MAGIC `STFAK`,
# MAGIC `MAGRV`,
# MAGIC `BEGRU`,
# MAGIC `DATAB`,
# MAGIC `LIQDT`,
# MAGIC `SAISJ`,
# MAGIC `PLGTP`,
# MAGIC `MLGUT`,
# MAGIC `EXTWG`,
# MAGIC `SATNR`,
# MAGIC `ATTYP`,
# MAGIC `KZKUP`,
# MAGIC `KZNFM`,
# MAGIC `PMATA`,
# MAGIC `MSTAE`,
# MAGIC `MSTAV`,
# MAGIC `MSTDE`,
# MAGIC `MSTDV`,
# MAGIC `TAKLV`,
# MAGIC `RBNRM`,
# MAGIC `MHDRZ`,
# MAGIC `MHDHB`,
# MAGIC `MHDLP`,
# MAGIC `INHME`,
# MAGIC `INHAL`,
# MAGIC `VPREH`,
# MAGIC `ETIAG`,
# MAGIC `INHBR`,
# MAGIC `CMETH`,
# MAGIC `CUOBF`,
# MAGIC `KZUMW`,
# MAGIC `KOSCH`,
# MAGIC `SPROF`,
# MAGIC `NRFHG`,
# MAGIC `MFRPN`,
# MAGIC `MFRNR`,
# MAGIC `BMATN`,
# MAGIC `MPROF`,
# MAGIC `KZWSM`,
# MAGIC `SAITY`,
# MAGIC `PROFL`,
# MAGIC `IHIVI`,
# MAGIC `ILOOS`,
# MAGIC `SERLV`,
# MAGIC `KZGVH`,
# MAGIC `XGCHP`,
# MAGIC `KZEFF`,
# MAGIC `COMPL`,
# MAGIC `IPRKZ`,
# MAGIC `RDMHD`,
# MAGIC `PRZUS`,
# MAGIC `MTPOS_MARA`,
# MAGIC `BFLME`,
# MAGIC `MATFI`,
# MAGIC `CMREL`,
# MAGIC `BBTYP`,
# MAGIC `SLED_BBD`,
# MAGIC `GTIN_VARIANT`,
# MAGIC `GENNR`,
# MAGIC `RMATP`,
# MAGIC `GDS_RELEVANT`,
# MAGIC `WEORA`,
# MAGIC `HUTYP_DFLT`,
# MAGIC `PILFERABLE`,
# MAGIC `WHSTC`,
# MAGIC `WHMATGR`,
# MAGIC `HNDLCODE`,
# MAGIC `HAZMAT`,
# MAGIC `HUTYP`,
# MAGIC `TARE_VAR`,
# MAGIC `MAXC`,
# MAGIC `MAXC_TOL`,
# MAGIC `MAXL`,
# MAGIC `MAXB`,
# MAGIC `MAXH`,
# MAGIC `MAXDIM_UOM`,
# MAGIC `HERKL`,
# MAGIC `MFRGR`,
# MAGIC `QQTIME`,
# MAGIC `QQTIMEUOM`,
# MAGIC `QGRP`,
# MAGIC `SERIAL`,
# MAGIC `PS_SMARTFORM`,
# MAGIC `LOGUNIT`,
# MAGIC `CWQREL`,
# MAGIC `CWQPROC`,
# MAGIC `CWQTOLGR`,
# MAGIC `ADPROF`,
# MAGIC `IPMIPPRODUCT`,
# MAGIC `ALLOW_PMAT_IGNO`,
# MAGIC `MEDIUM`,
# MAGIC `COMMODITY`,
# MAGIC `ANIMAL_ORIGIN`,
# MAGIC `TEXTILE_COMP_IND`,
# MAGIC `LAST_CHANGED_TIME`,
# MAGIC `MATNR_EXTERNAL`,
# MAGIC `CHML_CMPLNC_RLVNCE_IND`,
# MAGIC `LOGISTICAL_MAT_CATEGORY`,
# MAGIC `SALES_MATERIAL`,
# MAGIC `IDENTIFICATION_TAG_TYPE`,
# MAGIC `SGT_CSGR`,
# MAGIC `SGT_COVSA`,
# MAGIC `SGT_STAT`,
# MAGIC `SGT_SCOPE`,
# MAGIC `SGT_REL`,
# MAGIC `ANP`,
# MAGIC `PSM_CODE`,
# MAGIC `FSH_MG_AT1`,
# MAGIC `FSH_MG_AT2`,
# MAGIC `FSH_MG_AT3`,
# MAGIC `FSH_SEALV`,
# MAGIC `FSH_SEAIM`,
# MAGIC `FSH_SC_MID`,
# MAGIC `DUMMY_PRD_INCL_EEW_PS`,
# MAGIC `SCM_MATID_GUID16`,
# MAGIC `SCM_MATID_GUID22`,
# MAGIC `SCM_MATURITY_DUR`,
# MAGIC `SCM_SHLF_LFE_REQ_MIN`,
# MAGIC `SCM_SHLF_LFE_REQ_MAX`,
# MAGIC `SCM_PUOM`,
# MAGIC `RMATP_PB`,
# MAGIC `PROD_SHAPE`,
# MAGIC `MO_PROFILE_ID`,
# MAGIC `OVERHANG_TRESH`,
# MAGIC `BRIDGE_TRESH`,
# MAGIC `BRIDGE_MAX_SLOPE`,
# MAGIC `HEIGHT_NONFLAT`,
# MAGIC `HEIGHT_NONFLAT_UOM`,
# MAGIC `SCM_KITCOMP`,
# MAGIC `SCM_PROD_PAOOPT`,
# MAGIC `SCM_BOD_DEPLVL`,
# MAGIC `SCM_RESTRICT_INVBAL`,
# MAGIC `SCM_DRP_GL_STOCK`,
# MAGIC `SCM_EXCL_EXPEDITE`,
# MAGIC `/CWM/XCWMAT`,
# MAGIC `/CWM/VALUM`,
# MAGIC `/CWM/TOLGR`,
# MAGIC `/CWM/TARA`,
# MAGIC `/CWM/TARUM`,
# MAGIC `/BEV1/LULEINH`,
# MAGIC `/BEV1/LULDEGRP`,
# MAGIC `/BEV1/NESTRUCCAT`,
# MAGIC `/DSD/SL_TOLTYP`,
# MAGIC `/DSD/SV_CNT_GRP`,
# MAGIC `/DSD/VC_GROUP`,
# MAGIC `/SAPMP/KADU`,
# MAGIC `/SAPMP/ABMEIN`,
# MAGIC `/SAPMP/KADP`,
# MAGIC `/SAPMP/BRAD`,
# MAGIC `/SAPMP/SPBI`,
# MAGIC `/SAPMP/TRAD`,
# MAGIC `/SAPMP/KEDU`,
# MAGIC `/SAPMP/SPTR`,
# MAGIC `/SAPMP/FBDK`,
# MAGIC `/SAPMP/FBHK`,
# MAGIC `/SAPMP/RILI`,
# MAGIC `/SAPMP/FBAK`,
# MAGIC `/SAPMP/AHO`,
# MAGIC `/SAPMP/MIFRR`,
# MAGIC `/STTPEC/SERTYPE`,
# MAGIC `/STTPEC/SYNCACT`,
# MAGIC `/STTPEC/SYNCTIME`,
# MAGIC `/STTPEC/SYNCCHG`,
# MAGIC `/STTPEC/COUNTRY_REF`,
# MAGIC `/STTPEC/PRDCAT`,
# MAGIC `/VSO/R_TILT_IND`,
# MAGIC `/VSO/R_STACK_IND`,
# MAGIC `/VSO/R_BOT_IND`,
# MAGIC `/VSO/R_TOP_IND`,
# MAGIC `/VSO/R_STACK_NO`,
# MAGIC `/VSO/R_PAL_IND`,
# MAGIC `/VSO/R_PAL_OVR_D`,
# MAGIC `/VSO/R_PAL_OVR_W`,
# MAGIC `/VSO/R_PAL_B_HT`,
# MAGIC `/VSO/R_PAL_MIN_H`,
# MAGIC `/VSO/R_TOL_B_HT`,
# MAGIC `/VSO/R_NO_P_GVH`,
# MAGIC `/VSO/R_QUAN_UNIT`,
# MAGIC `/VSO/R_KZGVH_IND`,
# MAGIC `PACKCODE`,
# MAGIC `DG_PACK_STATUS`,
# MAGIC `SRV_DURA`,
# MAGIC `SRV_DURA_UOM`,
# MAGIC `SRV_SERWI`,
# MAGIC `SRV_ESCAL`,
# MAGIC `SOM_CYCLE`,
# MAGIC `SOM_CYCLE_RULE`,
# MAGIC `SOM_TC_SCHEMA`,
# MAGIC `SOM_CTR_AUTORENEWAL`,
# MAGIC `MCOND`,
# MAGIC `RETDELC`,
# MAGIC `LOGLEV_RETO`,
# MAGIC `NSNID`,
# MAGIC `ICFA`,
# MAGIC `RIC_ID`,
# MAGIC `DFS_SENSITIVITY_KEY`,
# MAGIC `DFS_MFRP2`,
# MAGIC `OVLPN`,
# MAGIC `ADSPC_SPC`,
# MAGIC `VARID`,
# MAGIC `MSBOOKPARTNO`,
# MAGIC `TOLERANCE_TYPE`,
# MAGIC `DPCBT`,
# MAGIC `XGRDT`,
# MAGIC `IMATN`,
# MAGIC `PICNUM`,
# MAGIC `BSTAT`,
# MAGIC `COLOR_ATINN`,
# MAGIC `SIZE1_ATINN`,
# MAGIC `SIZE2_ATINN`,
# MAGIC `COLOR`,
# MAGIC `SIZE1`,
# MAGIC `SIZE2`,
# MAGIC `FREE_CHAR`,
# MAGIC `CARE_CODE`,
# MAGIC `BRAND_ID`,
# MAGIC `FIBER_CODE1`,
# MAGIC `FIBER_PART1`,
# MAGIC `FIBER_CODE2`,
# MAGIC `FIBER_PART2`,
# MAGIC `FIBER_CODE3`,
# MAGIC `FIBER_PART3`,
# MAGIC `FIBER_CODE4`,
# MAGIC `FIBER_PART4`,
# MAGIC `FIBER_CODE5`,
# MAGIC `FIBER_PART5`,
# MAGIC `FASHGRD`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC ) VALUES 
# MAGIC (
# MAGIC S.`DI_SEQUENCE_NUMBER`,
# MAGIC S.`DI_OPERATION_TYPE`,
# MAGIC S.`MANDT`,
# MAGIC S.`MATNR`,
# MAGIC S.`ERSDA`,
# MAGIC S.`CREATED_AT_TIME`,
# MAGIC S.`ERNAM`,
# MAGIC S.`LAEDA`,
# MAGIC S.`AENAM`,
# MAGIC S.`VPSTA`,
# MAGIC S.`PSTAT`,
# MAGIC S.`LVORM`,
# MAGIC S.`MTART`,
# MAGIC S.`MBRSH`,
# MAGIC S.`MATKL`,
# MAGIC S.`BISMT`,
# MAGIC S.`MEINS`,
# MAGIC S.`BSTME`,
# MAGIC S.`ZEINR`,
# MAGIC S.`ZEIAR`,
# MAGIC S.`ZEIVR`,
# MAGIC S.`ZEIFO`,
# MAGIC S.`AESZN`,
# MAGIC S.`BLATT`,
# MAGIC S.`BLANZ`,
# MAGIC S.`FERTH`,
# MAGIC S.`FORMT`,
# MAGIC S.`GROES`,
# MAGIC S.`WRKST`,
# MAGIC S.`NORMT`,
# MAGIC S.`LABOR`,
# MAGIC S.`EKWSL`,
# MAGIC S.`BRGEW`,
# MAGIC S.`NTGEW`,
# MAGIC S.`GEWEI`,
# MAGIC S.`VOLUM`,
# MAGIC S.`VOLEH`,
# MAGIC S.`BEHVO`,
# MAGIC S.`RAUBE`,
# MAGIC S.`TEMPB`,
# MAGIC S.`DISST`,
# MAGIC S.`TRAGR`,
# MAGIC S.`STOFF`,
# MAGIC S.`SPART`,
# MAGIC S.`KUNNR`,
# MAGIC S.`EANNR`,
# MAGIC S.`WESCH`,
# MAGIC S.`BWVOR`,
# MAGIC S.`BWSCL`,
# MAGIC S.`SAISO`,
# MAGIC S.`ETIAR`,
# MAGIC S.`ETIFO`,
# MAGIC S.`ENTAR`,
# MAGIC S.`EAN11`,
# MAGIC S.`NUMTP`,
# MAGIC S.`LAENG`,
# MAGIC S.`BREIT`,
# MAGIC S.`HOEHE`,
# MAGIC S.`MEABM`,
# MAGIC S.`PRDHA`,
# MAGIC S.`AEKLK`,
# MAGIC S.`CADKZ`,
# MAGIC S.`QMPUR`,
# MAGIC S.`ERGEW`,
# MAGIC S.`ERGEI`,
# MAGIC S.`ERVOL`,
# MAGIC S.`ERVOE`,
# MAGIC S.`GEWTO`,
# MAGIC S.`VOLTO`,
# MAGIC S.`VABME`,
# MAGIC S.`KZREV`,
# MAGIC S.`KZKFG`,
# MAGIC S.`XCHPF`,
# MAGIC S.`VHART`,
# MAGIC S.`FUELG`,
# MAGIC S.`STFAK`,
# MAGIC S.`MAGRV`,
# MAGIC S.`BEGRU`,
# MAGIC S.`DATAB`,
# MAGIC S.`LIQDT`,
# MAGIC S.`SAISJ`,
# MAGIC S.`PLGTP`,
# MAGIC S.`MLGUT`,
# MAGIC S.`EXTWG`,
# MAGIC S.`SATNR`,
# MAGIC S.`ATTYP`,
# MAGIC S.`KZKUP`,
# MAGIC S.`KZNFM`,
# MAGIC S.`PMATA`,
# MAGIC S.`MSTAE`,
# MAGIC S.`MSTAV`,
# MAGIC S.`MSTDE`,
# MAGIC S.`MSTDV`,
# MAGIC S.`TAKLV`,
# MAGIC S.`RBNRM`,
# MAGIC S.`MHDRZ`,
# MAGIC S.`MHDHB`,
# MAGIC S.`MHDLP`,
# MAGIC S.`INHME`,
# MAGIC S.`INHAL`,
# MAGIC S.`VPREH`,
# MAGIC S.`ETIAG`,
# MAGIC S.`INHBR`,
# MAGIC S.`CMETH`,
# MAGIC S.`CUOBF`,
# MAGIC S.`KZUMW`,
# MAGIC S.`KOSCH`,
# MAGIC S.`SPROF`,
# MAGIC S.`NRFHG`,
# MAGIC S.`MFRPN`,
# MAGIC S.`MFRNR`,
# MAGIC S.`BMATN`,
# MAGIC S.`MPROF`,
# MAGIC S.`KZWSM`,
# MAGIC S.`SAITY`,
# MAGIC S.`PROFL`,
# MAGIC S.`IHIVI`,
# MAGIC S.`ILOOS`,
# MAGIC S.`SERLV`,
# MAGIC S.`KZGVH`,
# MAGIC S.`XGCHP`,
# MAGIC S.`KZEFF`,
# MAGIC S.`COMPL`,
# MAGIC S.`IPRKZ`,
# MAGIC S.`RDMHD`,
# MAGIC S.`PRZUS`,
# MAGIC S.`MTPOS_MARA`,
# MAGIC S.`BFLME`,
# MAGIC S.`MATFI`,
# MAGIC S.`CMREL`,
# MAGIC S.`BBTYP`,
# MAGIC S.`SLED_BBD`,
# MAGIC S.`GTIN_VARIANT`,
# MAGIC S.`GENNR`,
# MAGIC S.`RMATP`,
# MAGIC S.`GDS_RELEVANT`,
# MAGIC S.`WEORA`,
# MAGIC S.`HUTYP_DFLT`,
# MAGIC S.`PILFERABLE`,
# MAGIC S.`WHSTC`,
# MAGIC S.`WHMATGR`,
# MAGIC S.`HNDLCODE`,
# MAGIC S.`HAZMAT`,
# MAGIC S.`HUTYP`,
# MAGIC S.`TARE_VAR`,
# MAGIC S.`MAXC`,
# MAGIC S.`MAXC_TOL`,
# MAGIC S.`MAXL`,
# MAGIC S.`MAXB`,
# MAGIC S.`MAXH`,
# MAGIC S.`MAXDIM_UOM`,
# MAGIC S.`HERKL`,
# MAGIC S.`MFRGR`,
# MAGIC S.`QQTIME`,
# MAGIC S.`QQTIMEUOM`,
# MAGIC S.`QGRP`,
# MAGIC S.`SERIAL`,
# MAGIC S.`PS_SMARTFORM`,
# MAGIC S.`LOGUNIT`,
# MAGIC S.`CWQREL`,
# MAGIC S.`CWQPROC`,
# MAGIC S.`CWQTOLGR`,
# MAGIC S.`ADPROF`,
# MAGIC S.`IPMIPPRODUCT`,
# MAGIC S.`ALLOW_PMAT_IGNO`,
# MAGIC S.`MEDIUM`,
# MAGIC S.`COMMODITY`,
# MAGIC S.`ANIMAL_ORIGIN`,
# MAGIC S.`TEXTILE_COMP_IND`,
# MAGIC S.`LAST_CHANGED_TIME`,
# MAGIC S.`MATNR_EXTERNAL`,
# MAGIC S.`CHML_CMPLNC_RLVNCE_IND`,
# MAGIC S.`LOGISTICAL_MAT_CATEGORY`,
# MAGIC S.`SALES_MATERIAL`,
# MAGIC S.`IDENTIFICATION_TAG_TYPE`,
# MAGIC S.`SGT_CSGR`,
# MAGIC S.`SGT_COVSA`,
# MAGIC S.`SGT_STAT`,
# MAGIC S.`SGT_SCOPE`,
# MAGIC S.`SGT_REL`,
# MAGIC S.`ANP`,
# MAGIC S.`PSM_CODE`,
# MAGIC S.`FSH_MG_AT1`,
# MAGIC S.`FSH_MG_AT2`,
# MAGIC S.`FSH_MG_AT3`,
# MAGIC S.`FSH_SEALV`,
# MAGIC S.`FSH_SEAIM`,
# MAGIC S.`FSH_SC_MID`,
# MAGIC S.`DUMMY_PRD_INCL_EEW_PS`,
# MAGIC S.`SCM_MATID_GUID16`,
# MAGIC S.`SCM_MATID_GUID22`,
# MAGIC S.`SCM_MATURITY_DUR`,
# MAGIC S.`SCM_SHLF_LFE_REQ_MIN`,
# MAGIC S.`SCM_SHLF_LFE_REQ_MAX`,
# MAGIC S.`SCM_PUOM`,
# MAGIC S.`RMATP_PB`,
# MAGIC S.`PROD_SHAPE`,
# MAGIC S.`MO_PROFILE_ID`,
# MAGIC S.`OVERHANG_TRESH`,
# MAGIC S.`BRIDGE_TRESH`,
# MAGIC S.`BRIDGE_MAX_SLOPE`,
# MAGIC S.`HEIGHT_NONFLAT`,
# MAGIC S.`HEIGHT_NONFLAT_UOM`,
# MAGIC S.`SCM_KITCOMP`,
# MAGIC S.`SCM_PROD_PAOOPT`,
# MAGIC S.`SCM_BOD_DEPLVL`,
# MAGIC S.`SCM_RESTRICT_INVBAL`,
# MAGIC S.`SCM_DRP_GL_STOCK`,
# MAGIC S.`SCM_EXCL_EXPEDITE`,
# MAGIC S.`/CWM/XCWMAT`,
# MAGIC S.`/CWM/VALUM`,
# MAGIC S.`/CWM/TOLGR`,
# MAGIC S.`/CWM/TARA`,
# MAGIC S.`/CWM/TARUM`,
# MAGIC S.`/BEV1/LULEINH`,
# MAGIC S.`/BEV1/LULDEGRP`,
# MAGIC S.`/BEV1/NESTRUCCAT`,
# MAGIC S.`/DSD/SL_TOLTYP`,
# MAGIC S.`/DSD/SV_CNT_GRP`,
# MAGIC S.`/DSD/VC_GROUP`,
# MAGIC S.`/SAPMP/KADU`,
# MAGIC S.`/SAPMP/ABMEIN`,
# MAGIC S.`/SAPMP/KADP`,
# MAGIC S.`/SAPMP/BRAD`,
# MAGIC S.`/SAPMP/SPBI`,
# MAGIC S.`/SAPMP/TRAD`,
# MAGIC S.`/SAPMP/KEDU`,
# MAGIC S.`/SAPMP/SPTR`,
# MAGIC S.`/SAPMP/FBDK`,
# MAGIC S.`/SAPMP/FBHK`,
# MAGIC S.`/SAPMP/RILI`,
# MAGIC S.`/SAPMP/FBAK`,
# MAGIC S.`/SAPMP/AHO`,
# MAGIC S.`/SAPMP/MIFRR`,
# MAGIC S.`/STTPEC/SERTYPE`,
# MAGIC S.`/STTPEC/SYNCACT`,
# MAGIC S.`/STTPEC/SYNCTIME`,
# MAGIC S.`/STTPEC/SYNCCHG`,
# MAGIC S.`/STTPEC/COUNTRY_REF`,
# MAGIC S.`/STTPEC/PRDCAT`,
# MAGIC S.`/VSO/R_TILT_IND`,
# MAGIC S.`/VSO/R_STACK_IND`,
# MAGIC S.`/VSO/R_BOT_IND`,
# MAGIC S.`/VSO/R_TOP_IND`,
# MAGIC S.`/VSO/R_STACK_NO`,
# MAGIC S.`/VSO/R_PAL_IND`,
# MAGIC S.`/VSO/R_PAL_OVR_D`,
# MAGIC S.`/VSO/R_PAL_OVR_W`,
# MAGIC S.`/VSO/R_PAL_B_HT`,
# MAGIC S.`/VSO/R_PAL_MIN_H`,
# MAGIC S.`/VSO/R_TOL_B_HT`,
# MAGIC S.`/VSO/R_NO_P_GVH`,
# MAGIC S.`/VSO/R_QUAN_UNIT`,
# MAGIC S.`/VSO/R_KZGVH_IND`,
# MAGIC S.`PACKCODE`,
# MAGIC S.`DG_PACK_STATUS`,
# MAGIC S.`SRV_DURA`,
# MAGIC S.`SRV_DURA_UOM`,
# MAGIC S.`SRV_SERWI`,
# MAGIC S.`SRV_ESCAL`,
# MAGIC S.`SOM_CYCLE`,
# MAGIC S.`SOM_CYCLE_RULE`,
# MAGIC S.`SOM_TC_SCHEMA`,
# MAGIC S.`SOM_CTR_AUTORENEWAL`,
# MAGIC S.`MCOND`,
# MAGIC S.`RETDELC`,
# MAGIC S.`LOGLEV_RETO`,
# MAGIC S.`NSNID`,
# MAGIC S.`ICFA`,
# MAGIC S.`RIC_ID`,
# MAGIC S.`DFS_SENSITIVITY_KEY`,
# MAGIC S.`DFS_MFRP2`,
# MAGIC S.`OVLPN`,
# MAGIC S.`ADSPC_SPC`,
# MAGIC S.`VARID`,
# MAGIC S.`MSBOOKPARTNO`,
# MAGIC S.`TOLERANCE_TYPE`,
# MAGIC S.`DPCBT`,
# MAGIC S.`XGRDT`,
# MAGIC S.`IMATN`,
# MAGIC S.`PICNUM`,
# MAGIC S.`BSTAT`,
# MAGIC S.`COLOR_ATINN`,
# MAGIC S.`SIZE1_ATINN`,
# MAGIC S.`SIZE2_ATINN`,
# MAGIC S.`COLOR`,
# MAGIC S.`SIZE1`,
# MAGIC S.`SIZE2`,
# MAGIC S.`FREE_CHAR`,
# MAGIC S.`CARE_CODE`,
# MAGIC S.`BRAND_ID`,
# MAGIC S.`FIBER_CODE1`,
# MAGIC S.`FIBER_PART1`,
# MAGIC S.`FIBER_CODE2`,
# MAGIC S.`FIBER_PART2`,
# MAGIC S.`FIBER_CODE3`,
# MAGIC S.`FIBER_PART3`,
# MAGIC S.`FIBER_CODE4`,
# MAGIC S.`FIBER_PART4`,
# MAGIC S.`FIBER_CODE5`,
# MAGIC S.`FIBER_PART5`,
# MAGIC S.`FASHGRD`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.LandingFileTimeStamp,
# MAGIC 'SAP',
# MAGIC now()
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


