# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'MARC'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

schema = StructType([ \
StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',IntegerType(),True),\
StructField('MATNR',StringType(),True),\
StructField('WERKS',IntegerType(),True),\
StructField('PSTAT',StringType(),True),\
StructField('LVORM',StringType(),True),\
StructField('BWTTY',StringType(),True),\
StructField('XCHAR',StringType(),True),\
StructField('MMSTA',StringType(),True),\
StructField('MMSTD',StringType(),True),\
StructField('MAABC',StringType(),True),\
StructField('KZKRI',StringType(),True),\
StructField('EKGRP',StringType(),True),\
StructField('AUSME',StringType(),True),\
StructField('DISPR',StringType(),True),\
StructField('DISMM',StringType(),True),\
StructField('DISPO',StringType(),True),\
StructField('KZDIE',StringType(),True),\
StructField('PLIFZ',IntegerType(),True),\
StructField('WEBAZ',IntegerType(),True),\
StructField('PERKZ',StringType(),True),\
StructField('AUSSS',DoubleType(),True),\
StructField('DISLS',StringType(),True),\
StructField('BESKZ',StringType(),True),\
StructField('SOBSL',StringType(),True),\
StructField('MINBE',DoubleType(),True),\
StructField('EISBE',DoubleType(),True),\
StructField('BSTMI',DoubleType(),True),\
StructField('BSTMA',DoubleType(),True),\
StructField('BSTFE',DoubleType(),True),\
StructField('BSTRF',DoubleType(),True),\
StructField('MABST',DoubleType(),True),\
StructField('LOSFX',DoubleType(),True),\
StructField('SBDKZ',StringType(),True),\
StructField('LAGPR',StringType(),True),\
StructField('ALTSL',StringType(),True),\
StructField('KZAUS',StringType(),True),\
StructField('AUSDT',StringType(),True),\
StructField('NFMAT',StringType(),True),\
StructField('KZBED',StringType(),True),\
StructField('MISKZ',StringType(),True),\
StructField('FHORI',StringType(),True),\
StructField('PFREI',StringType(),True),\
StructField('FFREI',StringType(),True),\
StructField('RGEKZ',StringType(),True),\
StructField('FEVOR',StringType(),True),\
StructField('BEARZ',DoubleType(),True),\
StructField('RUEZT',DoubleType(),True),\
StructField('TRANZ',DoubleType(),True),\
StructField('BASMG',DoubleType(),True),\
StructField('DZEIT',IntegerType(),True),\
StructField('MAXLZ',IntegerType(),True),\
StructField('LZEIH',StringType(),True),\
StructField('KZPRO',StringType(),True),\
StructField('GPMKZ',StringType(),True),\
StructField('UEETO',DoubleType(),True),\
StructField('UEETK',StringType(),True),\
StructField('UNETO',DoubleType(),True),\
StructField('WZEIT',IntegerType(),True),\
StructField('ATPKZ',StringType(),True),\
StructField('VZUSL',DoubleType(),True),\
StructField('HERBL',StringType(),True),\
StructField('INSMK',StringType(),True),\
StructField('SPROZ',DoubleType(),True),\
StructField('QUAZT',IntegerType(),True),\
StructField('SSQSS',StringType(),True),\
StructField('MPDAU',IntegerType(),True),\
StructField('KZPPV',StringType(),True),\
StructField('KZDKZ',StringType(),True),\
StructField('WSTGH',IntegerType(),True),\
StructField('PRFRQ',IntegerType(),True),\
StructField('NKMPR',StringType(),True),\
StructField('UMLMC',DoubleType(),True),\
StructField('LADGR',StringType(),True),\
StructField('XCHPF',StringType(),True),\
StructField('USEQU',StringType(),True),\
StructField('LGRAD',DoubleType(),True),\
StructField('AUFTL',StringType(),True),\
StructField('PLVAR',StringType(),True),\
StructField('OTYPE',StringType(),True),\
StructField('OBJID',IntegerType(),True),\
StructField('MTVFP',StringType(),True),\
StructField('PERIV',StringType(),True),\
StructField('KZKFK',StringType(),True),\
StructField('VRVEZ',DoubleType(),True),\
StructField('VBAMG',DoubleType(),True),\
StructField('VBEAZ',DoubleType(),True),\
StructField('LIZYK',StringType(),True),\
StructField('BWSCL',StringType(),True),\
StructField('KAUTB',StringType(),True),\
StructField('KORDB',StringType(),True),\
StructField('STAWN',StringType(),True),\
StructField('HERKL',StringType(),True),\
StructField('HERKR',StringType(),True),\
StructField('EXPME',StringType(),True),\
StructField('MTVER',StringType(),True),\
StructField('PRCTR',StringType(),True),\
StructField('TRAME',DoubleType(),True),\
StructField('MRPPP',StringType(),True),\
StructField('SAUFT',StringType(),True),\
StructField('FXHOR',IntegerType(),True),\
StructField('VRMOD',StringType(),True),\
StructField('VINT1',IntegerType(),True),\
StructField('VINT2',IntegerType(),True),\
StructField('VERKZ',StringType(),True),\
StructField('STLAL',StringType(),True),\
StructField('STLAN',StringType(),True),\
StructField('PLNNR',StringType(),True),\
StructField('APLAL',StringType(),True),\
StructField('LOSGR',DoubleType(),True),\
StructField('SOBSK',StringType(),True),\
StructField('FRTME',StringType(),True),\
StructField('LGPRO',StringType(),True),\
StructField('DISGR',StringType(),True),\
StructField('KAUSF',DoubleType(),True),\
StructField('QZGTP',StringType(),True),\
StructField('QMATV',StringType(),True),\
StructField('TAKZT',IntegerType(),True),\
StructField('RWPRO',StringType(),True),\
StructField('COPAM',StringType(),True),\
StructField('ABCIN',StringType(),True),\
StructField('AWSLS',StringType(),True),\
StructField('SERNP',StringType(),True),\
StructField('CUOBJ',IntegerType(),True),\
StructField('STDPD',StringType(),True),\
StructField('SFEPR',StringType(),True),\
StructField('XMCNG',StringType(),True),\
StructField('QSSYS',StringType(),True),\
StructField('LFRHY',StringType(),True),\
StructField('RDPRF',StringType(),True),\
StructField('VRBMT',StringType(),True),\
StructField('VRBWK',StringType(),True),\
StructField('VRBDT',StringType(),True),\
StructField('VRBFK',DoubleType(),True),\
StructField('AUTRU',StringType(),True),\
StructField('PREFE',StringType(),True),\
StructField('PRENC',StringType(),True),\
StructField('PRENO',StringType(),True),\
StructField('PREND',StringType(),True),\
StructField('PRENE',StringType(),True),\
StructField('PRENG',StringType(),True),\
StructField('ITARK',StringType(),True),\
StructField('SERVG',StringType(),True),\
StructField('KZKUP',StringType(),True),\
StructField('STRGR',StringType(),True),\
StructField('CUOBV',IntegerType(),True),\
StructField('LGFSB',StringType(),True),\
StructField('SCHGT',StringType(),True),\
StructField('CCFIX',StringType(),True),\
StructField('EPRIO',StringType(),True),\
StructField('QMATA',StringType(),True),\
StructField('RESVP',IntegerType(),True),\
StructField('PLNTY',StringType(),True),\
StructField('UOMGR',StringType(),True),\
StructField('UMRSL',StringType(),True),\
StructField('ABFAC',DoubleType(),True),\
StructField('SFCPF',StringType(),True),\
StructField('SHFLG',StringType(),True),\
StructField('SHZET',IntegerType(),True),\
StructField('MDACH',StringType(),True),\
StructField('KZECH',StringType(),True),\
StructField('MEGRU',StringType(),True),\
StructField('MFRGR',StringType(),True),\
StructField('PROFIL',StringType(),True),\
StructField('VKUMC',DoubleType(),True),\
StructField('VKTRW',DoubleType(),True),\
StructField('KZAGL',StringType(),True),\
StructField('FVIDK',StringType(),True),\
StructField('FXPRU',StringType(),True),\
StructField('LOGGR',StringType(),True),\
StructField('FPRFM',StringType(),True),\
StructField('GLGMG',DoubleType(),True),\
StructField('VKGLG',DoubleType(),True),\
StructField('INDUS',StringType(),True),\
StructField('MOWNR',StringType(),True),\
StructField('MOGRU',StringType(),True),\
StructField('CASNR',StringType(),True),\
StructField('GPNUM',StringType(),True),\
StructField('STEUC',StringType(),True),\
StructField('FABKZ',StringType(),True),\
StructField('MATGR',StringType(),True),\
StructField('VSPVB',StringType(),True),\
StructField('DPLFS',StringType(),True),\
StructField('DPLPU',StringType(),True),\
StructField('DPLHO',IntegerType(),True),\
StructField('MINLS',DoubleType(),True),\
StructField('MAXLS',DoubleType(),True),\
StructField('FIXLS',DoubleType(),True),\
StructField('LTINC',DoubleType(),True),\
StructField('COMPL',IntegerType(),True),\
StructField('CONVT',StringType(),True),\
StructField('SHPRO',StringType(),True),\
StructField('AHDIS',StringType(),True),\
StructField('DIBER',StringType(),True),\
StructField('KZPSP',StringType(),True),\
StructField('OCMPF',StringType(),True),\
StructField('APOKZ',StringType(),True),\
StructField('MCRUE',StringType(),True),\
StructField('LFMON',IntegerType(),True),\
StructField('LFGJA',IntegerType(),True),\
StructField('EISLO',DoubleType(),True),\
StructField('NCOST',StringType(),True),\
StructField('ROTATION_DATE',StringType(),True),\
StructField('UCHKZ',StringType(),True),\
StructField('UCMAT',StringType(),True),\
StructField('EXCISE_TAX_RLVNCE',StringType(),True),\
StructField('BWESB',DoubleType(),True),\
StructField('SGT_COVS',StringType(),True),\
StructField('SGT_STATC',StringType(),True),\
StructField('SGT_SCOPE',StringType(),True),\
StructField('SGT_MRPSI',StringType(),True),\
StructField('SGT_PRCM',StringType(),True),\
StructField('SGT_CHINT',StringType(),True),\
StructField('SGT_STK_PRT',StringType(),True),\
StructField('SGT_DEFSC',StringType(),True),\
StructField('SGT_MRP_ATP_STATUS',StringType(),True),\
StructField('SGT_MMSTD',StringType(),True),\
StructField('FSH_MG_ARUN_REQ',StringType(),True),\
StructField('FSH_SEAIM',StringType(),True),\
StructField('FSH_VAR_GROUP',StringType(),True),\
StructField('FSH_KZECH',StringType(),True),\
StructField('FSH_CALENDAR_GROUP',StringType(),True),\
StructField('ARUN_FIX_BATCH',StringType(),True),\
StructField('PPSKZ',StringType(),True),\
StructField('CONS_PROCG',StringType(),True),\
StructField('GI_PR_TIME',IntegerType(),True),\
StructField('MULTIPLE_EKGRP',StringType(),True),\
StructField('REF_SCHEMA',StringType(),True),\
StructField('MIN_TROC',IntegerType(),True),\
StructField('MAX_TROC',IntegerType(),True),\
StructField('TARGET_STOCK',DoubleType(),True),\
StructField('NF_FLAG',StringType(),True),\
StructField('/CWM/UMLMC',DoubleType(),True),\
StructField('/CWM/TRAME',DoubleType(),True),\
StructField('/CWM/BWESB',DoubleType(),True),\
StructField('SCM_MATLOCID_GUID16',StringType(),True),\
StructField('SCM_MATLOCID_GUID22',StringType(),True),\
StructField('SCM_GRPRT',IntegerType(),True),\
StructField('SCM_GIPRT',IntegerType(),True),\
StructField('SCM_SCOST',DoubleType(),True),\
StructField('SCM_RELDT',IntegerType(),True),\
StructField('SCM_RRP_TYPE',StringType(),True),\
StructField('SCM_HEUR_ID',StringType(),True),\
StructField('SCM_PACKAGE_ID',StringType(),True),\
StructField('SCM_SSPEN',DoubleType(),True),\
StructField('SCM_GET_ALERTS',StringType(),True),\
StructField('SCM_RES_NET_NAME',StringType(),True),\
StructField('SCM_CONHAP',DoubleType(),True),\
StructField('SCM_HUNIT',StringType(),True),\
StructField('SCM_CONHAP_OUT',DoubleType(),True),\
StructField('SCM_HUNIT_OUT',StringType(),True),\
StructField('SCM_SHELF_LIFE_LOC',StringType(),True),\
StructField('SCM_SHELF_LIFE_DUR',IntegerType(),True),\
StructField('SCM_MATURITY_DUR',IntegerType(),True),\
StructField('SCM_SHLF_LFE_REQ_MIN',IntegerType(),True),\
StructField('SCM_SHLF_LFE_REQ_MAX',IntegerType(),True),\
StructField('SCM_LSUOM',StringType(),True),\
StructField('SCM_REORD_DUR',IntegerType(),True),\
StructField('SCM_TARGET_DUR',IntegerType(),True),\
StructField('SCM_TSTRID',StringType(),True),\
StructField('SCM_STRA1',StringType(),True),\
StructField('SCM_PEG_PAST_ALERT',IntegerType(),True),\
StructField('SCM_PEG_FUTURE_ALERT',IntegerType(),True),\
StructField('SCM_PEG_STRATEGY',IntegerType(),True),\
StructField('SCM_PEG_WO_ALERT_FST',StringType(),True),\
StructField('SCM_FIXPEG_PROD_SET',StringType(),True),\
StructField('SCM_WHATBOM',StringType(),True),\
StructField('SCM_RRP_SEL_GROUP',StringType(),True),\
StructField('SCM_INTSRC_PROF',StringType(),True),\
StructField('SCM_PRIO',IntegerType(),True),\
StructField('SCM_MIN_PASS_AMOUNT',DoubleType(),True),\
StructField('SCM_PROFID',StringType(),True),\
StructField('SCM_GES_MNG_USE',StringType(),True),\
StructField('SCM_GES_BST_USE',StringType(),True),\
StructField('ESPPFLG',StringType(),True),\
StructField('SCM_THRUPUT_TIME',IntegerType(),True),\
StructField('SCM_TPOP',StringType(),True),\
StructField('SCM_SAFTY_V',DoubleType(),True),\
StructField('SCM_PPSAFTYSTK',DoubleType(),True),\
StructField('SCM_PPSAFTYSTK_V',DoubleType(),True),\
StructField('SCM_REPSAFTY',DoubleType(),True),\
StructField('SCM_REPSAFTY_V',DoubleType(),True),\
StructField('SCM_REORD_V',DoubleType(),True),\
StructField('SCM_MAXSTOCK_V',DoubleType(),True),\
StructField('SCM_SCOST_PRCNT',DoubleType(),True),\
StructField('SCM_PROC_COST',DoubleType(),True),\
StructField('SCM_NDCOSTWE',DoubleType(),True),\
StructField('SCM_NDCOSTWA',DoubleType(),True),\
StructField('SCM_CONINP',DoubleType(),True),\
StructField('CONF_GMSYNC',StringType(),True),\
StructField('SCM_IUNIT',StringType(),True),\
StructField('SCM_SFT_LOCK',StringType(),True),\
StructField('DUMMY_PLNT_INCL_EEW_PS',StringType(),True),\
StructField('/SAPMP/TOLPRPL',DoubleType(),True),\
StructField('/SAPMP/TOLPRMI',DoubleType(),True),\
StructField('/STTPEC/SERVALID',StringType(),True),\
StructField('/VSO/R_PKGRP',StringType(),True),\
StructField('/VSO/R_LANE_NUM',StringType(),True),\
StructField('/VSO/R_PAL_VEND',StringType(),True),\
StructField('/VSO/R_FORK_DIR',StringType(),True),\
StructField('IUID_RELEVANT',StringType(),True),\
StructField('IUID_TYPE',StringType(),True),\
StructField('UID_IEA',StringType(),True),\
StructField('DPCBT',StringType(),True),\
StructField('ZZECCN',StringType(),True),\
StructField('ZZSCHB',StringType(),True),\
StructField('ZZHTS',StringType(),True),\
StructField('ZZECCN_YR',IntegerType(),True),\
StructField('ZZMATREF',StringType(),True),\
StructField('ZZROUTING',StringType(),True),\
StructField('ZZPRODCODE',StringType(),True),\
StructField('ZZSOECELLFUSION',StringType(),True),\
StructField('ZZCHK',StringType(),True),\
StructField('ZZPLNTGRP',StringType(),True),\
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
                            .withColumn("MMSTD", to_date(regexp_replace(df_add_column.MMSTD,'\.','-'))) \
                            .withColumn("AUSDT", to_date(regexp_replace(df_add_column.AUSDT,'\.','-'))) \
                            .withColumn("NKMPR", to_date(regexp_replace(df_add_column.NKMPR,'\.','-'))) \
                            .withColumn("VRBDT", to_date(regexp_replace(df_add_column.VRBDT,'\.','-'))) \
                            .withColumn("PREND", to_date(regexp_replace(df_add_column.PREND,'\.','-'))) \
                            .withColumn("PRENG", to_date(regexp_replace(df_add_column.PRENG,'\.','-'))) \
                            .withColumn("SGT_MMSTD", to_date(regexp_replace(df_add_column.SGT_MMSTD,'\.','-'))) \
                            .withColumn("/STTPEC/SERVALID", to_date(regexp_replace("/STTPEC/SERVALID",'\.','-'))) \
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
# MAGIC MERGE INTO S42.MARC as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,MATNR, WERKS ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_MARC where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and 
# MAGIC T.MATNR = S.MATNR and
# MAGIC T.WERKS = S.WERKS
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`DI_SEQUENCE_NUMBER` = S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` = S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` = S.`MANDT`,
# MAGIC T.`MATNR` = S.`MATNR`,
# MAGIC T.`WERKS` = S.`WERKS`,
# MAGIC T.`PSTAT` = S.`PSTAT`,
# MAGIC T.`LVORM` = S.`LVORM`,
# MAGIC T.`BWTTY` = S.`BWTTY`,
# MAGIC T.`XCHAR` = S.`XCHAR`,
# MAGIC T.`MMSTA` = S.`MMSTA`,
# MAGIC T.`MMSTD` = S.`MMSTD`,
# MAGIC T.`MAABC` = S.`MAABC`,
# MAGIC T.`KZKRI` = S.`KZKRI`,
# MAGIC T.`EKGRP` = S.`EKGRP`,
# MAGIC T.`AUSME` = S.`AUSME`,
# MAGIC T.`DISPR` = S.`DISPR`,
# MAGIC T.`DISMM` = S.`DISMM`,
# MAGIC T.`DISPO` = S.`DISPO`,
# MAGIC T.`KZDIE` = S.`KZDIE`,
# MAGIC T.`PLIFZ` = S.`PLIFZ`,
# MAGIC T.`WEBAZ` = S.`WEBAZ`,
# MAGIC T.`PERKZ` = S.`PERKZ`,
# MAGIC T.`AUSSS` = S.`AUSSS`,
# MAGIC T.`DISLS` = S.`DISLS`,
# MAGIC T.`BESKZ` = S.`BESKZ`,
# MAGIC T.`SOBSL` = S.`SOBSL`,
# MAGIC T.`MINBE` = S.`MINBE`,
# MAGIC T.`EISBE` = S.`EISBE`,
# MAGIC T.`BSTMI` = S.`BSTMI`,
# MAGIC T.`BSTMA` = S.`BSTMA`,
# MAGIC T.`BSTFE` = S.`BSTFE`,
# MAGIC T.`BSTRF` = S.`BSTRF`,
# MAGIC T.`MABST` = S.`MABST`,
# MAGIC T.`LOSFX` = S.`LOSFX`,
# MAGIC T.`SBDKZ` = S.`SBDKZ`,
# MAGIC T.`LAGPR` = S.`LAGPR`,
# MAGIC T.`ALTSL` = S.`ALTSL`,
# MAGIC T.`KZAUS` = S.`KZAUS`,
# MAGIC T.`AUSDT` = S.`AUSDT`,
# MAGIC T.`NFMAT` = S.`NFMAT`,
# MAGIC T.`KZBED` = S.`KZBED`,
# MAGIC T.`MISKZ` = S.`MISKZ`,
# MAGIC T.`FHORI` = S.`FHORI`,
# MAGIC T.`PFREI` = S.`PFREI`,
# MAGIC T.`FFREI` = S.`FFREI`,
# MAGIC T.`RGEKZ` = S.`RGEKZ`,
# MAGIC T.`FEVOR` = S.`FEVOR`,
# MAGIC T.`BEARZ` = S.`BEARZ`,
# MAGIC T.`RUEZT` = S.`RUEZT`,
# MAGIC T.`TRANZ` = S.`TRANZ`,
# MAGIC T.`BASMG` = S.`BASMG`,
# MAGIC T.`DZEIT` = S.`DZEIT`,
# MAGIC T.`MAXLZ` = S.`MAXLZ`,
# MAGIC T.`LZEIH` = S.`LZEIH`,
# MAGIC T.`KZPRO` = S.`KZPRO`,
# MAGIC T.`GPMKZ` = S.`GPMKZ`,
# MAGIC T.`UEETO` = S.`UEETO`,
# MAGIC T.`UEETK` = S.`UEETK`,
# MAGIC T.`UNETO` = S.`UNETO`,
# MAGIC T.`WZEIT` = S.`WZEIT`,
# MAGIC T.`ATPKZ` = S.`ATPKZ`,
# MAGIC T.`VZUSL` = S.`VZUSL`,
# MAGIC T.`HERBL` = S.`HERBL`,
# MAGIC T.`INSMK` = S.`INSMK`,
# MAGIC T.`SPROZ` = S.`SPROZ`,
# MAGIC T.`QUAZT` = S.`QUAZT`,
# MAGIC T.`SSQSS` = S.`SSQSS`,
# MAGIC T.`MPDAU` = S.`MPDAU`,
# MAGIC T.`KZPPV` = S.`KZPPV`,
# MAGIC T.`KZDKZ` = S.`KZDKZ`,
# MAGIC T.`WSTGH` = S.`WSTGH`,
# MAGIC T.`PRFRQ` = S.`PRFRQ`,
# MAGIC T.`NKMPR` = S.`NKMPR`,
# MAGIC T.`UMLMC` = S.`UMLMC`,
# MAGIC T.`LADGR` = S.`LADGR`,
# MAGIC T.`XCHPF` = S.`XCHPF`,
# MAGIC T.`USEQU` = S.`USEQU`,
# MAGIC T.`LGRAD` = S.`LGRAD`,
# MAGIC T.`AUFTL` = S.`AUFTL`,
# MAGIC T.`PLVAR` = S.`PLVAR`,
# MAGIC T.`OTYPE` = S.`OTYPE`,
# MAGIC T.`OBJID` = S.`OBJID`,
# MAGIC T.`MTVFP` = S.`MTVFP`,
# MAGIC T.`PERIV` = S.`PERIV`,
# MAGIC T.`KZKFK` = S.`KZKFK`,
# MAGIC T.`VRVEZ` = S.`VRVEZ`,
# MAGIC T.`VBAMG` = S.`VBAMG`,
# MAGIC T.`VBEAZ` = S.`VBEAZ`,
# MAGIC T.`LIZYK` = S.`LIZYK`,
# MAGIC T.`BWSCL` = S.`BWSCL`,
# MAGIC T.`KAUTB` = S.`KAUTB`,
# MAGIC T.`KORDB` = S.`KORDB`,
# MAGIC T.`STAWN` = S.`STAWN`,
# MAGIC T.`HERKL` = S.`HERKL`,
# MAGIC T.`HERKR` = S.`HERKR`,
# MAGIC T.`EXPME` = S.`EXPME`,
# MAGIC T.`MTVER` = S.`MTVER`,
# MAGIC T.`PRCTR` = S.`PRCTR`,
# MAGIC T.`TRAME` = S.`TRAME`,
# MAGIC T.`MRPPP` = S.`MRPPP`,
# MAGIC T.`SAUFT` = S.`SAUFT`,
# MAGIC T.`FXHOR` = S.`FXHOR`,
# MAGIC T.`VRMOD` = S.`VRMOD`,
# MAGIC T.`VINT1` = S.`VINT1`,
# MAGIC T.`VINT2` = S.`VINT2`,
# MAGIC T.`VERKZ` = S.`VERKZ`,
# MAGIC T.`STLAL` = S.`STLAL`,
# MAGIC T.`STLAN` = S.`STLAN`,
# MAGIC T.`PLNNR` = S.`PLNNR`,
# MAGIC T.`APLAL` = S.`APLAL`,
# MAGIC T.`LOSGR` = S.`LOSGR`,
# MAGIC T.`SOBSK` = S.`SOBSK`,
# MAGIC T.`FRTME` = S.`FRTME`,
# MAGIC T.`LGPRO` = S.`LGPRO`,
# MAGIC T.`DISGR` = S.`DISGR`,
# MAGIC T.`KAUSF` = S.`KAUSF`,
# MAGIC T.`QZGTP` = S.`QZGTP`,
# MAGIC T.`QMATV` = S.`QMATV`,
# MAGIC T.`TAKZT` = S.`TAKZT`,
# MAGIC T.`RWPRO` = S.`RWPRO`,
# MAGIC T.`COPAM` = S.`COPAM`,
# MAGIC T.`ABCIN` = S.`ABCIN`,
# MAGIC T.`AWSLS` = S.`AWSLS`,
# MAGIC T.`SERNP` = S.`SERNP`,
# MAGIC T.`CUOBJ` = S.`CUOBJ`,
# MAGIC T.`STDPD` = S.`STDPD`,
# MAGIC T.`SFEPR` = S.`SFEPR`,
# MAGIC T.`XMCNG` = S.`XMCNG`,
# MAGIC T.`QSSYS` = S.`QSSYS`,
# MAGIC T.`LFRHY` = S.`LFRHY`,
# MAGIC T.`RDPRF` = S.`RDPRF`,
# MAGIC T.`VRBMT` = S.`VRBMT`,
# MAGIC T.`VRBWK` = S.`VRBWK`,
# MAGIC T.`VRBDT` = S.`VRBDT`,
# MAGIC T.`VRBFK` = S.`VRBFK`,
# MAGIC T.`AUTRU` = S.`AUTRU`,
# MAGIC T.`PREFE` = S.`PREFE`,
# MAGIC T.`PRENC` = S.`PRENC`,
# MAGIC T.`PRENO` = S.`PRENO`,
# MAGIC T.`PREND` = S.`PREND`,
# MAGIC T.`PRENE` = S.`PRENE`,
# MAGIC T.`PRENG` = S.`PRENG`,
# MAGIC T.`ITARK` = S.`ITARK`,
# MAGIC T.`SERVG` = S.`SERVG`,
# MAGIC T.`KZKUP` = S.`KZKUP`,
# MAGIC T.`STRGR` = S.`STRGR`,
# MAGIC T.`CUOBV` = S.`CUOBV`,
# MAGIC T.`LGFSB` = S.`LGFSB`,
# MAGIC T.`SCHGT` = S.`SCHGT`,
# MAGIC T.`CCFIX` = S.`CCFIX`,
# MAGIC T.`EPRIO` = S.`EPRIO`,
# MAGIC T.`QMATA` = S.`QMATA`,
# MAGIC T.`RESVP` = S.`RESVP`,
# MAGIC T.`PLNTY` = S.`PLNTY`,
# MAGIC T.`UOMGR` = S.`UOMGR`,
# MAGIC T.`UMRSL` = S.`UMRSL`,
# MAGIC T.`ABFAC` = S.`ABFAC`,
# MAGIC T.`SFCPF` = S.`SFCPF`,
# MAGIC T.`SHFLG` = S.`SHFLG`,
# MAGIC T.`SHZET` = S.`SHZET`,
# MAGIC T.`MDACH` = S.`MDACH`,
# MAGIC T.`KZECH` = S.`KZECH`,
# MAGIC T.`MEGRU` = S.`MEGRU`,
# MAGIC T.`MFRGR` = S.`MFRGR`,
# MAGIC T.`PROFIL` = S.`PROFIL`,
# MAGIC T.`VKUMC` = S.`VKUMC`,
# MAGIC T.`VKTRW` = S.`VKTRW`,
# MAGIC T.`KZAGL` = S.`KZAGL`,
# MAGIC T.`FVIDK` = S.`FVIDK`,
# MAGIC T.`FXPRU` = S.`FXPRU`,
# MAGIC T.`LOGGR` = S.`LOGGR`,
# MAGIC T.`FPRFM` = S.`FPRFM`,
# MAGIC T.`GLGMG` = S.`GLGMG`,
# MAGIC T.`VKGLG` = S.`VKGLG`,
# MAGIC T.`INDUS` = S.`INDUS`,
# MAGIC T.`MOWNR` = S.`MOWNR`,
# MAGIC T.`MOGRU` = S.`MOGRU`,
# MAGIC T.`CASNR` = S.`CASNR`,
# MAGIC T.`GPNUM` = S.`GPNUM`,
# MAGIC T.`STEUC` = S.`STEUC`,
# MAGIC T.`FABKZ` = S.`FABKZ`,
# MAGIC T.`MATGR` = S.`MATGR`,
# MAGIC T.`VSPVB` = S.`VSPVB`,
# MAGIC T.`DPLFS` = S.`DPLFS`,
# MAGIC T.`DPLPU` = S.`DPLPU`,
# MAGIC T.`DPLHO` = S.`DPLHO`,
# MAGIC T.`MINLS` = S.`MINLS`,
# MAGIC T.`MAXLS` = S.`MAXLS`,
# MAGIC T.`FIXLS` = S.`FIXLS`,
# MAGIC T.`LTINC` = S.`LTINC`,
# MAGIC T.`COMPL` = S.`COMPL`,
# MAGIC T.`CONVT` = S.`CONVT`,
# MAGIC T.`SHPRO` = S.`SHPRO`,
# MAGIC T.`AHDIS` = S.`AHDIS`,
# MAGIC T.`DIBER` = S.`DIBER`,
# MAGIC T.`KZPSP` = S.`KZPSP`,
# MAGIC T.`OCMPF` = S.`OCMPF`,
# MAGIC T.`APOKZ` = S.`APOKZ`,
# MAGIC T.`MCRUE` = S.`MCRUE`,
# MAGIC T.`LFMON` = S.`LFMON`,
# MAGIC T.`LFGJA` = S.`LFGJA`,
# MAGIC T.`EISLO` = S.`EISLO`,
# MAGIC T.`NCOST` = S.`NCOST`,
# MAGIC T.`ROTATION_DATE` = S.`ROTATION_DATE`,
# MAGIC T.`UCHKZ` = S.`UCHKZ`,
# MAGIC T.`UCMAT` = S.`UCMAT`,
# MAGIC T.`EXCISE_TAX_RLVNCE` = S.`EXCISE_TAX_RLVNCE`,
# MAGIC T.`BWESB` = S.`BWESB`,
# MAGIC T.`SGT_COVS` = S.`SGT_COVS`,
# MAGIC T.`SGT_STATC` = S.`SGT_STATC`,
# MAGIC T.`SGT_SCOPE` = S.`SGT_SCOPE`,
# MAGIC T.`SGT_MRPSI` = S.`SGT_MRPSI`,
# MAGIC T.`SGT_PRCM` = S.`SGT_PRCM`,
# MAGIC T.`SGT_CHINT` = S.`SGT_CHINT`,
# MAGIC T.`SGT_STK_PRT` = S.`SGT_STK_PRT`,
# MAGIC T.`SGT_DEFSC` = S.`SGT_DEFSC`,
# MAGIC T.`SGT_MRP_ATP_STATUS` = S.`SGT_MRP_ATP_STATUS`,
# MAGIC T.`SGT_MMSTD` = S.`SGT_MMSTD`,
# MAGIC T.`FSH_MG_ARUN_REQ` = S.`FSH_MG_ARUN_REQ`,
# MAGIC T.`FSH_SEAIM` = S.`FSH_SEAIM`,
# MAGIC T.`FSH_VAR_GROUP` = S.`FSH_VAR_GROUP`,
# MAGIC T.`FSH_KZECH` = S.`FSH_KZECH`,
# MAGIC T.`FSH_CALENDAR_GROUP` = S.`FSH_CALENDAR_GROUP`,
# MAGIC T.`ARUN_FIX_BATCH` = S.`ARUN_FIX_BATCH`,
# MAGIC T.`PPSKZ` = S.`PPSKZ`,
# MAGIC T.`CONS_PROCG` = S.`CONS_PROCG`,
# MAGIC T.`GI_PR_TIME` = S.`GI_PR_TIME`,
# MAGIC T.`MULTIPLE_EKGRP` = S.`MULTIPLE_EKGRP`,
# MAGIC T.`REF_SCHEMA` = S.`REF_SCHEMA`,
# MAGIC T.`MIN_TROC` = S.`MIN_TROC`,
# MAGIC T.`MAX_TROC` = S.`MAX_TROC`,
# MAGIC T.`TARGET_STOCK` = S.`TARGET_STOCK`,
# MAGIC T.`NF_FLAG` = S.`NF_FLAG`,
# MAGIC T.`/CWM/UMLMC` = S.`/CWM/UMLMC`,
# MAGIC T.`/CWM/TRAME` = S.`/CWM/TRAME`,
# MAGIC T.`/CWM/BWESB` = S.`/CWM/BWESB`,
# MAGIC T.`SCM_MATLOCID_GUID16` = S.`SCM_MATLOCID_GUID16`,
# MAGIC T.`SCM_MATLOCID_GUID22` = S.`SCM_MATLOCID_GUID22`,
# MAGIC T.`SCM_GRPRT` = S.`SCM_GRPRT`,
# MAGIC T.`SCM_GIPRT` = S.`SCM_GIPRT`,
# MAGIC T.`SCM_SCOST` = S.`SCM_SCOST`,
# MAGIC T.`SCM_RELDT` = S.`SCM_RELDT`,
# MAGIC T.`SCM_RRP_TYPE` = S.`SCM_RRP_TYPE`,
# MAGIC T.`SCM_HEUR_ID` = S.`SCM_HEUR_ID`,
# MAGIC T.`SCM_PACKAGE_ID` = S.`SCM_PACKAGE_ID`,
# MAGIC T.`SCM_SSPEN` = S.`SCM_SSPEN`,
# MAGIC T.`SCM_GET_ALERTS` = S.`SCM_GET_ALERTS`,
# MAGIC T.`SCM_RES_NET_NAME` = S.`SCM_RES_NET_NAME`,
# MAGIC T.`SCM_CONHAP` = S.`SCM_CONHAP`,
# MAGIC T.`SCM_HUNIT` = S.`SCM_HUNIT`,
# MAGIC T.`SCM_CONHAP_OUT` = S.`SCM_CONHAP_OUT`,
# MAGIC T.`SCM_HUNIT_OUT` = S.`SCM_HUNIT_OUT`,
# MAGIC T.`SCM_SHELF_LIFE_LOC` = S.`SCM_SHELF_LIFE_LOC`,
# MAGIC T.`SCM_SHELF_LIFE_DUR` = S.`SCM_SHELF_LIFE_DUR`,
# MAGIC T.`SCM_MATURITY_DUR` = S.`SCM_MATURITY_DUR`,
# MAGIC T.`SCM_SHLF_LFE_REQ_MIN` = S.`SCM_SHLF_LFE_REQ_MIN`,
# MAGIC T.`SCM_SHLF_LFE_REQ_MAX` = S.`SCM_SHLF_LFE_REQ_MAX`,
# MAGIC T.`SCM_LSUOM` = S.`SCM_LSUOM`,
# MAGIC T.`SCM_REORD_DUR` = S.`SCM_REORD_DUR`,
# MAGIC T.`SCM_TARGET_DUR` = S.`SCM_TARGET_DUR`,
# MAGIC T.`SCM_TSTRID` = S.`SCM_TSTRID`,
# MAGIC T.`SCM_STRA1` = S.`SCM_STRA1`,
# MAGIC T.`SCM_PEG_PAST_ALERT` = S.`SCM_PEG_PAST_ALERT`,
# MAGIC T.`SCM_PEG_FUTURE_ALERT` = S.`SCM_PEG_FUTURE_ALERT`,
# MAGIC T.`SCM_PEG_STRATEGY` = S.`SCM_PEG_STRATEGY`,
# MAGIC T.`SCM_PEG_WO_ALERT_FST` = S.`SCM_PEG_WO_ALERT_FST`,
# MAGIC T.`SCM_FIXPEG_PROD_SET` = S.`SCM_FIXPEG_PROD_SET`,
# MAGIC T.`SCM_WHATBOM` = S.`SCM_WHATBOM`,
# MAGIC T.`SCM_RRP_SEL_GROUP` = S.`SCM_RRP_SEL_GROUP`,
# MAGIC T.`SCM_INTSRC_PROF` = S.`SCM_INTSRC_PROF`,
# MAGIC T.`SCM_PRIO` = S.`SCM_PRIO`,
# MAGIC T.`SCM_MIN_PASS_AMOUNT` = S.`SCM_MIN_PASS_AMOUNT`,
# MAGIC T.`SCM_PROFID` = S.`SCM_PROFID`,
# MAGIC T.`SCM_GES_MNG_USE` = S.`SCM_GES_MNG_USE`,
# MAGIC T.`SCM_GES_BST_USE` = S.`SCM_GES_BST_USE`,
# MAGIC T.`ESPPFLG` = S.`ESPPFLG`,
# MAGIC T.`SCM_THRUPUT_TIME` = S.`SCM_THRUPUT_TIME`,
# MAGIC T.`SCM_TPOP` = S.`SCM_TPOP`,
# MAGIC T.`SCM_SAFTY_V` = S.`SCM_SAFTY_V`,
# MAGIC T.`SCM_PPSAFTYSTK` = S.`SCM_PPSAFTYSTK`,
# MAGIC T.`SCM_PPSAFTYSTK_V` = S.`SCM_PPSAFTYSTK_V`,
# MAGIC T.`SCM_REPSAFTY` = S.`SCM_REPSAFTY`,
# MAGIC T.`SCM_REPSAFTY_V` = S.`SCM_REPSAFTY_V`,
# MAGIC T.`SCM_REORD_V` = S.`SCM_REORD_V`,
# MAGIC T.`SCM_MAXSTOCK_V` = S.`SCM_MAXSTOCK_V`,
# MAGIC T.`SCM_SCOST_PRCNT` = S.`SCM_SCOST_PRCNT`,
# MAGIC T.`SCM_PROC_COST` = S.`SCM_PROC_COST`,
# MAGIC T.`SCM_NDCOSTWE` = S.`SCM_NDCOSTWE`,
# MAGIC T.`SCM_NDCOSTWA` = S.`SCM_NDCOSTWA`,
# MAGIC T.`SCM_CONINP` = S.`SCM_CONINP`,
# MAGIC T.`CONF_GMSYNC` = S.`CONF_GMSYNC`,
# MAGIC T.`SCM_IUNIT` = S.`SCM_IUNIT`,
# MAGIC T.`SCM_SFT_LOCK` = S.`SCM_SFT_LOCK`,
# MAGIC T.`DUMMY_PLNT_INCL_EEW_PS` = S.`DUMMY_PLNT_INCL_EEW_PS`,
# MAGIC T.`/SAPMP/TOLPRPL` = S.`/SAPMP/TOLPRPL`,
# MAGIC T.`/SAPMP/TOLPRMI` = S.`/SAPMP/TOLPRMI`,
# MAGIC T.`/STTPEC/SERVALID` = S.`/STTPEC/SERVALID`,
# MAGIC T.`/VSO/R_PKGRP` = S.`/VSO/R_PKGRP`,
# MAGIC T.`/VSO/R_LANE_NUM` = S.`/VSO/R_LANE_NUM`,
# MAGIC T.`/VSO/R_PAL_VEND` = S.`/VSO/R_PAL_VEND`,
# MAGIC T.`/VSO/R_FORK_DIR` = S.`/VSO/R_FORK_DIR`,
# MAGIC T.`IUID_RELEVANT` = S.`IUID_RELEVANT`,
# MAGIC T.`IUID_TYPE` = S.`IUID_TYPE`,
# MAGIC T.`UID_IEA` = S.`UID_IEA`,
# MAGIC T.`DPCBT` = S.`DPCBT`,
# MAGIC T.`ZZECCN` = S.`ZZECCN`,
# MAGIC T.`ZZSCHB` = S.`ZZSCHB`,
# MAGIC T.`ZZHTS` = S.`ZZHTS`,
# MAGIC T.`ZZECCN_YR` = S.`ZZECCN_YR`,
# MAGIC T.`ZZMATREF` = S.`ZZMATREF`,
# MAGIC T.`ZZROUTING` = S.`ZZROUTING`,
# MAGIC T.`ZZPRODCODE` = S.`ZZPRODCODE`,
# MAGIC T.`ZZSOECELLFUSION` = S.`ZZSOECELLFUSION`,
# MAGIC T.`ZZCHK` = S.`ZZCHK`,
# MAGIC T.`ZZPLNTGRP` = S.`ZZPLNTGRP`,
# MAGIC T.`ODQ_CHANGEMODE` = S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` = S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` = S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `MATNR`,
# MAGIC `WERKS`,
# MAGIC `PSTAT`,
# MAGIC `LVORM`,
# MAGIC `BWTTY`,
# MAGIC `XCHAR`,
# MAGIC `MMSTA`,
# MAGIC `MMSTD`,
# MAGIC `MAABC`,
# MAGIC `KZKRI`,
# MAGIC `EKGRP`,
# MAGIC `AUSME`,
# MAGIC `DISPR`,
# MAGIC `DISMM`,
# MAGIC `DISPO`,
# MAGIC `KZDIE`,
# MAGIC `PLIFZ`,
# MAGIC `WEBAZ`,
# MAGIC `PERKZ`,
# MAGIC `AUSSS`,
# MAGIC `DISLS`,
# MAGIC `BESKZ`,
# MAGIC `SOBSL`,
# MAGIC `MINBE`,
# MAGIC `EISBE`,
# MAGIC `BSTMI`,
# MAGIC `BSTMA`,
# MAGIC `BSTFE`,
# MAGIC `BSTRF`,
# MAGIC `MABST`,
# MAGIC `LOSFX`,
# MAGIC `SBDKZ`,
# MAGIC `LAGPR`,
# MAGIC `ALTSL`,
# MAGIC `KZAUS`,
# MAGIC `AUSDT`,
# MAGIC `NFMAT`,
# MAGIC `KZBED`,
# MAGIC `MISKZ`,
# MAGIC `FHORI`,
# MAGIC `PFREI`,
# MAGIC `FFREI`,
# MAGIC `RGEKZ`,
# MAGIC `FEVOR`,
# MAGIC `BEARZ`,
# MAGIC `RUEZT`,
# MAGIC `TRANZ`,
# MAGIC `BASMG`,
# MAGIC `DZEIT`,
# MAGIC `MAXLZ`,
# MAGIC `LZEIH`,
# MAGIC `KZPRO`,
# MAGIC `GPMKZ`,
# MAGIC `UEETO`,
# MAGIC `UEETK`,
# MAGIC `UNETO`,
# MAGIC `WZEIT`,
# MAGIC `ATPKZ`,
# MAGIC `VZUSL`,
# MAGIC `HERBL`,
# MAGIC `INSMK`,
# MAGIC `SPROZ`,
# MAGIC `QUAZT`,
# MAGIC `SSQSS`,
# MAGIC `MPDAU`,
# MAGIC `KZPPV`,
# MAGIC `KZDKZ`,
# MAGIC `WSTGH`,
# MAGIC `PRFRQ`,
# MAGIC `NKMPR`,
# MAGIC `UMLMC`,
# MAGIC `LADGR`,
# MAGIC `XCHPF`,
# MAGIC `USEQU`,
# MAGIC `LGRAD`,
# MAGIC `AUFTL`,
# MAGIC `PLVAR`,
# MAGIC `OTYPE`,
# MAGIC `OBJID`,
# MAGIC `MTVFP`,
# MAGIC `PERIV`,
# MAGIC `KZKFK`,
# MAGIC `VRVEZ`,
# MAGIC `VBAMG`,
# MAGIC `VBEAZ`,
# MAGIC `LIZYK`,
# MAGIC `BWSCL`,
# MAGIC `KAUTB`,
# MAGIC `KORDB`,
# MAGIC `STAWN`,
# MAGIC `HERKL`,
# MAGIC `HERKR`,
# MAGIC `EXPME`,
# MAGIC `MTVER`,
# MAGIC `PRCTR`,
# MAGIC `TRAME`,
# MAGIC `MRPPP`,
# MAGIC `SAUFT`,
# MAGIC `FXHOR`,
# MAGIC `VRMOD`,
# MAGIC `VINT1`,
# MAGIC `VINT2`,
# MAGIC `VERKZ`,
# MAGIC `STLAL`,
# MAGIC `STLAN`,
# MAGIC `PLNNR`,
# MAGIC `APLAL`,
# MAGIC `LOSGR`,
# MAGIC `SOBSK`,
# MAGIC `FRTME`,
# MAGIC `LGPRO`,
# MAGIC `DISGR`,
# MAGIC `KAUSF`,
# MAGIC `QZGTP`,
# MAGIC `QMATV`,
# MAGIC `TAKZT`,
# MAGIC `RWPRO`,
# MAGIC `COPAM`,
# MAGIC `ABCIN`,
# MAGIC `AWSLS`,
# MAGIC `SERNP`,
# MAGIC `CUOBJ`,
# MAGIC `STDPD`,
# MAGIC `SFEPR`,
# MAGIC `XMCNG`,
# MAGIC `QSSYS`,
# MAGIC `LFRHY`,
# MAGIC `RDPRF`,
# MAGIC `VRBMT`,
# MAGIC `VRBWK`,
# MAGIC `VRBDT`,
# MAGIC `VRBFK`,
# MAGIC `AUTRU`,
# MAGIC `PREFE`,
# MAGIC `PRENC`,
# MAGIC `PRENO`,
# MAGIC `PREND`,
# MAGIC `PRENE`,
# MAGIC `PRENG`,
# MAGIC `ITARK`,
# MAGIC `SERVG`,
# MAGIC `KZKUP`,
# MAGIC `STRGR`,
# MAGIC `CUOBV`,
# MAGIC `LGFSB`,
# MAGIC `SCHGT`,
# MAGIC `CCFIX`,
# MAGIC `EPRIO`,
# MAGIC `QMATA`,
# MAGIC `RESVP`,
# MAGIC `PLNTY`,
# MAGIC `UOMGR`,
# MAGIC `UMRSL`,
# MAGIC `ABFAC`,
# MAGIC `SFCPF`,
# MAGIC `SHFLG`,
# MAGIC `SHZET`,
# MAGIC `MDACH`,
# MAGIC `KZECH`,
# MAGIC `MEGRU`,
# MAGIC `MFRGR`,
# MAGIC `PROFIL`,
# MAGIC `VKUMC`,
# MAGIC `VKTRW`,
# MAGIC `KZAGL`,
# MAGIC `FVIDK`,
# MAGIC `FXPRU`,
# MAGIC `LOGGR`,
# MAGIC `FPRFM`,
# MAGIC `GLGMG`,
# MAGIC `VKGLG`,
# MAGIC `INDUS`,
# MAGIC `MOWNR`,
# MAGIC `MOGRU`,
# MAGIC `CASNR`,
# MAGIC `GPNUM`,
# MAGIC `STEUC`,
# MAGIC `FABKZ`,
# MAGIC `MATGR`,
# MAGIC `VSPVB`,
# MAGIC `DPLFS`,
# MAGIC `DPLPU`,
# MAGIC `DPLHO`,
# MAGIC `MINLS`,
# MAGIC `MAXLS`,
# MAGIC `FIXLS`,
# MAGIC `LTINC`,
# MAGIC `COMPL`,
# MAGIC `CONVT`,
# MAGIC `SHPRO`,
# MAGIC `AHDIS`,
# MAGIC `DIBER`,
# MAGIC `KZPSP`,
# MAGIC `OCMPF`,
# MAGIC `APOKZ`,
# MAGIC `MCRUE`,
# MAGIC `LFMON`,
# MAGIC `LFGJA`,
# MAGIC `EISLO`,
# MAGIC `NCOST`,
# MAGIC `ROTATION_DATE`,
# MAGIC `UCHKZ`,
# MAGIC `UCMAT`,
# MAGIC `EXCISE_TAX_RLVNCE`,
# MAGIC `BWESB`,
# MAGIC `SGT_COVS`,
# MAGIC `SGT_STATC`,
# MAGIC `SGT_SCOPE`,
# MAGIC `SGT_MRPSI`,
# MAGIC `SGT_PRCM`,
# MAGIC `SGT_CHINT`,
# MAGIC `SGT_STK_PRT`,
# MAGIC `SGT_DEFSC`,
# MAGIC `SGT_MRP_ATP_STATUS`,
# MAGIC `SGT_MMSTD`,
# MAGIC `FSH_MG_ARUN_REQ`,
# MAGIC `FSH_SEAIM`,
# MAGIC `FSH_VAR_GROUP`,
# MAGIC `FSH_KZECH`,
# MAGIC `FSH_CALENDAR_GROUP`,
# MAGIC `ARUN_FIX_BATCH`,
# MAGIC `PPSKZ`,
# MAGIC `CONS_PROCG`,
# MAGIC `GI_PR_TIME`,
# MAGIC `MULTIPLE_EKGRP`,
# MAGIC `REF_SCHEMA`,
# MAGIC `MIN_TROC`,
# MAGIC `MAX_TROC`,
# MAGIC `TARGET_STOCK`,
# MAGIC `NF_FLAG`,
# MAGIC `/CWM/UMLMC`,
# MAGIC `/CWM/TRAME`,
# MAGIC `/CWM/BWESB`,
# MAGIC `SCM_MATLOCID_GUID16`,
# MAGIC `SCM_MATLOCID_GUID22`,
# MAGIC `SCM_GRPRT`,
# MAGIC `SCM_GIPRT`,
# MAGIC `SCM_SCOST`,
# MAGIC `SCM_RELDT`,
# MAGIC `SCM_RRP_TYPE`,
# MAGIC `SCM_HEUR_ID`,
# MAGIC `SCM_PACKAGE_ID`,
# MAGIC `SCM_SSPEN`,
# MAGIC `SCM_GET_ALERTS`,
# MAGIC `SCM_RES_NET_NAME`,
# MAGIC `SCM_CONHAP`,
# MAGIC `SCM_HUNIT`,
# MAGIC `SCM_CONHAP_OUT`,
# MAGIC `SCM_HUNIT_OUT`,
# MAGIC `SCM_SHELF_LIFE_LOC`,
# MAGIC `SCM_SHELF_LIFE_DUR`,
# MAGIC `SCM_MATURITY_DUR`,
# MAGIC `SCM_SHLF_LFE_REQ_MIN`,
# MAGIC `SCM_SHLF_LFE_REQ_MAX`,
# MAGIC `SCM_LSUOM`,
# MAGIC `SCM_REORD_DUR`,
# MAGIC `SCM_TARGET_DUR`,
# MAGIC `SCM_TSTRID`,
# MAGIC `SCM_STRA1`,
# MAGIC `SCM_PEG_PAST_ALERT`,
# MAGIC `SCM_PEG_FUTURE_ALERT`,
# MAGIC `SCM_PEG_STRATEGY`,
# MAGIC `SCM_PEG_WO_ALERT_FST`,
# MAGIC `SCM_FIXPEG_PROD_SET`,
# MAGIC `SCM_WHATBOM`,
# MAGIC `SCM_RRP_SEL_GROUP`,
# MAGIC `SCM_INTSRC_PROF`,
# MAGIC `SCM_PRIO`,
# MAGIC `SCM_MIN_PASS_AMOUNT`,
# MAGIC `SCM_PROFID`,
# MAGIC `SCM_GES_MNG_USE`,
# MAGIC `SCM_GES_BST_USE`,
# MAGIC `ESPPFLG`,
# MAGIC `SCM_THRUPUT_TIME`,
# MAGIC `SCM_TPOP`,
# MAGIC `SCM_SAFTY_V`,
# MAGIC `SCM_PPSAFTYSTK`,
# MAGIC `SCM_PPSAFTYSTK_V`,
# MAGIC `SCM_REPSAFTY`,
# MAGIC `SCM_REPSAFTY_V`,
# MAGIC `SCM_REORD_V`,
# MAGIC `SCM_MAXSTOCK_V`,
# MAGIC `SCM_SCOST_PRCNT`,
# MAGIC `SCM_PROC_COST`,
# MAGIC `SCM_NDCOSTWE`,
# MAGIC `SCM_NDCOSTWA`,
# MAGIC `SCM_CONINP`,
# MAGIC `CONF_GMSYNC`,
# MAGIC `SCM_IUNIT`,
# MAGIC `SCM_SFT_LOCK`,
# MAGIC `DUMMY_PLNT_INCL_EEW_PS`,
# MAGIC `/SAPMP/TOLPRPL`,
# MAGIC `/SAPMP/TOLPRMI`,
# MAGIC `/STTPEC/SERVALID`,
# MAGIC `/VSO/R_PKGRP`,
# MAGIC `/VSO/R_LANE_NUM`,
# MAGIC `/VSO/R_PAL_VEND`,
# MAGIC `/VSO/R_FORK_DIR`,
# MAGIC `IUID_RELEVANT`,
# MAGIC `IUID_TYPE`,
# MAGIC `UID_IEA`,
# MAGIC `DPCBT`,
# MAGIC `ZZECCN`,
# MAGIC `ZZSCHB`,
# MAGIC `ZZHTS`,
# MAGIC `ZZECCN_YR`,
# MAGIC `ZZMATREF`,
# MAGIC `ZZROUTING`,
# MAGIC `ZZPRODCODE`,
# MAGIC `ZZSOECELLFUSION`,
# MAGIC `ZZCHK`,
# MAGIC `ZZPLNTGRP`,
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
# MAGIC S.`WERKS`,
# MAGIC S.`PSTAT`,
# MAGIC S.`LVORM`,
# MAGIC S.`BWTTY`,
# MAGIC S.`XCHAR`,
# MAGIC S.`MMSTA`,
# MAGIC S.`MMSTD`,
# MAGIC S.`MAABC`,
# MAGIC S.`KZKRI`,
# MAGIC S.`EKGRP`,
# MAGIC S.`AUSME`,
# MAGIC S.`DISPR`,
# MAGIC S.`DISMM`,
# MAGIC S.`DISPO`,
# MAGIC S.`KZDIE`,
# MAGIC S.`PLIFZ`,
# MAGIC S.`WEBAZ`,
# MAGIC S.`PERKZ`,
# MAGIC S.`AUSSS`,
# MAGIC S.`DISLS`,
# MAGIC S.`BESKZ`,
# MAGIC S.`SOBSL`,
# MAGIC S.`MINBE`,
# MAGIC S.`EISBE`,
# MAGIC S.`BSTMI`,
# MAGIC S.`BSTMA`,
# MAGIC S.`BSTFE`,
# MAGIC S.`BSTRF`,
# MAGIC S.`MABST`,
# MAGIC S.`LOSFX`,
# MAGIC S.`SBDKZ`,
# MAGIC S.`LAGPR`,
# MAGIC S.`ALTSL`,
# MAGIC S.`KZAUS`,
# MAGIC S.`AUSDT`,
# MAGIC S.`NFMAT`,
# MAGIC S.`KZBED`,
# MAGIC S.`MISKZ`,
# MAGIC S.`FHORI`,
# MAGIC S.`PFREI`,
# MAGIC S.`FFREI`,
# MAGIC S.`RGEKZ`,
# MAGIC S.`FEVOR`,
# MAGIC S.`BEARZ`,
# MAGIC S.`RUEZT`,
# MAGIC S.`TRANZ`,
# MAGIC S.`BASMG`,
# MAGIC S.`DZEIT`,
# MAGIC S.`MAXLZ`,
# MAGIC S.`LZEIH`,
# MAGIC S.`KZPRO`,
# MAGIC S.`GPMKZ`,
# MAGIC S.`UEETO`,
# MAGIC S.`UEETK`,
# MAGIC S.`UNETO`,
# MAGIC S.`WZEIT`,
# MAGIC S.`ATPKZ`,
# MAGIC S.`VZUSL`,
# MAGIC S.`HERBL`,
# MAGIC S.`INSMK`,
# MAGIC S.`SPROZ`,
# MAGIC S.`QUAZT`,
# MAGIC S.`SSQSS`,
# MAGIC S.`MPDAU`,
# MAGIC S.`KZPPV`,
# MAGIC S.`KZDKZ`,
# MAGIC S.`WSTGH`,
# MAGIC S.`PRFRQ`,
# MAGIC S.`NKMPR`,
# MAGIC S.`UMLMC`,
# MAGIC S.`LADGR`,
# MAGIC S.`XCHPF`,
# MAGIC S.`USEQU`,
# MAGIC S.`LGRAD`,
# MAGIC S.`AUFTL`,
# MAGIC S.`PLVAR`,
# MAGIC S.`OTYPE`,
# MAGIC S.`OBJID`,
# MAGIC S.`MTVFP`,
# MAGIC S.`PERIV`,
# MAGIC S.`KZKFK`,
# MAGIC S.`VRVEZ`,
# MAGIC S.`VBAMG`,
# MAGIC S.`VBEAZ`,
# MAGIC S.`LIZYK`,
# MAGIC S.`BWSCL`,
# MAGIC S.`KAUTB`,
# MAGIC S.`KORDB`,
# MAGIC S.`STAWN`,
# MAGIC S.`HERKL`,
# MAGIC S.`HERKR`,
# MAGIC S.`EXPME`,
# MAGIC S.`MTVER`,
# MAGIC S.`PRCTR`,
# MAGIC S.`TRAME`,
# MAGIC S.`MRPPP`,
# MAGIC S.`SAUFT`,
# MAGIC S.`FXHOR`,
# MAGIC S.`VRMOD`,
# MAGIC S.`VINT1`,
# MAGIC S.`VINT2`,
# MAGIC S.`VERKZ`,
# MAGIC S.`STLAL`,
# MAGIC S.`STLAN`,
# MAGIC S.`PLNNR`,
# MAGIC S.`APLAL`,
# MAGIC S.`LOSGR`,
# MAGIC S.`SOBSK`,
# MAGIC S.`FRTME`,
# MAGIC S.`LGPRO`,
# MAGIC S.`DISGR`,
# MAGIC S.`KAUSF`,
# MAGIC S.`QZGTP`,
# MAGIC S.`QMATV`,
# MAGIC S.`TAKZT`,
# MAGIC S.`RWPRO`,
# MAGIC S.`COPAM`,
# MAGIC S.`ABCIN`,
# MAGIC S.`AWSLS`,
# MAGIC S.`SERNP`,
# MAGIC S.`CUOBJ`,
# MAGIC S.`STDPD`,
# MAGIC S.`SFEPR`,
# MAGIC S.`XMCNG`,
# MAGIC S.`QSSYS`,
# MAGIC S.`LFRHY`,
# MAGIC S.`RDPRF`,
# MAGIC S.`VRBMT`,
# MAGIC S.`VRBWK`,
# MAGIC S.`VRBDT`,
# MAGIC S.`VRBFK`,
# MAGIC S.`AUTRU`,
# MAGIC S.`PREFE`,
# MAGIC S.`PRENC`,
# MAGIC S.`PRENO`,
# MAGIC S.`PREND`,
# MAGIC S.`PRENE`,
# MAGIC S.`PRENG`,
# MAGIC S.`ITARK`,
# MAGIC S.`SERVG`,
# MAGIC S.`KZKUP`,
# MAGIC S.`STRGR`,
# MAGIC S.`CUOBV`,
# MAGIC S.`LGFSB`,
# MAGIC S.`SCHGT`,
# MAGIC S.`CCFIX`,
# MAGIC S.`EPRIO`,
# MAGIC S.`QMATA`,
# MAGIC S.`RESVP`,
# MAGIC S.`PLNTY`,
# MAGIC S.`UOMGR`,
# MAGIC S.`UMRSL`,
# MAGIC S.`ABFAC`,
# MAGIC S.`SFCPF`,
# MAGIC S.`SHFLG`,
# MAGIC S.`SHZET`,
# MAGIC S.`MDACH`,
# MAGIC S.`KZECH`,
# MAGIC S.`MEGRU`,
# MAGIC S.`MFRGR`,
# MAGIC S.`PROFIL`,
# MAGIC S.`VKUMC`,
# MAGIC S.`VKTRW`,
# MAGIC S.`KZAGL`,
# MAGIC S.`FVIDK`,
# MAGIC S.`FXPRU`,
# MAGIC S.`LOGGR`,
# MAGIC S.`FPRFM`,
# MAGIC S.`GLGMG`,
# MAGIC S.`VKGLG`,
# MAGIC S.`INDUS`,
# MAGIC S.`MOWNR`,
# MAGIC S.`MOGRU`,
# MAGIC S.`CASNR`,
# MAGIC S.`GPNUM`,
# MAGIC S.`STEUC`,
# MAGIC S.`FABKZ`,
# MAGIC S.`MATGR`,
# MAGIC S.`VSPVB`,
# MAGIC S.`DPLFS`,
# MAGIC S.`DPLPU`,
# MAGIC S.`DPLHO`,
# MAGIC S.`MINLS`,
# MAGIC S.`MAXLS`,
# MAGIC S.`FIXLS`,
# MAGIC S.`LTINC`,
# MAGIC S.`COMPL`,
# MAGIC S.`CONVT`,
# MAGIC S.`SHPRO`,
# MAGIC S.`AHDIS`,
# MAGIC S.`DIBER`,
# MAGIC S.`KZPSP`,
# MAGIC S.`OCMPF`,
# MAGIC S.`APOKZ`,
# MAGIC S.`MCRUE`,
# MAGIC S.`LFMON`,
# MAGIC S.`LFGJA`,
# MAGIC S.`EISLO`,
# MAGIC S.`NCOST`,
# MAGIC S.`ROTATION_DATE`,
# MAGIC S.`UCHKZ`,
# MAGIC S.`UCMAT`,
# MAGIC S.`EXCISE_TAX_RLVNCE`,
# MAGIC S.`BWESB`,
# MAGIC S.`SGT_COVS`,
# MAGIC S.`SGT_STATC`,
# MAGIC S.`SGT_SCOPE`,
# MAGIC S.`SGT_MRPSI`,
# MAGIC S.`SGT_PRCM`,
# MAGIC S.`SGT_CHINT`,
# MAGIC S.`SGT_STK_PRT`,
# MAGIC S.`SGT_DEFSC`,
# MAGIC S.`SGT_MRP_ATP_STATUS`,
# MAGIC S.`SGT_MMSTD`,
# MAGIC S.`FSH_MG_ARUN_REQ`,
# MAGIC S.`FSH_SEAIM`,
# MAGIC S.`FSH_VAR_GROUP`,
# MAGIC S.`FSH_KZECH`,
# MAGIC S.`FSH_CALENDAR_GROUP`,
# MAGIC S.`ARUN_FIX_BATCH`,
# MAGIC S.`PPSKZ`,
# MAGIC S.`CONS_PROCG`,
# MAGIC S.`GI_PR_TIME`,
# MAGIC S.`MULTIPLE_EKGRP`,
# MAGIC S.`REF_SCHEMA`,
# MAGIC S.`MIN_TROC`,
# MAGIC S.`MAX_TROC`,
# MAGIC S.`TARGET_STOCK`,
# MAGIC S.`NF_FLAG`,
# MAGIC S.`/CWM/UMLMC`,
# MAGIC S.`/CWM/TRAME`,
# MAGIC S.`/CWM/BWESB`,
# MAGIC S.`SCM_MATLOCID_GUID16`,
# MAGIC S.`SCM_MATLOCID_GUID22`,
# MAGIC S.`SCM_GRPRT`,
# MAGIC S.`SCM_GIPRT`,
# MAGIC S.`SCM_SCOST`,
# MAGIC S.`SCM_RELDT`,
# MAGIC S.`SCM_RRP_TYPE`,
# MAGIC S.`SCM_HEUR_ID`,
# MAGIC S.`SCM_PACKAGE_ID`,
# MAGIC S.`SCM_SSPEN`,
# MAGIC S.`SCM_GET_ALERTS`,
# MAGIC S.`SCM_RES_NET_NAME`,
# MAGIC S.`SCM_CONHAP`,
# MAGIC S.`SCM_HUNIT`,
# MAGIC S.`SCM_CONHAP_OUT`,
# MAGIC S.`SCM_HUNIT_OUT`,
# MAGIC S.`SCM_SHELF_LIFE_LOC`,
# MAGIC S.`SCM_SHELF_LIFE_DUR`,
# MAGIC S.`SCM_MATURITY_DUR`,
# MAGIC S.`SCM_SHLF_LFE_REQ_MIN`,
# MAGIC S.`SCM_SHLF_LFE_REQ_MAX`,
# MAGIC S.`SCM_LSUOM`,
# MAGIC S.`SCM_REORD_DUR`,
# MAGIC S.`SCM_TARGET_DUR`,
# MAGIC S.`SCM_TSTRID`,
# MAGIC S.`SCM_STRA1`,
# MAGIC S.`SCM_PEG_PAST_ALERT`,
# MAGIC S.`SCM_PEG_FUTURE_ALERT`,
# MAGIC S.`SCM_PEG_STRATEGY`,
# MAGIC S.`SCM_PEG_WO_ALERT_FST`,
# MAGIC S.`SCM_FIXPEG_PROD_SET`,
# MAGIC S.`SCM_WHATBOM`,
# MAGIC S.`SCM_RRP_SEL_GROUP`,
# MAGIC S.`SCM_INTSRC_PROF`,
# MAGIC S.`SCM_PRIO`,
# MAGIC S.`SCM_MIN_PASS_AMOUNT`,
# MAGIC S.`SCM_PROFID`,
# MAGIC S.`SCM_GES_MNG_USE`,
# MAGIC S.`SCM_GES_BST_USE`,
# MAGIC S.`ESPPFLG`,
# MAGIC S.`SCM_THRUPUT_TIME`,
# MAGIC S.`SCM_TPOP`,
# MAGIC S.`SCM_SAFTY_V`,
# MAGIC S.`SCM_PPSAFTYSTK`,
# MAGIC S.`SCM_PPSAFTYSTK_V`,
# MAGIC S.`SCM_REPSAFTY`,
# MAGIC S.`SCM_REPSAFTY_V`,
# MAGIC S.`SCM_REORD_V`,
# MAGIC S.`SCM_MAXSTOCK_V`,
# MAGIC S.`SCM_SCOST_PRCNT`,
# MAGIC S.`SCM_PROC_COST`,
# MAGIC S.`SCM_NDCOSTWE`,
# MAGIC S.`SCM_NDCOSTWA`,
# MAGIC S.`SCM_CONINP`,
# MAGIC S.`CONF_GMSYNC`,
# MAGIC S.`SCM_IUNIT`,
# MAGIC S.`SCM_SFT_LOCK`,
# MAGIC S.`DUMMY_PLNT_INCL_EEW_PS`,
# MAGIC S.`/SAPMP/TOLPRPL`,
# MAGIC S.`/SAPMP/TOLPRMI`,
# MAGIC S.`/STTPEC/SERVALID`,
# MAGIC S.`/VSO/R_PKGRP`,
# MAGIC S.`/VSO/R_LANE_NUM`,
# MAGIC S.`/VSO/R_PAL_VEND`,
# MAGIC S.`/VSO/R_FORK_DIR`,
# MAGIC S.`IUID_RELEVANT`,
# MAGIC S.`IUID_TYPE`,
# MAGIC S.`UID_IEA`,
# MAGIC S.`DPCBT`,
# MAGIC S.`ZZECCN`,
# MAGIC S.`ZZSCHB`,
# MAGIC S.`ZZHTS`,
# MAGIC S.`ZZECCN_YR`,
# MAGIC S.`ZZMATREF`,
# MAGIC S.`ZZROUTING`,
# MAGIC S.`ZZPRODCODE`,
# MAGIC S.`ZZSOECELLFUSION`,
# MAGIC S.`ZZCHK`,
# MAGIC S.`ZZPLNTGRP`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.LandingFileTimeStamp,
# MAGIC 'SAP',
# MAGIC now()
# MAGIC )
# MAGIC  

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)
