# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'EKPO'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

schema = StructType([StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
                     StructField('DI_OPERATION_TYPE',StringType(),True),\
                     StructField('MANDT',IntegerType(),True),\
                     StructField('EBELN',LongType(),True),\
StructField('EBELP',IntegerType(),True),\
StructField('UNIQUEID',LongType(),True),\
StructField('LOEKZ',StringType(),True),\
StructField('STATU',StringType(),True),\
StructField('AEDAT',StringType(),True),\
StructField('TXZ01',StringType(),True),\
StructField('MATNR',StringType(),True),\
StructField('EMATN',StringType(),True),\
StructField('BUKRS',StringType(),True),\
StructField('WERKS',StringType(),True),\
StructField('LGORT',StringType(),True),\
StructField('BEDNR',StringType(),True),\
StructField('MATKL',StringType(),True),\
StructField('INFNR',StringType(),True),\
StructField('IDNLF',StringType(),True),\
StructField('KTMNG',StringType(),True),\
StructField('MENGE',StringType(),True),\
StructField('MEINS',StringType(),True),\
StructField('BPRME',StringType(),True),\
StructField('BPUMZ',StringType(),True),\
StructField('BPUMN',StringType(),True),\
StructField('UMREZ',IntegerType(),True),\
StructField('UMREN',IntegerType(),True),\
StructField('NETPR',DoubleType(),True),\
StructField('PEINH',DoubleType(),True),\
StructField('NETWR',DoubleType(),True),\
StructField('BRTWR',DoubleType(),True),\
StructField('AGDAT',StringType(),True),\
StructField('WEBAZ',StringType(),True),\
StructField('MWSKZ',StringType(),True),\
StructField('TXDAT_FROM',StringType(),True),\
StructField('TXDAT',StringType(),True),\
StructField('TAX_COUNTRY',StringType(),True),\
StructField('BONUS',StringType(),True),\
StructField('INSMK',StringType(),True),\
StructField('SPINF',StringType(),True),\
StructField('PRSDR',StringType(),True),\
StructField('SCHPR',StringType(),True),\
StructField('MAHNZ',StringType(),True),\
StructField('MAHN1',StringType(),True),\
StructField('MAHN2',IntegerType(),True),\
StructField('MAHN3',IntegerType(),True),\
StructField('UEBTO',DoubleType(),True),\
StructField('UEBTK',StringType(),True),\
StructField('UNTTO',StringType(),True),\
StructField('BWTAR',StringType(),True),\
StructField('BWTTY',StringType(),True),\
StructField('ABSKZ',StringType(),True),\
StructField('AGMEM',StringType(),True),\
StructField('ELIKZ',StringType(),True),\
StructField('EREKZ',StringType(),True),\
StructField('PSTYP',StringType(),True),\
StructField('KNTTP',StringType(),True),\
StructField('KZVBR',StringType(),True),\
StructField('VRTKZ',StringType(),True),\
StructField('TWRKZ',StringType(),True),\
StructField('WEPOS',StringType(),True),\
StructField('WEUNB',StringType(),True),\
StructField('REPOS',StringType(),True),\
StructField('WEBRE',StringType(),True),\
StructField('KZABS',StringType(),True),\
StructField('LABNR',StringType(),True),\
StructField('KONNR',StringType(),True),\
StructField('KTPNR',StringType(),True),\
StructField('ABDAT',StringType(),True),\
StructField('ABFTZ',StringType(),True),\
StructField('ETFZ1',StringType(),True),\
StructField('ETFZ2',DoubleType(),True),\
StructField('KZSTU',StringType(),True),\
StructField('NOTKZ',StringType(),True),\
StructField('LMEIN',StringType(),True),\
StructField('EVERS',StringType(),True),\
StructField('ZWERT',StringType(),True),\
StructField('NAVNW',StringType(),True),\
StructField('ABMNG',DoubleType(),True),\
StructField('PRDAT',StringType(),True),\
StructField('BSTYP',StringType(),True),\
StructField('EFFWR',StringType(),True),\
StructField('XOBLR',StringType(),True),\
StructField('KUNNR',StringType(),True),\
StructField('ADRNR',StringType(),True),\
StructField('EKKOL',StringType(),True),\
StructField('SKTOF',StringType(),True),\
StructField('STAFO',StringType(),True),\
StructField('PLIFZ',StringType(),True),\
StructField('NTGEW',StringType(),True),\
StructField('GEWEI',StringType(),True),\
StructField('TXJCD',StringType(),True),\
StructField('ETDRK',StringType(),True),\
StructField('SOBKZ',StringType(),True),\
StructField('ARSNR',StringType(),True),\
StructField('ARSPS',StringType(),True),\
StructField('INSNC',StringType(),True),\
StructField('SSQSS',StringType(),True),\
StructField('ZGTYP',StringType(),True),\
StructField('EAN11',StringType(),True),\
StructField('BSTAE',StringType(),True),\
StructField('REVLV',StringType(),True),\
StructField('GEBER',StringType(),True),\
StructField('FISTL',StringType(),True),\
StructField('FIPOS',StringType(),True),\
StructField('KO_GSBER',StringType(),True),\
StructField('KO_PARGB',StringType(),True),\
StructField('KO_PRCTR',StringType(),True),\
StructField('KO_PPRCTR',StringType(),True),\
StructField('MEPRF',StringType(),True),\
StructField('BRGEW',StringType(),True),\
StructField('VOLUM',StringType(),True),\
StructField('VOLEH',StringType(),True),\
StructField('INCO1',StringType(),True),\
StructField('INCO2',StringType(),True),\
StructField('VORAB',StringType(),True),\
StructField('KOLIF',StringType(),True),\
StructField('LTSNR',StringType(),True),\
StructField('PACKNO',StringType(),True),\
StructField('FPLNR',StringType(),True),\
StructField('GNETWR',StringType(),True),\
StructField('STAPO',StringType(),True),\
StructField('UEBPO',StringType(),True),\
StructField('LEWED',StringType(),True),\
StructField('EMLIF',StringType(),True),\
StructField('LBLKZ',StringType(),True),\
StructField('SATNR',StringType(),True),\
StructField('ATTYP',StringType(),True),\
StructField('VSART',StringType(),True),\
StructField('HANDOVERLOC',StringType(),True),\
StructField('KANBA',StringType(),True),\
StructField('ADRN2',StringType(),True),\
StructField('CUOBJ',StringType(),True),\
StructField('XERSY',StringType(),True),\
StructField('EILDT',StringType(),True),\
StructField('DRDAT',StringType(),True),\
StructField('DRUHR',StringType(),True),\
StructField('DRUNR',StringType(),True),\
StructField('AKTNR',StringType(),True),\
StructField('ABELN',StringType(),True),\
StructField('ABELP',StringType(),True),\
StructField('ANZPU',StringType(),True),\
StructField('PUNEI',StringType(),True),\
StructField('SAISO',StringType(),True),\
StructField('SAISJ',StringType(),True),\
StructField('EBON2',StringType(),True),\
StructField('EBON3',StringType(),True),\
StructField('EBONF',StringType(),True),\
StructField('MLMAA',StringType(),True),\
StructField('MHDRZ',StringType(),True),\
StructField('ANFNR',StringType(),True),\
StructField('ANFPS',StringType(),True),\
StructField('KZKFG',StringType(),True),\
StructField('USEQU',StringType(),True),\
StructField('UMSOK',StringType(),True),\
StructField('BANFN',StringType(),True),\
StructField('BNFPO',StringType(),True),\
StructField('MTART',StringType(),True),\
StructField('UPTYP',StringType(),True),\
StructField('UPVOR',StringType(),True),\
StructField('KZWI1',StringType(),True),\
StructField('KZWI2',StringType(),True),\
StructField('KZWI3',DoubleType(),True),\
StructField('KZWI4',DoubleType(),True),\
StructField('KZWI5',DoubleType(),True),\
StructField('KZWI6',DoubleType(),True),\
StructField('SIKGR',StringType(),True),\
StructField('MFZHI',StringType(),True),\
StructField('FFZHI',StringType(),True),\
StructField('RETPO',StringType(),True),\
StructField('AUREL',StringType(),True),\
StructField('BSGRU',StringType(),True),\
StructField('LFRET',StringType(),True),\
StructField('MFRGR',StringType(),True),\
StructField('NRFHG',StringType(),True),\
StructField('J_1BNBM',StringType(),True),\
StructField('J_1BMATUSE',StringType(),True),\
StructField('J_1BMATORG',StringType(),True),\
StructField('J_1BOWNPRO',StringType(),True),\
StructField('J_1BINDUST',StringType(),True),\
StructField('ABUEB',StringType(),True),\
StructField('NLABD',StringType(),True),\
StructField('NFABD',StringType(),True),\
StructField('KZBWS',StringType(),True),\
StructField('BONBA',StringType(),True),\
StructField('FABKZ',StringType(),True),\
StructField('LOADINGPOINT',StringType(),True),\
StructField('J_1AINDXP',StringType(),True),\
StructField('J_1AIDATEP',StringType(),True),\
StructField('MPROF',StringType(),True),\
StructField('EGLKZ',StringType(),True),\
StructField('KZTLF',StringType(),True),\
StructField('KZFME',StringType(),True),\
StructField('RDPRF',StringType(),True),\
StructField('TECHS',StringType(),True),\
StructField('CHG_SRV',StringType(),True),\
StructField('CHG_FPLNR',StringType(),True),\
StructField('MFRPN',StringType(),True),\
StructField('MFRNR',StringType(),True),\
StructField('EMNFR',StringType(),True),\
StructField('NOVET',StringType(),True),\
StructField('AFNAM',StringType(),True),\
StructField('TZONRC',StringType(),True),\
StructField('IPRKZ',StringType(),True),\
StructField('LEBRE',StringType(),True),\
StructField('BERID',StringType(),True),\
StructField('XCONDITIONS',StringType(),True),\
StructField('APOMS',StringType(),True),\
StructField('CCOMP',StringType(),True),\
StructField('GRANT_NBR',StringType(),True),\
StructField('FKBER',StringType(),True),\
StructField('STATUS',StringType(),True),\
StructField('RESLO',StringType(),True),\
StructField('KBLNR',StringType(),True),\
StructField('KBLPOS',StringType(),True),\
StructField('PS_PSP_PNR',StringType(),True),\
StructField('KOSTL',StringType(),True),\
StructField('SAKTO',StringType(),True),\
StructField('WEORA',StringType(),True),\
StructField('SRV_BAS_COM',StringType(),True),\
StructField('PRIO_URG',StringType(),True),\
StructField('PRIO_REQ',StringType(),True),\
StructField('EMPST',StringType(),True),\
StructField('DIFF_INVOICE',StringType(),True),\
StructField('TRMRISK_RELEVANT',StringType(),True),\
StructField('CREATIONDATE',StringType(),True),\
StructField('CREATIONTIME',StringType(),True),\
StructField('SPE_ABGRU',StringType(),True),\
StructField('SPE_CRM_SO',StringType(),True),\
StructField('SPE_CRM_SO_ITEM',StringType(),True),\
StructField('SPE_CRM_REF_SO',StringType(),True),\
StructField('SPE_CRM_REF_ITEM',StringType(),True),\
StructField('SPE_CRM_FKREL',StringType(),True),\
StructField('SPE_CHNG_SYS',StringType(),True),\
StructField('SPE_INSMK_SRC',StringType(),True),\
StructField('SPE_CQ_CTRLTYPE',StringType(),True),\
StructField('SPE_CQ_NOCQ',StringType(),True),\
StructField('REASON_CODE',StringType(),True),\
StructField('CQU_SAR',StringType(),True),\
StructField('ANZSN',StringType(),True),\
StructField('SPE_EWM_DTC',StringType(),True),\
StructField('EXLIN',StringType(),True),\
StructField('EXSNR',StringType(),True),\
StructField('EHTYP',StringType(),True),\
StructField('RETPC',StringType(),True),\
StructField('DPTYP',StringType(),True),\
StructField('DPPCT',StringType(),True),\
StructField('DPAMT',StringType(),True),\
StructField('DPDAT',StringType(),True),\
StructField('FLS_RSTO',StringType(),True),\
StructField('EXT_RFX_NUMBER',StringType(),True),\
StructField('EXT_RFX_ITEM',StringType(),True),\
StructField('EXT_RFX_SYSTEM',StringType(),True),\
StructField('SRM_CONTRACT_ID',StringType(),True),\
StructField('SRM_CONTRACT_ITM',StringType(),True),\
StructField('BLK_REASON_ID',StringType(),True),\
StructField('BLK_REASON_TXT',StringType(),True),\
StructField('ITCONS',StringType(),True),\
StructField('FIXMG',StringType(),True),\
StructField('WABWE',StringType(),True),\
StructField('CMPL_DLV_ITM',StringType(),True),\
StructField('INCO2_L',StringType(),True),\
StructField('INCO3_L',StringType(),True),\
StructField('STAWN',StringType(),True),\
StructField('ISVCO',StringType(),True),\
StructField('GRWRT',StringType(),True),\
StructField('SERVICEPERFORMER',StringType(),True),\
StructField('PRODUCTTYPE',StringType(),True),\
StructField('GR_BY_SES',StringType(),True),\
StructField('REQUESTFORQUOTATION',StringType(),True),\
StructField('REQUESTFORQUOTATIONITEM',StringType(),True),\
StructField('EXTMATERIALFORPURG',StringType(),True),\
StructField('TARGET_VALUE',StringType(),True),\
StructField('EXTERNALREFERENCEID',StringType(),True),\
StructField('TC_AUT_DET',StringType(),True),\
StructField('MANUAL_TC_REASON',StringType(),True),\
StructField('FISCAL_INCENTIVE',StringType(),True),\
StructField('TAX_SUBJECT_ST',StringType(),True),\
StructField('FISCAL_INCENTIVE_ID',StringType(),True),\
StructField('SF_TXJCD',StringType(),True),\
StructField('DUMMY_EKPO_INCL_EEW_PS',StringType(),True),\
StructField('EXPECTED_VALUE',StringType(),True),\
StructField('LIMIT_AMOUNT',StringType(),True),\
StructField('CONTRACT_FOR_LIMIT',StringType(),True),\
StructField('ENH_DATE1',StringType(),True),\
StructField('ENH_DATE2',StringType(),True),\
StructField('ENH_PERCENT',StringType(),True),\
StructField('ENH_NUMC1',StringType(),True),\
StructField('_DATAAGING',StringType(),True),\
StructField('CUPIT',StringType(),True),\
StructField('CIGIT',StringType(),True),\
StructField('/BEV1/NEGEN_ITEM',StringType(),True),\
StructField('/BEV1/NEDEPFREE',StringType(),True),\
StructField('/BEV1/NESTRUCCAT',StringType(),True),\
StructField('ADVCODE',StringType(),True),\
StructField('BUDGET_PD',StringType(),True),\
StructField('EXCPE',StringType(),True),\
StructField('FMFGUS_KEY',StringType(),True),\
StructField('IUID_RELEVANT',StringType(),True),\
StructField('MRPIND',StringType(),True),\
StructField('SGT_SCAT',StringType(),True),\
StructField('SGT_RCAT',StringType(),True),\
StructField('TMS_REF_UUID',StringType(),True),\
StructField('TMS_SRC_LOC_KEY',StringType(),True),\
StructField('TMS_DES_LOC_KEY',StringType(),True),\
StructField('WRF_CHARSTC1',StringType(),True),\
StructField('WRF_CHARSTC2',StringType(),True),\
StructField('WRF_CHARSTC3',StringType(),True),\
StructField('ZZSSTHT',StringType(),True),\
StructField('ZZRMANUM',StringType(),True),\
StructField('ZZVBELN',StringType(),True),\
StructField('ZZAUFNR',StringType(),True),\
StructField('ZZQMNUM',StringType(),True),\
StructField('ZZPPV',StringType(),True),\
StructField('ZZREAS_CODE',StringType(),True),\
StructField('ZZREQHOT',StringType(),True),\
StructField('ZZHOTQTY',StringType(),True),\
StructField('ZZPULLINDT',StringType(),True),\
StructField('ZZCURRAGREV',StringType(),True),\
StructField('ZZCURRSAPREV',StringType(),True),\
StructField('ZZPOREV',StringType(),True),\
StructField('ZZPQV',StringType(),True),\
StructField('ZZEBELP',StringType(),True),\
StructField('ZZMENGE',DoubleType(),True),\
StructField('ZZAUFNR_OVS',StringType(),True),\
StructField('ZZPLNBEZ',StringType(),True),\
StructField('ZZVORNR',StringType(),True),\
StructField('ZZAGILE_REV',StringType(),True),\
StructField('ZZVBELN_TOLL',StringType(),True),\
StructField('ZZPOSNR_TOLL',StringType(),True),\
StructField('ZZCORDER',StringType(),True),\
StructField('ZZCOITEM',StringType(),True),\
StructField('ZZMPN',StringType(),True),\
StructField('ZZMFNAM',StringType(),True),\
StructField('ZZPPV_TOLE',StringType(),True),\
StructField('REFSITE',StringType(),True),\
StructField('ZAPCGK',StringType(),True),\
StructField('APCGK_EXTEND',StringType(),True),\
StructField('ZBAS_DATE',StringType(),True),\
StructField('ZADATTYP',StringType(),True),\
StructField('ZSTART_DAT',StringType(),True),\
StructField('Z_DEV',StringType(),True),\
StructField('ZINDANX',StringType(),True),\
StructField('ZLIMIT_DAT',StringType(),True),\
StructField('NUMERATOR',StringType(),True),\
StructField('HASHCAL_BDAT',StringType(),True),\
StructField('HASHCAL',StringType(),True),\
StructField('NEGATIVE',StringType(),True),\
StructField('HASHCAL_EXISTS',StringType(),True),\
StructField('KNOWN_INDEX',StringType(),True),\
StructField('/SAPMP/GPOSE',StringType(),True),\
StructField('ANGPN',StringType(),True),\
StructField('ADMOI',StringType(),True),\
StructField('ADPRI',StringType(),True),\
StructField('LPRIO',StringType(),True),\
StructField('ADACN',StringType(),True),\
StructField('AFPNR',StringType(),True),\
StructField('BSARK',StringType(),True),\
StructField('AUDAT',StringType(),True),\
StructField('ANGNR',StringType(),True),\
StructField('PNSTAT',StringType(),True),\
StructField('ADDNS',StringType(),True),\
StructField('ASSIGNMENT_PRIORITY',StringType(),True),\
StructField('ARUN_GROUP_PRIO',StringType(),True),\
StructField('ARUN_ORDER_PRIO',StringType(),True),\
StructField('SERRU',StringType(),True),\
StructField('SERNP',StringType(),True),\
StructField('DISUB_SOBKZ',StringType(),True),\
StructField('DISUB_PSPNR',StringType(),True),\
StructField('DISUB_KUNNR',StringType(),True),\
StructField('DISUB_VBELN',StringType(),True),\
StructField('DISUB_POSNR',StringType(),True),\
StructField('DISUB_OWNER',StringType(),True),\
StructField('FSH_SEASON_YEAR',StringType(),True),\
StructField('FSH_SEASON',StringType(),True),\
StructField('FSH_COLLECTION',StringType(),True),\
StructField('FSH_THEME',StringType(),True),\
StructField('FSH_ATP_DATE',StringType(),True),\
StructField('FSH_VAS_REL',StringType(),True),\
StructField('FSH_VAS_PRNT_ID',StringType(),True),\
StructField('FSH_TRANSACTION',StringType(),True),\
StructField('FSH_ITEM_GROUP',StringType(),True),\
StructField('FSH_ITEM',StringType(),True),\
StructField('FSH_SS',StringType(),True),\
StructField('FSH_GRID_COND_REC',StringType(),True),\
StructField('FSH_PSM_PFM_SPLIT',StringType(),True),\
StructField('CNFM_QTY',StringType(),True),\
StructField('FSH_PQR_UEPOS',StringType(),True),\
StructField('RFM_DIVERSION',StringType(),True),\
StructField('RFM_SCC_INDICATOR',StringType(),True),\
StructField('STPAC',StringType(),True),\
StructField('LGBZO',StringType(),True),\
StructField('LGBZO_B',StringType(),True),\
StructField('ADDRNUM',StringType(),True),\
StructField('CONSNUM',StringType(),True),\
StructField('BORGR_MISS',StringType(),True),\
StructField('DEP_ID',StringType(),True),\
StructField('BELNR',StringType(),True),\
StructField('KBLPOS_CAB',StringType(),True),\
StructField('KBLNR_COMP',StringType(),True),\
StructField('KBLPOS_COMP',StringType(),True),\
StructField('WBS_ELEMENT',StringType(),True),\
StructField('RFM_PSST_RULE',StringType(),True),\
StructField('RFM_PSST_GROUP',StringType(),True),\
StructField('RFM_REF_DOC',StringType(),True),\
StructField('RFM_REF_ITEM',StringType(),True),\
StructField('RFM_REF_ACTION',StringType(),True),\
StructField('RFM_REF_SLITEM',StringType(),True),\
StructField('REF_ITEM',StringType(),True),\
StructField('SOURCE_ID',StringType(),True),\
StructField('SOURCE_KEY',StringType(),True),\
StructField('PUT_BACK',StringType(),True),\
StructField('POL_ID',StringType(),True),\
StructField('CONS_ORDER',StringType(),True),\
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

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('S42'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
                            .withColumn("AEDAT", to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
                            .withColumn("AGDAT", to_date(regexp_replace(df_add_column.AGDAT,'\.','-'))) \
                            .withColumn("TXDAT_FROM", to_date(regexp_replace(df_add_column.TXDAT_FROM,'\.','-'))) \
                            .withColumn("TXDAT", to_date(regexp_replace(df_add_column.TXDAT,'\.','-'))) \
                            .withColumn("ABDAT", to_date(regexp_replace(df_add_column.ABDAT,'\.','-'))) \
                            .withColumn("PRDAT", to_date(regexp_replace(df_add_column.PRDAT,'\.','-'))) \
                            .withColumn("LEWED", regexp_replace(df_add_column.LEWED,'-','')) \
                            .withColumn("EILDT", to_date(regexp_replace(df_add_column.EILDT,'\.','-'))) \
                            .withColumn("DRDAT", to_date(regexp_replace(df_add_column.DRDAT,'\.','-'))) \
                            .withColumn("NLABD", to_date(regexp_replace(df_add_column.NLABD,'\.','-'))) \
                            .withColumn("NFABD", to_date(regexp_replace(df_add_column.NFABD,'\.','-'))) \
                            .withColumn("J_1AIDATEP", to_date(regexp_replace(df_add_column.J_1AIDATEP,'\.','-'))) \
                            .withColumn("CREATIONDATE", to_date(regexp_replace(df_add_column.CREATIONDATE,'\.','-'))) \
                            .withColumn("DPDAT", to_date(regexp_replace(df_add_column.DPDAT,'\.','-'))) \
                            .withColumn("ENH_DATE1", to_date(regexp_replace(df_add_column.ENH_DATE1,'\.','-'))) \
                            .withColumn("ENH_DATE2", to_date(regexp_replace(df_add_column.ENH_DATE2,'\.','-'))) \
                            .withColumn("_DATAAGING", to_date(regexp_replace("_DATAAGING",'\.','-'))) \
                            .withColumn("ZZPULLINDT", to_date(regexp_replace(df_add_column.ZZPULLINDT,'\.','-'))) \
                            .withColumn("ZBAS_DATE", to_date(regexp_replace(df_add_column.ZBAS_DATE,'\.','-'))) \
                            .withColumn("ZSTART_DAT", to_date(regexp_replace(df_add_column.ZSTART_DAT,'\.','-'))) \
                            .withColumn("ZLIMIT_DAT", to_date(regexp_replace(df_add_column.ZLIMIT_DAT,'\.','-'))) \
                            .withColumn("HASHCAL_BDAT", to_date(regexp_replace(df_add_column.HASHCAL_BDAT,'\.','-'))) \
                            .withColumn("AUDAT", to_date(regexp_replace(df_add_column.AUDAT,'\.','-'))) \
                            .withColumn("FSH_ATP_DATE", to_date(regexp_replace(df_add_column.FSH_ATP_DATE,'\.','-'))) \
                            .withColumn("DRUHR", to_timestamp(regexp_replace(df_add_column.DRUHR,'\.','-'))) \
                            .withColumn("CREATIONTIME", to_timestamp(regexp_replace(df_add_column.CREATIONTIME,'\.','-'))) \
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
# MAGIC MERGE INTO S42.EKPO as T
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,EBELN,EBELP ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_EKPO)A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and
# MAGIC T.EBELN = S.EBELN and
# MAGIC T.EBELP = S.EBELP 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`MANDT` =  S.`MANDT`,
# MAGIC T.`EBELN` =  S.`EBELN`,
# MAGIC T.`EBELP` =  S.`EBELP`,
# MAGIC T.`UNIQUEID` =  S.`UNIQUEID`,
# MAGIC T.`LOEKZ` =  S.`LOEKZ`,
# MAGIC T.`STATU` =  S.`STATU`,
# MAGIC T.`AEDAT` =  S.`AEDAT`,
# MAGIC T.`TXZ01` =  S.`TXZ01`,
# MAGIC T.`MATNR` =  S.`MATNR`,
# MAGIC T.`EMATN` =  S.`EMATN`,
# MAGIC T.`BUKRS` =  S.`BUKRS`,
# MAGIC T.`WERKS` =  S.`WERKS`,
# MAGIC T.`LGORT` =  S.`LGORT`,
# MAGIC T.`BEDNR` =  S.`BEDNR`,
# MAGIC T.`MATKL` =  S.`MATKL`,
# MAGIC T.`INFNR` =  S.`INFNR`,
# MAGIC T.`IDNLF` =  S.`IDNLF`,
# MAGIC T.`KTMNG` =  S.`KTMNG`,
# MAGIC T.`MENGE` =  S.`MENGE`,
# MAGIC T.`MEINS` =  S.`MEINS`,
# MAGIC T.`BPRME` =  S.`BPRME`,
# MAGIC T.`BPUMZ` =  S.`BPUMZ`,
# MAGIC T.`BPUMN` =  S.`BPUMN`,
# MAGIC T.`UMREZ` =  S.`UMREZ`,
# MAGIC T.`UMREN` =  S.`UMREN`,
# MAGIC T.`NETPR` =  S.`NETPR`,
# MAGIC T.`PEINH` =  S.`PEINH`,
# MAGIC T.`NETWR` =  S.`NETWR`,
# MAGIC T.`BRTWR` =  S.`BRTWR`,
# MAGIC T.`AGDAT` =  S.`AGDAT`,
# MAGIC T.`WEBAZ` =  S.`WEBAZ`,
# MAGIC T.`MWSKZ` =  S.`MWSKZ`,
# MAGIC T.`TXDAT_FROM` =  S.`TXDAT_FROM`,
# MAGIC T.`TXDAT` =  S.`TXDAT`,
# MAGIC T.`TAX_COUNTRY` =  S.`TAX_COUNTRY`,
# MAGIC T.`BONUS` =  S.`BONUS`,
# MAGIC T.`INSMK` =  S.`INSMK`,
# MAGIC T.`SPINF` =  S.`SPINF`,
# MAGIC T.`PRSDR` =  S.`PRSDR`,
# MAGIC T.`SCHPR` =  S.`SCHPR`,
# MAGIC T.`MAHNZ` =  S.`MAHNZ`,
# MAGIC T.`MAHN1` =  S.`MAHN1`,
# MAGIC T.`MAHN2` =  S.`MAHN2`,
# MAGIC T.`MAHN3` =  S.`MAHN3`,
# MAGIC T.`UEBTO` =  S.`UEBTO`,
# MAGIC T.`UEBTK` =  S.`UEBTK`,
# MAGIC T.`UNTTO` =  S.`UNTTO`,
# MAGIC T.`BWTAR` =  S.`BWTAR`,
# MAGIC T.`BWTTY` =  S.`BWTTY`,
# MAGIC T.`ABSKZ` =  S.`ABSKZ`,
# MAGIC T.`AGMEM` =  S.`AGMEM`,
# MAGIC T.`ELIKZ` =  S.`ELIKZ`,
# MAGIC T.`EREKZ` =  S.`EREKZ`,
# MAGIC T.`PSTYP` =  S.`PSTYP`,
# MAGIC T.`KNTTP` =  S.`KNTTP`,
# MAGIC T.`KZVBR` =  S.`KZVBR`,
# MAGIC T.`VRTKZ` =  S.`VRTKZ`,
# MAGIC T.`TWRKZ` =  S.`TWRKZ`,
# MAGIC T.`WEPOS` =  S.`WEPOS`,
# MAGIC T.`WEUNB` =  S.`WEUNB`,
# MAGIC T.`REPOS` =  S.`REPOS`,
# MAGIC T.`WEBRE` =  S.`WEBRE`,
# MAGIC T.`KZABS` =  S.`KZABS`,
# MAGIC T.`LABNR` =  S.`LABNR`,
# MAGIC T.`KONNR` =  S.`KONNR`,
# MAGIC T.`KTPNR` =  S.`KTPNR`,
# MAGIC T.`ABDAT` =  S.`ABDAT`,
# MAGIC T.`ABFTZ` =  S.`ABFTZ`,
# MAGIC T.`ETFZ1` =  S.`ETFZ1`,
# MAGIC T.`ETFZ2` =  S.`ETFZ2`,
# MAGIC T.`KZSTU` =  S.`KZSTU`,
# MAGIC T.`NOTKZ` =  S.`NOTKZ`,
# MAGIC T.`LMEIN` =  S.`LMEIN`,
# MAGIC T.`EVERS` =  S.`EVERS`,
# MAGIC T.`ZWERT` =  S.`ZWERT`,
# MAGIC T.`NAVNW` =  S.`NAVNW`,
# MAGIC T.`ABMNG` =  S.`ABMNG`,
# MAGIC T.`PRDAT` =  S.`PRDAT`,
# MAGIC T.`BSTYP` =  S.`BSTYP`,
# MAGIC T.`EFFWR` =  S.`EFFWR`,
# MAGIC T.`XOBLR` =  S.`XOBLR`,
# MAGIC T.`KUNNR` =  S.`KUNNR`,
# MAGIC T.`ADRNR` =  S.`ADRNR`,
# MAGIC T.`EKKOL` =  S.`EKKOL`,
# MAGIC T.`SKTOF` =  S.`SKTOF`,
# MAGIC T.`STAFO` =  S.`STAFO`,
# MAGIC T.`PLIFZ` =  S.`PLIFZ`,
# MAGIC T.`NTGEW` =  S.`NTGEW`,
# MAGIC T.`GEWEI` =  S.`GEWEI`,
# MAGIC T.`TXJCD` =  S.`TXJCD`,
# MAGIC T.`ETDRK` =  S.`ETDRK`,
# MAGIC T.`SOBKZ` =  S.`SOBKZ`,
# MAGIC T.`ARSNR` =  S.`ARSNR`,
# MAGIC T.`ARSPS` =  S.`ARSPS`,
# MAGIC T.`INSNC` =  S.`INSNC`,
# MAGIC T.`SSQSS` =  S.`SSQSS`,
# MAGIC T.`ZGTYP` =  S.`ZGTYP`,
# MAGIC T.`EAN11` =  S.`EAN11`,
# MAGIC T.`BSTAE` =  S.`BSTAE`,
# MAGIC T.`REVLV` =  S.`REVLV`,
# MAGIC T.`GEBER` =  S.`GEBER`,
# MAGIC T.`FISTL` =  S.`FISTL`,
# MAGIC T.`FIPOS` =  S.`FIPOS`,
# MAGIC T.`KO_GSBER` =  S.`KO_GSBER`,
# MAGIC T.`KO_PARGB` =  S.`KO_PARGB`,
# MAGIC T.`KO_PRCTR` =  S.`KO_PRCTR`,
# MAGIC T.`KO_PPRCTR` =  S.`KO_PPRCTR`,
# MAGIC T.`MEPRF` =  S.`MEPRF`,
# MAGIC T.`BRGEW` =  S.`BRGEW`,
# MAGIC T.`VOLUM` =  S.`VOLUM`,
# MAGIC T.`VOLEH` =  S.`VOLEH`,
# MAGIC T.`INCO1` =  S.`INCO1`,
# MAGIC T.`INCO2` =  S.`INCO2`,
# MAGIC T.`VORAB` =  S.`VORAB`,
# MAGIC T.`KOLIF` =  S.`KOLIF`,
# MAGIC T.`LTSNR` =  S.`LTSNR`,
# MAGIC T.`PACKNO` =  S.`PACKNO`,
# MAGIC T.`FPLNR` =  S.`FPLNR`,
# MAGIC T.`GNETWR` =  S.`GNETWR`,
# MAGIC T.`STAPO` =  S.`STAPO`,
# MAGIC T.`UEBPO` =  S.`UEBPO`,
# MAGIC T.`LEWED` =  S.`LEWED`,
# MAGIC T.`EMLIF` =  S.`EMLIF`,
# MAGIC T.`LBLKZ` =  S.`LBLKZ`,
# MAGIC T.`SATNR` =  S.`SATNR`,
# MAGIC T.`ATTYP` =  S.`ATTYP`,
# MAGIC T.`VSART` =  S.`VSART`,
# MAGIC T.`HANDOVERLOC` =  S.`HANDOVERLOC`,
# MAGIC T.`KANBA` =  S.`KANBA`,
# MAGIC T.`ADRN2` =  S.`ADRN2`,
# MAGIC T.`CUOBJ` =  S.`CUOBJ`,
# MAGIC T.`XERSY` =  S.`XERSY`,
# MAGIC T.`EILDT` =  S.`EILDT`,
# MAGIC T.`DRDAT` =  S.`DRDAT`,
# MAGIC T.`DRUHR` =  S.`DRUHR`,
# MAGIC T.`DRUNR` =  S.`DRUNR`,
# MAGIC T.`AKTNR` =  S.`AKTNR`,
# MAGIC T.`ABELN` =  S.`ABELN`,
# MAGIC T.`ABELP` =  S.`ABELP`,
# MAGIC T.`ANZPU` =  S.`ANZPU`,
# MAGIC T.`PUNEI` =  S.`PUNEI`,
# MAGIC T.`SAISO` =  S.`SAISO`,
# MAGIC T.`SAISJ` =  S.`SAISJ`,
# MAGIC T.`EBON2` =  S.`EBON2`,
# MAGIC T.`EBON3` =  S.`EBON3`,
# MAGIC T.`EBONF` =  S.`EBONF`,
# MAGIC T.`MLMAA` =  S.`MLMAA`,
# MAGIC T.`MHDRZ` =  S.`MHDRZ`,
# MAGIC T.`ANFNR` =  S.`ANFNR`,
# MAGIC T.`ANFPS` =  S.`ANFPS`,
# MAGIC T.`KZKFG` =  S.`KZKFG`,
# MAGIC T.`USEQU` =  S.`USEQU`,
# MAGIC T.`UMSOK` =  S.`UMSOK`,
# MAGIC T.`BANFN` =  S.`BANFN`,
# MAGIC T.`BNFPO` =  S.`BNFPO`,
# MAGIC T.`MTART` =  S.`MTART`,
# MAGIC T.`UPTYP` =  S.`UPTYP`,
# MAGIC T.`UPVOR` =  S.`UPVOR`,
# MAGIC T.`KZWI1` =  S.`KZWI1`,
# MAGIC T.`KZWI2` =  S.`KZWI2`,
# MAGIC T.`KZWI3` =  S.`KZWI3`,
# MAGIC T.`KZWI4` =  S.`KZWI4`,
# MAGIC T.`KZWI5` =  S.`KZWI5`,
# MAGIC T.`KZWI6` =  S.`KZWI6`,
# MAGIC T.`SIKGR` =  S.`SIKGR`,
# MAGIC T.`MFZHI` =  S.`MFZHI`,
# MAGIC T.`FFZHI` =  S.`FFZHI`,
# MAGIC T.`RETPO` =  S.`RETPO`,
# MAGIC T.`AUREL` =  S.`AUREL`,
# MAGIC T.`BSGRU` =  S.`BSGRU`,
# MAGIC T.`LFRET` =  S.`LFRET`,
# MAGIC T.`MFRGR` =  S.`MFRGR`,
# MAGIC T.`NRFHG` =  S.`NRFHG`,
# MAGIC T.`J_1BNBM` =  S.`J_1BNBM`,
# MAGIC T.`J_1BMATUSE` =  S.`J_1BMATUSE`,
# MAGIC T.`J_1BMATORG` =  S.`J_1BMATORG`,
# MAGIC T.`J_1BOWNPRO` =  S.`J_1BOWNPRO`,
# MAGIC T.`J_1BINDUST` =  S.`J_1BINDUST`,
# MAGIC T.`ABUEB` =  S.`ABUEB`,
# MAGIC T.`NLABD` =  S.`NLABD`,
# MAGIC T.`NFABD` =  S.`NFABD`,
# MAGIC T.`KZBWS` =  S.`KZBWS`,
# MAGIC T.`BONBA` =  S.`BONBA`,
# MAGIC T.`FABKZ` =  S.`FABKZ`,
# MAGIC T.`LOADINGPOINT` =  S.`LOADINGPOINT`,
# MAGIC T.`J_1AINDXP` =  S.`J_1AINDXP`,
# MAGIC T.`J_1AIDATEP` =  S.`J_1AIDATEP`,
# MAGIC T.`MPROF` =  S.`MPROF`,
# MAGIC T.`EGLKZ` =  S.`EGLKZ`,
# MAGIC T.`KZTLF` =  S.`KZTLF`,
# MAGIC T.`KZFME` =  S.`KZFME`,
# MAGIC T.`RDPRF` =  S.`RDPRF`,
# MAGIC T.`TECHS` =  S.`TECHS`,
# MAGIC T.`CHG_SRV` =  S.`CHG_SRV`,
# MAGIC T.`CHG_FPLNR` =  S.`CHG_FPLNR`,
# MAGIC T.`MFRPN` =  S.`MFRPN`,
# MAGIC T.`MFRNR` =  S.`MFRNR`,
# MAGIC T.`EMNFR` =  S.`EMNFR`,
# MAGIC T.`NOVET` =  S.`NOVET`,
# MAGIC T.`AFNAM` =  S.`AFNAM`,
# MAGIC T.`TZONRC` =  S.`TZONRC`,
# MAGIC T.`IPRKZ` =  S.`IPRKZ`,
# MAGIC T.`LEBRE` =  S.`LEBRE`,
# MAGIC T.`BERID` =  S.`BERID`,
# MAGIC T.`XCONDITIONS` =  S.`XCONDITIONS`,
# MAGIC T.`APOMS` =  S.`APOMS`,
# MAGIC T.`CCOMP` =  S.`CCOMP`,
# MAGIC T.`GRANT_NBR` =  S.`GRANT_NBR`,
# MAGIC T.`FKBER` =  S.`FKBER`,
# MAGIC T.`STATUS` =  S.`STATUS`,
# MAGIC T.`RESLO` =  S.`RESLO`,
# MAGIC T.`KBLNR` =  S.`KBLNR`,
# MAGIC T.`KBLPOS` =  S.`KBLPOS`,
# MAGIC T.`PS_PSP_PNR` =  S.`PS_PSP_PNR`,
# MAGIC T.`KOSTL` =  S.`KOSTL`,
# MAGIC T.`SAKTO` =  S.`SAKTO`,
# MAGIC T.`WEORA` =  S.`WEORA`,
# MAGIC T.`SRV_BAS_COM` =  S.`SRV_BAS_COM`,
# MAGIC T.`PRIO_URG` =  S.`PRIO_URG`,
# MAGIC T.`PRIO_REQ` =  S.`PRIO_REQ`,
# MAGIC T.`EMPST` =  S.`EMPST`,
# MAGIC T.`DIFF_INVOICE` =  S.`DIFF_INVOICE`,
# MAGIC T.`TRMRISK_RELEVANT` =  S.`TRMRISK_RELEVANT`,
# MAGIC T.`CREATIONDATE` =  S.`CREATIONDATE`,
# MAGIC T.`CREATIONTIME` =  S.`CREATIONTIME`,
# MAGIC T.`SPE_ABGRU` =  S.`SPE_ABGRU`,
# MAGIC T.`SPE_CRM_SO` =  S.`SPE_CRM_SO`,
# MAGIC T.`SPE_CRM_SO_ITEM` =  S.`SPE_CRM_SO_ITEM`,
# MAGIC T.`SPE_CRM_REF_SO` =  S.`SPE_CRM_REF_SO`,
# MAGIC T.`SPE_CRM_REF_ITEM` =  S.`SPE_CRM_REF_ITEM`,
# MAGIC T.`SPE_CRM_FKREL` =  S.`SPE_CRM_FKREL`,
# MAGIC T.`SPE_CHNG_SYS` =  S.`SPE_CHNG_SYS`,
# MAGIC T.`SPE_INSMK_SRC` =  S.`SPE_INSMK_SRC`,
# MAGIC T.`SPE_CQ_CTRLTYPE` =  S.`SPE_CQ_CTRLTYPE`,
# MAGIC T.`SPE_CQ_NOCQ` =  S.`SPE_CQ_NOCQ`,
# MAGIC T.`REASON_CODE` =  S.`REASON_CODE`,
# MAGIC T.`CQU_SAR` =  S.`CQU_SAR`,
# MAGIC T.`ANZSN` =  S.`ANZSN`,
# MAGIC T.`SPE_EWM_DTC` =  S.`SPE_EWM_DTC`,
# MAGIC T.`EXLIN` =  S.`EXLIN`,
# MAGIC T.`EXSNR` =  S.`EXSNR`,
# MAGIC T.`EHTYP` =  S.`EHTYP`,
# MAGIC T.`RETPC` =  S.`RETPC`,
# MAGIC T.`DPTYP` =  S.`DPTYP`,
# MAGIC T.`DPPCT` =  S.`DPPCT`,
# MAGIC T.`DPAMT` =  S.`DPAMT`,
# MAGIC T.`DPDAT` =  S.`DPDAT`,
# MAGIC T.`FLS_RSTO` =  S.`FLS_RSTO`,
# MAGIC T.`EXT_RFX_NUMBER` =  S.`EXT_RFX_NUMBER`,
# MAGIC T.`EXT_RFX_ITEM` =  S.`EXT_RFX_ITEM`,
# MAGIC T.`EXT_RFX_SYSTEM` =  S.`EXT_RFX_SYSTEM`,
# MAGIC T.`SRM_CONTRACT_ID` =  S.`SRM_CONTRACT_ID`,
# MAGIC T.`SRM_CONTRACT_ITM` =  S.`SRM_CONTRACT_ITM`,
# MAGIC T.`BLK_REASON_ID` =  S.`BLK_REASON_ID`,
# MAGIC T.`BLK_REASON_TXT` =  S.`BLK_REASON_TXT`,
# MAGIC T.`ITCONS` =  S.`ITCONS`,
# MAGIC T.`FIXMG` =  S.`FIXMG`,
# MAGIC T.`WABWE` =  S.`WABWE`,
# MAGIC T.`CMPL_DLV_ITM` =  S.`CMPL_DLV_ITM`,
# MAGIC T.`INCO2_L` =  S.`INCO2_L`,
# MAGIC T.`INCO3_L` =  S.`INCO3_L`,
# MAGIC T.`STAWN` =  S.`STAWN`,
# MAGIC T.`ISVCO` =  S.`ISVCO`,
# MAGIC T.`GRWRT` =  S.`GRWRT`,
# MAGIC T.`SERVICEPERFORMER` =  S.`SERVICEPERFORMER`,
# MAGIC T.`PRODUCTTYPE` =  S.`PRODUCTTYPE`,
# MAGIC T.`GR_BY_SES` =  S.`GR_BY_SES`,
# MAGIC T.`REQUESTFORQUOTATION` =  S.`REQUESTFORQUOTATION`,
# MAGIC T.`REQUESTFORQUOTATIONITEM` =  S.`REQUESTFORQUOTATIONITEM`,
# MAGIC T.`EXTMATERIALFORPURG` =  S.`EXTMATERIALFORPURG`,
# MAGIC T.`TARGET_VALUE` =  S.`TARGET_VALUE`,
# MAGIC T.`EXTERNALREFERENCEID` =  S.`EXTERNALREFERENCEID`,
# MAGIC T.`TC_AUT_DET` =  S.`TC_AUT_DET`,
# MAGIC T.`MANUAL_TC_REASON` =  S.`MANUAL_TC_REASON`,
# MAGIC T.`FISCAL_INCENTIVE` =  S.`FISCAL_INCENTIVE`,
# MAGIC T.`TAX_SUBJECT_ST` =  S.`TAX_SUBJECT_ST`,
# MAGIC T.`FISCAL_INCENTIVE_ID` =  S.`FISCAL_INCENTIVE_ID`,
# MAGIC T.`SF_TXJCD` =  S.`SF_TXJCD`,
# MAGIC T.`DUMMY_EKPO_INCL_EEW_PS` =  S.`DUMMY_EKPO_INCL_EEW_PS`,
# MAGIC T.`EXPECTED_VALUE` =  S.`EXPECTED_VALUE`,
# MAGIC T.`LIMIT_AMOUNT` =  S.`LIMIT_AMOUNT`,
# MAGIC T.`CONTRACT_FOR_LIMIT` =  S.`CONTRACT_FOR_LIMIT`,
# MAGIC T.`ENH_DATE1` =  S.`ENH_DATE1`,
# MAGIC T.`ENH_DATE2` =  S.`ENH_DATE2`,
# MAGIC T.`ENH_PERCENT` =  S.`ENH_PERCENT`,
# MAGIC T.`ENH_NUMC1` =  S.`ENH_NUMC1`,
# MAGIC T.`_DATAAGING` =  S.`_DATAAGING`,
# MAGIC T.`CUPIT` =  S.`CUPIT`,
# MAGIC T.`CIGIT` =  S.`CIGIT`,
# MAGIC T.`/BEV1/NEGEN_ITEM` =  S.`/BEV1/NEGEN_ITEM`,
# MAGIC T.`/BEV1/NEDEPFREE` =  S.`/BEV1/NEDEPFREE`,
# MAGIC T.`/BEV1/NESTRUCCAT` =  S.`/BEV1/NESTRUCCAT`,
# MAGIC T.`ADVCODE` =  S.`ADVCODE`,
# MAGIC T.`BUDGET_PD` =  S.`BUDGET_PD`,
# MAGIC T.`EXCPE` =  S.`EXCPE`,
# MAGIC T.`FMFGUS_KEY` =  S.`FMFGUS_KEY`,
# MAGIC T.`IUID_RELEVANT` =  S.`IUID_RELEVANT`,
# MAGIC T.`MRPIND` =  S.`MRPIND`,
# MAGIC T.`SGT_SCAT` =  S.`SGT_SCAT`,
# MAGIC T.`SGT_RCAT` =  S.`SGT_RCAT`,
# MAGIC T.`TMS_REF_UUID` =  S.`TMS_REF_UUID`,
# MAGIC T.`TMS_SRC_LOC_KEY` =  S.`TMS_SRC_LOC_KEY`,
# MAGIC T.`TMS_DES_LOC_KEY` =  S.`TMS_DES_LOC_KEY`,
# MAGIC T.`WRF_CHARSTC1` =  S.`WRF_CHARSTC1`,
# MAGIC T.`WRF_CHARSTC2` =  S.`WRF_CHARSTC2`,
# MAGIC T.`WRF_CHARSTC3` =  S.`WRF_CHARSTC3`,
# MAGIC T.`ZZSSTHT` =  S.`ZZSSTHT`,
# MAGIC T.`ZZRMANUM` =  S.`ZZRMANUM`,
# MAGIC T.`ZZVBELN` =  S.`ZZVBELN`,
# MAGIC T.`ZZAUFNR` =  S.`ZZAUFNR`,
# MAGIC T.`ZZQMNUM` =  S.`ZZQMNUM`,
# MAGIC T.`ZZPPV` =  S.`ZZPPV`,
# MAGIC T.`ZZREAS_CODE` =  S.`ZZREAS_CODE`,
# MAGIC T.`ZZREQHOT` =  S.`ZZREQHOT`,
# MAGIC T.`ZZHOTQTY` =  S.`ZZHOTQTY`,
# MAGIC T.`ZZPULLINDT` =  S.`ZZPULLINDT`,
# MAGIC T.`ZZCURRAGREV` =  S.`ZZCURRAGREV`,
# MAGIC T.`ZZCURRSAPREV` =  S.`ZZCURRSAPREV`,
# MAGIC T.`ZZPOREV` =  S.`ZZPOREV`,
# MAGIC T.`ZZPQV` =  S.`ZZPQV`,
# MAGIC T.`ZZEBELP` =  S.`ZZEBELP`,
# MAGIC T.`ZZMENGE` =  S.`ZZMENGE`,
# MAGIC T.`ZZAUFNR_OVS` =  S.`ZZAUFNR_OVS`,
# MAGIC T.`ZZPLNBEZ` =  S.`ZZPLNBEZ`,
# MAGIC T.`ZZVORNR` =  S.`ZZVORNR`,
# MAGIC T.`ZZAGILE_REV` =  S.`ZZAGILE_REV`,
# MAGIC T.`ZZVBELN_TOLL` =  S.`ZZVBELN_TOLL`,
# MAGIC T.`ZZPOSNR_TOLL` =  S.`ZZPOSNR_TOLL`,
# MAGIC T.`ZZCORDER` =  S.`ZZCORDER`,
# MAGIC T.`ZZCOITEM` =  S.`ZZCOITEM`,
# MAGIC T.`ZZMPN` =  S.`ZZMPN`,
# MAGIC T.`ZZMFNAM` =  S.`ZZMFNAM`,
# MAGIC T.`ZZPPV_TOLE` =  S.`ZZPPV_TOLE`,
# MAGIC T.`REFSITE` =  S.`REFSITE`,
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
# MAGIC T.`/SAPMP/GPOSE` =  S.`/SAPMP/GPOSE`,
# MAGIC T.`ANGPN` =  S.`ANGPN`,
# MAGIC T.`ADMOI` =  S.`ADMOI`,
# MAGIC T.`ADPRI` =  S.`ADPRI`,
# MAGIC T.`LPRIO` =  S.`LPRIO`,
# MAGIC T.`ADACN` =  S.`ADACN`,
# MAGIC T.`AFPNR` =  S.`AFPNR`,
# MAGIC T.`BSARK` =  S.`BSARK`,
# MAGIC T.`AUDAT` =  S.`AUDAT`,
# MAGIC T.`ANGNR` =  S.`ANGNR`,
# MAGIC T.`PNSTAT` =  S.`PNSTAT`,
# MAGIC T.`ADDNS` =  S.`ADDNS`,
# MAGIC T.`ASSIGNMENT_PRIORITY` =  S.`ASSIGNMENT_PRIORITY`,
# MAGIC T.`ARUN_GROUP_PRIO` =  S.`ARUN_GROUP_PRIO`,
# MAGIC T.`ARUN_ORDER_PRIO` =  S.`ARUN_ORDER_PRIO`,
# MAGIC T.`SERRU` =  S.`SERRU`,
# MAGIC T.`SERNP` =  S.`SERNP`,
# MAGIC T.`DISUB_SOBKZ` =  S.`DISUB_SOBKZ`,
# MAGIC T.`DISUB_PSPNR` =  S.`DISUB_PSPNR`,
# MAGIC T.`DISUB_KUNNR` =  S.`DISUB_KUNNR`,
# MAGIC T.`DISUB_VBELN` =  S.`DISUB_VBELN`,
# MAGIC T.`DISUB_POSNR` =  S.`DISUB_POSNR`,
# MAGIC T.`DISUB_OWNER` =  S.`DISUB_OWNER`,
# MAGIC T.`FSH_SEASON_YEAR` =  S.`FSH_SEASON_YEAR`,
# MAGIC T.`FSH_SEASON` =  S.`FSH_SEASON`,
# MAGIC T.`FSH_COLLECTION` =  S.`FSH_COLLECTION`,
# MAGIC T.`FSH_THEME` =  S.`FSH_THEME`,
# MAGIC T.`FSH_ATP_DATE` =  S.`FSH_ATP_DATE`,
# MAGIC T.`FSH_VAS_REL` =  S.`FSH_VAS_REL`,
# MAGIC T.`FSH_VAS_PRNT_ID` =  S.`FSH_VAS_PRNT_ID`,
# MAGIC T.`FSH_TRANSACTION` =  S.`FSH_TRANSACTION`,
# MAGIC T.`FSH_ITEM_GROUP` =  S.`FSH_ITEM_GROUP`,
# MAGIC T.`FSH_ITEM` =  S.`FSH_ITEM`,
# MAGIC T.`FSH_SS` =  S.`FSH_SS`,
# MAGIC T.`FSH_GRID_COND_REC` =  S.`FSH_GRID_COND_REC`,
# MAGIC T.`FSH_PSM_PFM_SPLIT` =  S.`FSH_PSM_PFM_SPLIT`,
# MAGIC T.`CNFM_QTY` =  S.`CNFM_QTY`,
# MAGIC T.`FSH_PQR_UEPOS` =  S.`FSH_PQR_UEPOS`,
# MAGIC T.`RFM_DIVERSION` =  S.`RFM_DIVERSION`,
# MAGIC T.`RFM_SCC_INDICATOR` =  S.`RFM_SCC_INDICATOR`,
# MAGIC T.`STPAC` =  S.`STPAC`,
# MAGIC T.`LGBZO` =  S.`LGBZO`,
# MAGIC T.`LGBZO_B` =  S.`LGBZO_B`,
# MAGIC T.`ADDRNUM` =  S.`ADDRNUM`,
# MAGIC T.`CONSNUM` =  S.`CONSNUM`,
# MAGIC T.`BORGR_MISS` =  S.`BORGR_MISS`,
# MAGIC T.`DEP_ID` =  S.`DEP_ID`,
# MAGIC T.`BELNR` =  S.`BELNR`,
# MAGIC T.`KBLPOS_CAB` =  S.`KBLPOS_CAB`,
# MAGIC T.`KBLNR_COMP` =  S.`KBLNR_COMP`,
# MAGIC T.`KBLPOS_COMP` =  S.`KBLPOS_COMP`,
# MAGIC T.`WBS_ELEMENT` =  S.`WBS_ELEMENT`,
# MAGIC T.`RFM_PSST_RULE` =  S.`RFM_PSST_RULE`,
# MAGIC T.`RFM_PSST_GROUP` =  S.`RFM_PSST_GROUP`,
# MAGIC T.`RFM_REF_DOC` =  S.`RFM_REF_DOC`,
# MAGIC T.`RFM_REF_ITEM` =  S.`RFM_REF_ITEM`,
# MAGIC T.`RFM_REF_ACTION` =  S.`RFM_REF_ACTION`,
# MAGIC T.`RFM_REF_SLITEM` =  S.`RFM_REF_SLITEM`,
# MAGIC T.`REF_ITEM` =  S.`REF_ITEM`,
# MAGIC T.`SOURCE_ID` =  S.`SOURCE_ID`,
# MAGIC T.`SOURCE_KEY` =  S.`SOURCE_KEY`,
# MAGIC T.`PUT_BACK` =  S.`PUT_BACK`,
# MAGIC T.`POL_ID` =  S.`POL_ID`,
# MAGIC T.`CONS_ORDER` =  S.`CONS_ORDER`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `EBELN`,
# MAGIC `EBELP`,
# MAGIC `UNIQUEID`,
# MAGIC `LOEKZ`,
# MAGIC `STATU`,
# MAGIC `AEDAT`,
# MAGIC `TXZ01`,
# MAGIC `MATNR`,
# MAGIC `EMATN`,
# MAGIC `BUKRS`,
# MAGIC `WERKS`,
# MAGIC `LGORT`,
# MAGIC `BEDNR`,
# MAGIC `MATKL`,
# MAGIC `INFNR`,
# MAGIC `IDNLF`,
# MAGIC `KTMNG`,
# MAGIC `MENGE`,
# MAGIC `MEINS`,
# MAGIC `BPRME`,
# MAGIC `BPUMZ`,
# MAGIC `BPUMN`,
# MAGIC `UMREZ`,
# MAGIC `UMREN`,
# MAGIC `NETPR`,
# MAGIC `PEINH`,
# MAGIC `NETWR`,
# MAGIC `BRTWR`,
# MAGIC `AGDAT`,
# MAGIC `WEBAZ`,
# MAGIC `MWSKZ`,
# MAGIC `TXDAT_FROM`,
# MAGIC `TXDAT`,
# MAGIC `TAX_COUNTRY`,
# MAGIC `BONUS`,
# MAGIC `INSMK`,
# MAGIC `SPINF`,
# MAGIC `PRSDR`,
# MAGIC `SCHPR`,
# MAGIC `MAHNZ`,
# MAGIC `MAHN1`,
# MAGIC `MAHN2`,
# MAGIC `MAHN3`,
# MAGIC `UEBTO`,
# MAGIC `UEBTK`,
# MAGIC `UNTTO`,
# MAGIC `BWTAR`,
# MAGIC `BWTTY`,
# MAGIC `ABSKZ`,
# MAGIC `AGMEM`,
# MAGIC `ELIKZ`,
# MAGIC `EREKZ`,
# MAGIC `PSTYP`,
# MAGIC `KNTTP`,
# MAGIC `KZVBR`,
# MAGIC `VRTKZ`,
# MAGIC `TWRKZ`,
# MAGIC `WEPOS`,
# MAGIC `WEUNB`,
# MAGIC `REPOS`,
# MAGIC `WEBRE`,
# MAGIC `KZABS`,
# MAGIC `LABNR`,
# MAGIC `KONNR`,
# MAGIC `KTPNR`,
# MAGIC `ABDAT`,
# MAGIC `ABFTZ`,
# MAGIC `ETFZ1`,
# MAGIC `ETFZ2`,
# MAGIC `KZSTU`,
# MAGIC `NOTKZ`,
# MAGIC `LMEIN`,
# MAGIC `EVERS`,
# MAGIC `ZWERT`,
# MAGIC `NAVNW`,
# MAGIC `ABMNG`,
# MAGIC `PRDAT`,
# MAGIC `BSTYP`,
# MAGIC `EFFWR`,
# MAGIC `XOBLR`,
# MAGIC `KUNNR`,
# MAGIC `ADRNR`,
# MAGIC `EKKOL`,
# MAGIC `SKTOF`,
# MAGIC `STAFO`,
# MAGIC `PLIFZ`,
# MAGIC `NTGEW`,
# MAGIC `GEWEI`,
# MAGIC `TXJCD`,
# MAGIC `ETDRK`,
# MAGIC `SOBKZ`,
# MAGIC `ARSNR`,
# MAGIC `ARSPS`,
# MAGIC `INSNC`,
# MAGIC `SSQSS`,
# MAGIC `ZGTYP`,
# MAGIC `EAN11`,
# MAGIC `BSTAE`,
# MAGIC `REVLV`,
# MAGIC `GEBER`,
# MAGIC `FISTL`,
# MAGIC `FIPOS`,
# MAGIC `KO_GSBER`,
# MAGIC `KO_PARGB`,
# MAGIC `KO_PRCTR`,
# MAGIC `KO_PPRCTR`,
# MAGIC `MEPRF`,
# MAGIC `BRGEW`,
# MAGIC `VOLUM`,
# MAGIC `VOLEH`,
# MAGIC `INCO1`,
# MAGIC `INCO2`,
# MAGIC `VORAB`,
# MAGIC `KOLIF`,
# MAGIC `LTSNR`,
# MAGIC `PACKNO`,
# MAGIC `FPLNR`,
# MAGIC `GNETWR`,
# MAGIC `STAPO`,
# MAGIC `UEBPO`,
# MAGIC `LEWED`,
# MAGIC `EMLIF`,
# MAGIC `LBLKZ`,
# MAGIC `SATNR`,
# MAGIC `ATTYP`,
# MAGIC `VSART`,
# MAGIC `HANDOVERLOC`,
# MAGIC `KANBA`,
# MAGIC `ADRN2`,
# MAGIC `CUOBJ`,
# MAGIC `XERSY`,
# MAGIC `EILDT`,
# MAGIC `DRDAT`,
# MAGIC `DRUHR`,
# MAGIC `DRUNR`,
# MAGIC `AKTNR`,
# MAGIC `ABELN`,
# MAGIC `ABELP`,
# MAGIC `ANZPU`,
# MAGIC `PUNEI`,
# MAGIC `SAISO`,
# MAGIC `SAISJ`,
# MAGIC `EBON2`,
# MAGIC `EBON3`,
# MAGIC `EBONF`,
# MAGIC `MLMAA`,
# MAGIC `MHDRZ`,
# MAGIC `ANFNR`,
# MAGIC `ANFPS`,
# MAGIC `KZKFG`,
# MAGIC `USEQU`,
# MAGIC `UMSOK`,
# MAGIC `BANFN`,
# MAGIC `BNFPO`,
# MAGIC `MTART`,
# MAGIC `UPTYP`,
# MAGIC `UPVOR`,
# MAGIC `KZWI1`,
# MAGIC `KZWI2`,
# MAGIC `KZWI3`,
# MAGIC `KZWI4`,
# MAGIC `KZWI5`,
# MAGIC `KZWI6`,
# MAGIC `SIKGR`,
# MAGIC `MFZHI`,
# MAGIC `FFZHI`,
# MAGIC `RETPO`,
# MAGIC `AUREL`,
# MAGIC `BSGRU`,
# MAGIC `LFRET`,
# MAGIC `MFRGR`,
# MAGIC `NRFHG`,
# MAGIC `J_1BNBM`,
# MAGIC `J_1BMATUSE`,
# MAGIC `J_1BMATORG`,
# MAGIC `J_1BOWNPRO`,
# MAGIC `J_1BINDUST`,
# MAGIC `ABUEB`,
# MAGIC `NLABD`,
# MAGIC `NFABD`,
# MAGIC `KZBWS`,
# MAGIC `BONBA`,
# MAGIC `FABKZ`,
# MAGIC `LOADINGPOINT`,
# MAGIC `J_1AINDXP`,
# MAGIC `J_1AIDATEP`,
# MAGIC `MPROF`,
# MAGIC `EGLKZ`,
# MAGIC `KZTLF`,
# MAGIC `KZFME`,
# MAGIC `RDPRF`,
# MAGIC `TECHS`,
# MAGIC `CHG_SRV`,
# MAGIC `CHG_FPLNR`,
# MAGIC `MFRPN`,
# MAGIC `MFRNR`,
# MAGIC `EMNFR`,
# MAGIC `NOVET`,
# MAGIC `AFNAM`,
# MAGIC `TZONRC`,
# MAGIC `IPRKZ`,
# MAGIC `LEBRE`,
# MAGIC `BERID`,
# MAGIC `XCONDITIONS`,
# MAGIC `APOMS`,
# MAGIC `CCOMP`,
# MAGIC `GRANT_NBR`,
# MAGIC `FKBER`,
# MAGIC `STATUS`,
# MAGIC `RESLO`,
# MAGIC `KBLNR`,
# MAGIC `KBLPOS`,
# MAGIC `PS_PSP_PNR`,
# MAGIC `KOSTL`,
# MAGIC `SAKTO`,
# MAGIC `WEORA`,
# MAGIC `SRV_BAS_COM`,
# MAGIC `PRIO_URG`,
# MAGIC `PRIO_REQ`,
# MAGIC `EMPST`,
# MAGIC `DIFF_INVOICE`,
# MAGIC `TRMRISK_RELEVANT`,
# MAGIC `CREATIONDATE`,
# MAGIC `CREATIONTIME`,
# MAGIC `SPE_ABGRU`,
# MAGIC `SPE_CRM_SO`,
# MAGIC `SPE_CRM_SO_ITEM`,
# MAGIC `SPE_CRM_REF_SO`,
# MAGIC `SPE_CRM_REF_ITEM`,
# MAGIC `SPE_CRM_FKREL`,
# MAGIC `SPE_CHNG_SYS`,
# MAGIC `SPE_INSMK_SRC`,
# MAGIC `SPE_CQ_CTRLTYPE`,
# MAGIC `SPE_CQ_NOCQ`,
# MAGIC `REASON_CODE`,
# MAGIC `CQU_SAR`,
# MAGIC `ANZSN`,
# MAGIC `SPE_EWM_DTC`,
# MAGIC `EXLIN`,
# MAGIC `EXSNR`,
# MAGIC `EHTYP`,
# MAGIC `RETPC`,
# MAGIC `DPTYP`,
# MAGIC `DPPCT`,
# MAGIC `DPAMT`,
# MAGIC `DPDAT`,
# MAGIC `FLS_RSTO`,
# MAGIC `EXT_RFX_NUMBER`,
# MAGIC `EXT_RFX_ITEM`,
# MAGIC `EXT_RFX_SYSTEM`,
# MAGIC `SRM_CONTRACT_ID`,
# MAGIC `SRM_CONTRACT_ITM`,
# MAGIC `BLK_REASON_ID`,
# MAGIC `BLK_REASON_TXT`,
# MAGIC `ITCONS`,
# MAGIC `FIXMG`,
# MAGIC `WABWE`,
# MAGIC `CMPL_DLV_ITM`,
# MAGIC `INCO2_L`,
# MAGIC `INCO3_L`,
# MAGIC `STAWN`,
# MAGIC `ISVCO`,
# MAGIC `GRWRT`,
# MAGIC `SERVICEPERFORMER`,
# MAGIC `PRODUCTTYPE`,
# MAGIC `GR_BY_SES`,
# MAGIC `REQUESTFORQUOTATION`,
# MAGIC `REQUESTFORQUOTATIONITEM`,
# MAGIC `EXTMATERIALFORPURG`,
# MAGIC `TARGET_VALUE`,
# MAGIC `EXTERNALREFERENCEID`,
# MAGIC `TC_AUT_DET`,
# MAGIC `MANUAL_TC_REASON`,
# MAGIC `FISCAL_INCENTIVE`,
# MAGIC `TAX_SUBJECT_ST`,
# MAGIC `FISCAL_INCENTIVE_ID`,
# MAGIC `SF_TXJCD`,
# MAGIC `DUMMY_EKPO_INCL_EEW_PS`,
# MAGIC `EXPECTED_VALUE`,
# MAGIC `LIMIT_AMOUNT`,
# MAGIC `CONTRACT_FOR_LIMIT`,
# MAGIC `ENH_DATE1`,
# MAGIC `ENH_DATE2`,
# MAGIC `ENH_PERCENT`,
# MAGIC `ENH_NUMC1`,
# MAGIC `_DATAAGING`,
# MAGIC `CUPIT`,
# MAGIC `CIGIT`,
# MAGIC `/BEV1/NEGEN_ITEM`,
# MAGIC `/BEV1/NEDEPFREE`,
# MAGIC `/BEV1/NESTRUCCAT`,
# MAGIC `ADVCODE`,
# MAGIC `BUDGET_PD`,
# MAGIC `EXCPE`,
# MAGIC `FMFGUS_KEY`,
# MAGIC `IUID_RELEVANT`,
# MAGIC `MRPIND`,
# MAGIC `SGT_SCAT`,
# MAGIC `SGT_RCAT`,
# MAGIC `TMS_REF_UUID`,
# MAGIC `TMS_SRC_LOC_KEY`,
# MAGIC `TMS_DES_LOC_KEY`,
# MAGIC `WRF_CHARSTC1`,
# MAGIC `WRF_CHARSTC2`,
# MAGIC `WRF_CHARSTC3`,
# MAGIC `ZZSSTHT`,
# MAGIC `ZZRMANUM`,
# MAGIC `ZZVBELN`,
# MAGIC `ZZAUFNR`,
# MAGIC `ZZQMNUM`,
# MAGIC `ZZPPV`,
# MAGIC `ZZREAS_CODE`,
# MAGIC `ZZREQHOT`,
# MAGIC `ZZHOTQTY`,
# MAGIC `ZZPULLINDT`,
# MAGIC `ZZCURRAGREV`,
# MAGIC `ZZCURRSAPREV`,
# MAGIC `ZZPOREV`,
# MAGIC `ZZPQV`,
# MAGIC `ZZEBELP`,
# MAGIC `ZZMENGE`,
# MAGIC `ZZAUFNR_OVS`,
# MAGIC `ZZPLNBEZ`,
# MAGIC `ZZVORNR`,
# MAGIC `ZZAGILE_REV`,
# MAGIC `ZZVBELN_TOLL`,
# MAGIC `ZZPOSNR_TOLL`,
# MAGIC `ZZCORDER`,
# MAGIC `ZZCOITEM`,
# MAGIC `ZZMPN`,
# MAGIC `ZZMFNAM`,
# MAGIC `ZZPPV_TOLE`,
# MAGIC `REFSITE`,
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
# MAGIC `/SAPMP/GPOSE`,
# MAGIC `ANGPN`,
# MAGIC `ADMOI`,
# MAGIC `ADPRI`,
# MAGIC `LPRIO`,
# MAGIC `ADACN`,
# MAGIC `AFPNR`,
# MAGIC `BSARK`,
# MAGIC `AUDAT`,
# MAGIC `ANGNR`,
# MAGIC `PNSTAT`,
# MAGIC `ADDNS`,
# MAGIC `ASSIGNMENT_PRIORITY`,
# MAGIC `ARUN_GROUP_PRIO`,
# MAGIC `ARUN_ORDER_PRIO`,
# MAGIC `SERRU`,
# MAGIC `SERNP`,
# MAGIC `DISUB_SOBKZ`,
# MAGIC `DISUB_PSPNR`,
# MAGIC `DISUB_KUNNR`,
# MAGIC `DISUB_VBELN`,
# MAGIC `DISUB_POSNR`,
# MAGIC `DISUB_OWNER`,
# MAGIC `FSH_SEASON_YEAR`,
# MAGIC `FSH_SEASON`,
# MAGIC `FSH_COLLECTION`,
# MAGIC `FSH_THEME`,
# MAGIC `FSH_ATP_DATE`,
# MAGIC `FSH_VAS_REL`,
# MAGIC `FSH_VAS_PRNT_ID`,
# MAGIC `FSH_TRANSACTION`,
# MAGIC `FSH_ITEM_GROUP`,
# MAGIC `FSH_ITEM`,
# MAGIC `FSH_SS`,
# MAGIC `FSH_GRID_COND_REC`,
# MAGIC `FSH_PSM_PFM_SPLIT`,
# MAGIC `CNFM_QTY`,
# MAGIC `FSH_PQR_UEPOS`,
# MAGIC `RFM_DIVERSION`,
# MAGIC `RFM_SCC_INDICATOR`,
# MAGIC `STPAC`,
# MAGIC `LGBZO`,
# MAGIC `LGBZO_B`,
# MAGIC `ADDRNUM`,
# MAGIC `CONSNUM`,
# MAGIC `BORGR_MISS`,
# MAGIC `DEP_ID`,
# MAGIC `BELNR`,
# MAGIC `KBLPOS_CAB`,
# MAGIC `KBLNR_COMP`,
# MAGIC `KBLPOS_COMP`,
# MAGIC `WBS_ELEMENT`,
# MAGIC `RFM_PSST_RULE`,
# MAGIC `RFM_PSST_GROUP`,
# MAGIC `RFM_REF_DOC`,
# MAGIC `RFM_REF_ITEM`,
# MAGIC `RFM_REF_ACTION`,
# MAGIC `RFM_REF_SLITEM`,
# MAGIC `REF_ITEM`,
# MAGIC `SOURCE_ID`,
# MAGIC `SOURCE_KEY`,
# MAGIC `PUT_BACK`,
# MAGIC `POL_ID`,
# MAGIC `CONS_ORDER`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`MANDT`,
# MAGIC S.`EBELN`,
# MAGIC S.`EBELP`,
# MAGIC S.`UNIQUEID`,
# MAGIC S.`LOEKZ`,
# MAGIC S.`STATU`,
# MAGIC S.`AEDAT`,
# MAGIC S.`TXZ01`,
# MAGIC S.`MATNR`,
# MAGIC S.`EMATN`,
# MAGIC S.`BUKRS`,
# MAGIC S.`WERKS`,
# MAGIC S.`LGORT`,
# MAGIC S.`BEDNR`,
# MAGIC S.`MATKL`,
# MAGIC S.`INFNR`,
# MAGIC S.`IDNLF`,
# MAGIC S.`KTMNG`,
# MAGIC S.`MENGE`,
# MAGIC S.`MEINS`,
# MAGIC S.`BPRME`,
# MAGIC S.`BPUMZ`,
# MAGIC S.`BPUMN`,
# MAGIC S.`UMREZ`,
# MAGIC S.`UMREN`,
# MAGIC S.`NETPR`,
# MAGIC S.`PEINH`,
# MAGIC S.`NETWR`,
# MAGIC S.`BRTWR`,
# MAGIC S.`AGDAT`,
# MAGIC S.`WEBAZ`,
# MAGIC S.`MWSKZ`,
# MAGIC S.`TXDAT_FROM`,
# MAGIC S.`TXDAT`,
# MAGIC S.`TAX_COUNTRY`,
# MAGIC S.`BONUS`,
# MAGIC S.`INSMK`,
# MAGIC S.`SPINF`,
# MAGIC S.`PRSDR`,
# MAGIC S.`SCHPR`,
# MAGIC S.`MAHNZ`,
# MAGIC S.`MAHN1`,
# MAGIC S.`MAHN2`,
# MAGIC S.`MAHN3`,
# MAGIC S.`UEBTO`,
# MAGIC S.`UEBTK`,
# MAGIC S.`UNTTO`,
# MAGIC S.`BWTAR`,
# MAGIC S.`BWTTY`,
# MAGIC S.`ABSKZ`,
# MAGIC S.`AGMEM`,
# MAGIC S.`ELIKZ`,
# MAGIC S.`EREKZ`,
# MAGIC S.`PSTYP`,
# MAGIC S.`KNTTP`,
# MAGIC S.`KZVBR`,
# MAGIC S.`VRTKZ`,
# MAGIC S.`TWRKZ`,
# MAGIC S.`WEPOS`,
# MAGIC S.`WEUNB`,
# MAGIC S.`REPOS`,
# MAGIC S.`WEBRE`,
# MAGIC S.`KZABS`,
# MAGIC S.`LABNR`,
# MAGIC S.`KONNR`,
# MAGIC S.`KTPNR`,
# MAGIC S.`ABDAT`,
# MAGIC S.`ABFTZ`,
# MAGIC S.`ETFZ1`,
# MAGIC S.`ETFZ2`,
# MAGIC S.`KZSTU`,
# MAGIC S.`NOTKZ`,
# MAGIC S.`LMEIN`,
# MAGIC S.`EVERS`,
# MAGIC S.`ZWERT`,
# MAGIC S.`NAVNW`,
# MAGIC S.`ABMNG`,
# MAGIC S.`PRDAT`,
# MAGIC S.`BSTYP`,
# MAGIC S.`EFFWR`,
# MAGIC S.`XOBLR`,
# MAGIC S.`KUNNR`,
# MAGIC S.`ADRNR`,
# MAGIC S.`EKKOL`,
# MAGIC S.`SKTOF`,
# MAGIC S.`STAFO`,
# MAGIC S.`PLIFZ`,
# MAGIC S.`NTGEW`,
# MAGIC S.`GEWEI`,
# MAGIC S.`TXJCD`,
# MAGIC S.`ETDRK`,
# MAGIC S.`SOBKZ`,
# MAGIC S.`ARSNR`,
# MAGIC S.`ARSPS`,
# MAGIC S.`INSNC`,
# MAGIC S.`SSQSS`,
# MAGIC S.`ZGTYP`,
# MAGIC S.`EAN11`,
# MAGIC S.`BSTAE`,
# MAGIC S.`REVLV`,
# MAGIC S.`GEBER`,
# MAGIC S.`FISTL`,
# MAGIC S.`FIPOS`,
# MAGIC S.`KO_GSBER`,
# MAGIC S.`KO_PARGB`,
# MAGIC S.`KO_PRCTR`,
# MAGIC S.`KO_PPRCTR`,
# MAGIC S.`MEPRF`,
# MAGIC S.`BRGEW`,
# MAGIC S.`VOLUM`,
# MAGIC S.`VOLEH`,
# MAGIC S.`INCO1`,
# MAGIC S.`INCO2`,
# MAGIC S.`VORAB`,
# MAGIC S.`KOLIF`,
# MAGIC S.`LTSNR`,
# MAGIC S.`PACKNO`,
# MAGIC S.`FPLNR`,
# MAGIC S.`GNETWR`,
# MAGIC S.`STAPO`,
# MAGIC S.`UEBPO`,
# MAGIC S.`LEWED`,
# MAGIC S.`EMLIF`,
# MAGIC S.`LBLKZ`,
# MAGIC S.`SATNR`,
# MAGIC S.`ATTYP`,
# MAGIC S.`VSART`,
# MAGIC S.`HANDOVERLOC`,
# MAGIC S.`KANBA`,
# MAGIC S.`ADRN2`,
# MAGIC S.`CUOBJ`,
# MAGIC S.`XERSY`,
# MAGIC S.`EILDT`,
# MAGIC S.`DRDAT`,
# MAGIC S.`DRUHR`,
# MAGIC S.`DRUNR`,
# MAGIC S.`AKTNR`,
# MAGIC S.`ABELN`,
# MAGIC S.`ABELP`,
# MAGIC S.`ANZPU`,
# MAGIC S.`PUNEI`,
# MAGIC S.`SAISO`,
# MAGIC S.`SAISJ`,
# MAGIC S.`EBON2`,
# MAGIC S.`EBON3`,
# MAGIC S.`EBONF`,
# MAGIC S.`MLMAA`,
# MAGIC S.`MHDRZ`,
# MAGIC S.`ANFNR`,
# MAGIC S.`ANFPS`,
# MAGIC S.`KZKFG`,
# MAGIC S.`USEQU`,
# MAGIC S.`UMSOK`,
# MAGIC S.`BANFN`,
# MAGIC S.`BNFPO`,
# MAGIC S.`MTART`,
# MAGIC S.`UPTYP`,
# MAGIC S.`UPVOR`,
# MAGIC S.`KZWI1`,
# MAGIC S.`KZWI2`,
# MAGIC S.`KZWI3`,
# MAGIC S.`KZWI4`,
# MAGIC S.`KZWI5`,
# MAGIC S.`KZWI6`,
# MAGIC S.`SIKGR`,
# MAGIC S.`MFZHI`,
# MAGIC S.`FFZHI`,
# MAGIC S.`RETPO`,
# MAGIC S.`AUREL`,
# MAGIC S.`BSGRU`,
# MAGIC S.`LFRET`,
# MAGIC S.`MFRGR`,
# MAGIC S.`NRFHG`,
# MAGIC S.`J_1BNBM`,
# MAGIC S.`J_1BMATUSE`,
# MAGIC S.`J_1BMATORG`,
# MAGIC S.`J_1BOWNPRO`,
# MAGIC S.`J_1BINDUST`,
# MAGIC S.`ABUEB`,
# MAGIC S.`NLABD`,
# MAGIC S.`NFABD`,
# MAGIC S.`KZBWS`,
# MAGIC S.`BONBA`,
# MAGIC S.`FABKZ`,
# MAGIC S.`LOADINGPOINT`,
# MAGIC S.`J_1AINDXP`,
# MAGIC S.`J_1AIDATEP`,
# MAGIC S.`MPROF`,
# MAGIC S.`EGLKZ`,
# MAGIC S.`KZTLF`,
# MAGIC S.`KZFME`,
# MAGIC S.`RDPRF`,
# MAGIC S.`TECHS`,
# MAGIC S.`CHG_SRV`,
# MAGIC S.`CHG_FPLNR`,
# MAGIC S.`MFRPN`,
# MAGIC S.`MFRNR`,
# MAGIC S.`EMNFR`,
# MAGIC S.`NOVET`,
# MAGIC S.`AFNAM`,
# MAGIC S.`TZONRC`,
# MAGIC S.`IPRKZ`,
# MAGIC S.`LEBRE`,
# MAGIC S.`BERID`,
# MAGIC S.`XCONDITIONS`,
# MAGIC S.`APOMS`,
# MAGIC S.`CCOMP`,
# MAGIC S.`GRANT_NBR`,
# MAGIC S.`FKBER`,
# MAGIC S.`STATUS`,
# MAGIC S.`RESLO`,
# MAGIC S.`KBLNR`,
# MAGIC S.`KBLPOS`,
# MAGIC S.`PS_PSP_PNR`,
# MAGIC S.`KOSTL`,
# MAGIC S.`SAKTO`,
# MAGIC S.`WEORA`,
# MAGIC S.`SRV_BAS_COM`,
# MAGIC S.`PRIO_URG`,
# MAGIC S.`PRIO_REQ`,
# MAGIC S.`EMPST`,
# MAGIC S.`DIFF_INVOICE`,
# MAGIC S.`TRMRISK_RELEVANT`,
# MAGIC S.`CREATIONDATE`,
# MAGIC S.`CREATIONTIME`,
# MAGIC S.`SPE_ABGRU`,
# MAGIC S.`SPE_CRM_SO`,
# MAGIC S.`SPE_CRM_SO_ITEM`,
# MAGIC S.`SPE_CRM_REF_SO`,
# MAGIC S.`SPE_CRM_REF_ITEM`,
# MAGIC S.`SPE_CRM_FKREL`,
# MAGIC S.`SPE_CHNG_SYS`,
# MAGIC S.`SPE_INSMK_SRC`,
# MAGIC S.`SPE_CQ_CTRLTYPE`,
# MAGIC S.`SPE_CQ_NOCQ`,
# MAGIC S.`REASON_CODE`,
# MAGIC S.`CQU_SAR`,
# MAGIC S.`ANZSN`,
# MAGIC S.`SPE_EWM_DTC`,
# MAGIC S.`EXLIN`,
# MAGIC S.`EXSNR`,
# MAGIC S.`EHTYP`,
# MAGIC S.`RETPC`,
# MAGIC S.`DPTYP`,
# MAGIC S.`DPPCT`,
# MAGIC S.`DPAMT`,
# MAGIC S.`DPDAT`,
# MAGIC S.`FLS_RSTO`,
# MAGIC S.`EXT_RFX_NUMBER`,
# MAGIC S.`EXT_RFX_ITEM`,
# MAGIC S.`EXT_RFX_SYSTEM`,
# MAGIC S.`SRM_CONTRACT_ID`,
# MAGIC S.`SRM_CONTRACT_ITM`,
# MAGIC S.`BLK_REASON_ID`,
# MAGIC S.`BLK_REASON_TXT`,
# MAGIC S.`ITCONS`,
# MAGIC S.`FIXMG`,
# MAGIC S.`WABWE`,
# MAGIC S.`CMPL_DLV_ITM`,
# MAGIC S.`INCO2_L`,
# MAGIC S.`INCO3_L`,
# MAGIC S.`STAWN`,
# MAGIC S.`ISVCO`,
# MAGIC S.`GRWRT`,
# MAGIC S.`SERVICEPERFORMER`,
# MAGIC S.`PRODUCTTYPE`,
# MAGIC S.`GR_BY_SES`,
# MAGIC S.`REQUESTFORQUOTATION`,
# MAGIC S.`REQUESTFORQUOTATIONITEM`,
# MAGIC S.`EXTMATERIALFORPURG`,
# MAGIC S.`TARGET_VALUE`,
# MAGIC S.`EXTERNALREFERENCEID`,
# MAGIC S.`TC_AUT_DET`,
# MAGIC S.`MANUAL_TC_REASON`,
# MAGIC S.`FISCAL_INCENTIVE`,
# MAGIC S.`TAX_SUBJECT_ST`,
# MAGIC S.`FISCAL_INCENTIVE_ID`,
# MAGIC S.`SF_TXJCD`,
# MAGIC S.`DUMMY_EKPO_INCL_EEW_PS`,
# MAGIC S.`EXPECTED_VALUE`,
# MAGIC S.`LIMIT_AMOUNT`,
# MAGIC S.`CONTRACT_FOR_LIMIT`,
# MAGIC S.`ENH_DATE1`,
# MAGIC S.`ENH_DATE2`,
# MAGIC S.`ENH_PERCENT`,
# MAGIC S.`ENH_NUMC1`,
# MAGIC S.`_DATAAGING`,
# MAGIC S.`CUPIT`,
# MAGIC S.`CIGIT`,
# MAGIC S.`/BEV1/NEGEN_ITEM`,
# MAGIC S.`/BEV1/NEDEPFREE`,
# MAGIC S.`/BEV1/NESTRUCCAT`,
# MAGIC S.`ADVCODE`,
# MAGIC S.`BUDGET_PD`,
# MAGIC S.`EXCPE`,
# MAGIC S.`FMFGUS_KEY`,
# MAGIC S.`IUID_RELEVANT`,
# MAGIC S.`MRPIND`,
# MAGIC S.`SGT_SCAT`,
# MAGIC S.`SGT_RCAT`,
# MAGIC S.`TMS_REF_UUID`,
# MAGIC S.`TMS_SRC_LOC_KEY`,
# MAGIC S.`TMS_DES_LOC_KEY`,
# MAGIC S.`WRF_CHARSTC1`,
# MAGIC S.`WRF_CHARSTC2`,
# MAGIC S.`WRF_CHARSTC3`,
# MAGIC S.`ZZSSTHT`,
# MAGIC S.`ZZRMANUM`,
# MAGIC S.`ZZVBELN`,
# MAGIC S.`ZZAUFNR`,
# MAGIC S.`ZZQMNUM`,
# MAGIC S.`ZZPPV`,
# MAGIC S.`ZZREAS_CODE`,
# MAGIC S.`ZZREQHOT`,
# MAGIC S.`ZZHOTQTY`,
# MAGIC S.`ZZPULLINDT`,
# MAGIC S.`ZZCURRAGREV`,
# MAGIC S.`ZZCURRSAPREV`,
# MAGIC S.`ZZPOREV`,
# MAGIC S.`ZZPQV`,
# MAGIC S.`ZZEBELP`,
# MAGIC S.`ZZMENGE`,
# MAGIC S.`ZZAUFNR_OVS`,
# MAGIC S.`ZZPLNBEZ`,
# MAGIC S.`ZZVORNR`,
# MAGIC S.`ZZAGILE_REV`,
# MAGIC S.`ZZVBELN_TOLL`,
# MAGIC S.`ZZPOSNR_TOLL`,
# MAGIC S.`ZZCORDER`,
# MAGIC S.`ZZCOITEM`,
# MAGIC S.`ZZMPN`,
# MAGIC S.`ZZMFNAM`,
# MAGIC S.`ZZPPV_TOLE`,
# MAGIC S.`REFSITE`,
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
# MAGIC S.`/SAPMP/GPOSE`,
# MAGIC S.`ANGPN`,
# MAGIC S.`ADMOI`,
# MAGIC S.`ADPRI`,
# MAGIC S.`LPRIO`,
# MAGIC S.`ADACN`,
# MAGIC S.`AFPNR`,
# MAGIC S.`BSARK`,
# MAGIC S.`AUDAT`,
# MAGIC S.`ANGNR`,
# MAGIC S.`PNSTAT`,
# MAGIC S.`ADDNS`,
# MAGIC S.`ASSIGNMENT_PRIORITY`,
# MAGIC S.`ARUN_GROUP_PRIO`,
# MAGIC S.`ARUN_ORDER_PRIO`,
# MAGIC S.`SERRU`,
# MAGIC S.`SERNP`,
# MAGIC S.`DISUB_SOBKZ`,
# MAGIC S.`DISUB_PSPNR`,
# MAGIC S.`DISUB_KUNNR`,
# MAGIC S.`DISUB_VBELN`,
# MAGIC S.`DISUB_POSNR`,
# MAGIC S.`DISUB_OWNER`,
# MAGIC S.`FSH_SEASON_YEAR`,
# MAGIC S.`FSH_SEASON`,
# MAGIC S.`FSH_COLLECTION`,
# MAGIC S.`FSH_THEME`,
# MAGIC S.`FSH_ATP_DATE`,
# MAGIC S.`FSH_VAS_REL`,
# MAGIC S.`FSH_VAS_PRNT_ID`,
# MAGIC S.`FSH_TRANSACTION`,
# MAGIC S.`FSH_ITEM_GROUP`,
# MAGIC S.`FSH_ITEM`,
# MAGIC S.`FSH_SS`,
# MAGIC S.`FSH_GRID_COND_REC`,
# MAGIC S.`FSH_PSM_PFM_SPLIT`,
# MAGIC S.`CNFM_QTY`,
# MAGIC S.`FSH_PQR_UEPOS`,
# MAGIC S.`RFM_DIVERSION`,
# MAGIC S.`RFM_SCC_INDICATOR`,
# MAGIC S.`STPAC`,
# MAGIC S.`LGBZO`,
# MAGIC S.`LGBZO_B`,
# MAGIC S.`ADDRNUM`,
# MAGIC S.`CONSNUM`,
# MAGIC S.`BORGR_MISS`,
# MAGIC S.`DEP_ID`,
# MAGIC S.`BELNR`,
# MAGIC S.`KBLPOS_CAB`,
# MAGIC S.`KBLNR_COMP`,
# MAGIC S.`KBLPOS_COMP`,
# MAGIC S.`WBS_ELEMENT`,
# MAGIC S.`RFM_PSST_RULE`,
# MAGIC S.`RFM_PSST_GROUP`,
# MAGIC S.`RFM_REF_DOC`,
# MAGIC S.`RFM_REF_ITEM`,
# MAGIC S.`RFM_REF_ACTION`,
# MAGIC S.`RFM_REF_SLITEM`,
# MAGIC S.`REF_ITEM`,
# MAGIC S.`SOURCE_ID`,
# MAGIC S.`SOURCE_KEY`,
# MAGIC S.`PUT_BACK`,
# MAGIC S.`POL_ID`,
# MAGIC S.`CONS_ORDER`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path,True)

# COMMAND ----------


