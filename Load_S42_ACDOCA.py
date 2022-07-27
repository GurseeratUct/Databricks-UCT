# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp,input_file_name
from delta.tables import *

# COMMAND ----------

table_name = 'ACDOCA'
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

schema = StructType([ \
                     StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                       StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                     StructField('RCLNT',StringType(),True),\
StructField('RLDNR',StringType(),True),\
StructField('RBUKRS',IntegerType(),True),\
StructField('GJAHR',StringType(),True),\
StructField('BELNR',StringType(),True),\
StructField('DOCLN',StringType(),True),\
StructField('RYEAR',StringType(),True),\
StructField('DOCNR_LD',StringType(),True),\
StructField('RRCTY',StringType(),True),\
StructField('RMVCT',StringType(),True),\
StructField('VORGN',StringType(),True),\
StructField('VRGNG',StringType(),True),\
StructField('BTTYPE',StringType(),True),\
StructField('CBTTYPE',StringType(),True),\
StructField('AWTYP',StringType(),True),\
StructField('AWSYS',StringType(),True),\
StructField('AWORG',StringType(),True),\
StructField('AWREF',StringType(),True),\
StructField('AWITEM',StringType(),True),\
StructField('AWITGRP',StringType(),True),\
StructField('SUBTA',StringType(),True),\
StructField('XREVERSING',StringType(),True),\
StructField('XREVERSED',StringType(),True),\
StructField('XTRUEREV',StringType(),True),\
StructField('AWTYP_REV',StringType(),True),\
StructField('AWORG_REV',StringType(),True),\
StructField('AWREF_REV',StringType(),True),\
StructField('AWITEM_REV',StringType(),True),\
StructField('SUBTA_REV',StringType(),True),\
StructField('XSETTLING',StringType(),True),\
StructField('XSETTLED',StringType(),True),\
StructField('PREC_AWTYP',StringType(),True),\
StructField('PREC_AWSYS',StringType(),True),\
StructField('PREC_AWORG',StringType(),True),\
StructField('PREC_AWREF',StringType(),True),\
StructField('PREC_AWITEM',StringType(),True),\
StructField('PREC_SUBTA',StringType(),True),\
StructField('PREC_AWMULT',StringType(),True),\
StructField('PREC_BUKRS',StringType(),True),\
StructField('PREC_GJAHR',StringType(),True),\
StructField('PREC_BELNR',StringType(),True),\
StructField('PREC_DOCLN',StringType(),True),\
StructField('XSECONDARY',StringType(),True),\
StructField('CLOSING_RUN_ID',StringType(),True),\
StructField('ORGL_CHANGE',StringType(),True),\
StructField('SRC_AWTYP',StringType(),True),\
StructField('SRC_AWSYS',StringType(),True),\
StructField('SRC_AWORG',StringType(),True),\
StructField('SRC_AWREF',StringType(),True),\
StructField('SRC_AWITEM',StringType(),True),\
StructField('SRC_AWSUBIT',IntegerType(),True),\
StructField('XCOMMITMENT',StringType(),True),\
StructField('OBS_REASON',StringType(),True),\
StructField('RTCUR',StringType(),True),\
StructField('RWCUR',StringType(),True),\
StructField('RHCUR',StringType(),True),\
StructField('RKCUR',StringType(),True),\
StructField('ROCUR',StringType(),True),\
StructField('RVCUR',StringType(),True),\
StructField('RBCUR',StringType(),True),\
StructField('RCCUR',StringType(),True),\
StructField('RDCUR',StringType(),True),\
StructField('RECUR',StringType(),True),\
StructField('RFCUR',StringType(),True),\
StructField('RGCUR',StringType(),True),\
StructField('RCO_OCUR',StringType(),True),\
StructField('RGM_OCUR',StringType(),True),\
StructField('RUNIT',StringType(),True),\
StructField('RVUNIT',StringType(),True),\
StructField('RRUNIT',StringType(),True),\
StructField('RMSL_TYPE',StringType(),True),\
StructField('RIUNIT',StringType(),True),\
StructField('QUNIT1',StringType(),True),\
StructField('QUNIT2',StringType(),True),\
StructField('QUNIT3',StringType(),True),\
StructField('CO_MEINH',StringType(),True),\
StructField('RACCT',StringType(),True),\
StructField('RCNTR',StringType(),True),\
StructField('PRCTR',StringType(),True),\
StructField('RFAREA',StringType(),True),\
StructField('RBUSA',StringType(),True),\
StructField('KOKRS',StringType(),True),\
StructField('SEGMENT',StringType(),True),\
StructField('SCNTR',StringType(),True),\
StructField('PPRCTR',StringType(),True),\
StructField('SFAREA',StringType(),True),\
StructField('SBUSA',StringType(),True),\
StructField('RASSC',StringType(),True),\
StructField('PSEGMENT',StringType(),True),\
StructField('TSL',StringType(),True),\
StructField('WSL',StringType(),True),\
StructField('WSL2',StringType(),True),\
StructField('WSL3',StringType(),True),\
StructField('HSL',StringType(),True),\
StructField('KSL',StringType(),True),\
StructField('OSL',StringType(),True),\
StructField('VSL',StringType(),True),\
StructField('BSL',StringType(),True),\
StructField('CSL',StringType(),True),\
StructField('DSL',StringType(),True),\
StructField('ESL',StringType(),True),\
StructField('FSL',StringType(),True),\
StructField('GSL',StringType(),True),\
StructField('KFSL',StringType(),True),\
StructField('KFSL2',StringType(),True),\
StructField('KFSL3',DoubleType(),True),\
StructField('PSL',DoubleType(),True),\
StructField('PSL2',StringType(),True),\
StructField('PSL3',DoubleType(),True),\
StructField('PFSL',StringType(),True),\
StructField('PFSL2',StringType(),True),\
StructField('PFSL3',StringType(),True),\
StructField('CO_OSL',StringType(),True),\
StructField('GM_OSL',StringType(),True),\
StructField('HSLALT',StringType(),True),\
StructField('KSLALT',StringType(),True),\
StructField('OSLALT',StringType(),True),\
StructField('VSLALT',StringType(),True),\
StructField('BSLALT',StringType(),True),\
StructField('CSLALT',DoubleType(),True),\
StructField('DSLALT',StringType(),True),\
StructField('ESLALT',StringType(),True),\
StructField('FSLALT',DoubleType(),True),\
StructField('GSLALT',StringType(),True),\
StructField('HSLEXT',DoubleType(),True),\
StructField('KSLEXT',StringType(),True),\
StructField('OSLEXT',StringType(),True),\
StructField('VSLEXT',StringType(),True),\
StructField('BSLEXT',StringType(),True),\
StructField('CSLEXT',StringType(),True),\
StructField('DSLEXT',DoubleType(),True),\
StructField('ESLEXT',DoubleType(),True),\
StructField('FSLEXT',StringType(),True),\
StructField('GSLEXT',DoubleType(),True),\
StructField('HVKWRT',StringType(),True),\
StructField('MSL',StringType(),True),\
StructField('MFSL',DoubleType(),True),\
StructField('VMSL',DoubleType(),True),\
StructField('VMFSL',StringType(),True),\
StructField('RMSL',StringType(),True),\
StructField('QUANT1',StringType(),True),\
StructField('QUANT2',StringType(),True),\
StructField('QUANT3',StringType(),True),\
StructField('CO_MEGBTR',StringType(),True),\
StructField('CO_MEFBTR',DoubleType(),True),\
StructField('HSALK3',StringType(),True),\
StructField('KSALK3',StringType(),True),\
StructField('OSALK3',StringType(),True),\
StructField('VSALK3',DoubleType(),True),\
StructField('HSALKV',StringType(),True),\
StructField('KSALKV',DoubleType(),True),\
StructField('OSALKV',DoubleType(),True),\
StructField('VSALKV',DoubleType(),True),\
StructField('HPVPRS',DoubleType(),True),\
StructField('KPVPRS',DoubleType(),True),\
StructField('OPVPRS',DoubleType(),True),\
StructField('VPVPRS',DoubleType(),True),\
StructField('HSTPRS',DoubleType(),True),\
StructField('KSTPRS',DoubleType(),True),\
StructField('OSTPRS',DoubleType(),True),\
StructField('VSTPRS',DoubleType(),True),\
StructField('HVKSAL',DoubleType(),True),\
StructField('LBKUM',StringType(),True),\
StructField('DRCRK',StringType(),True),\
StructField('POPER',IntegerType(),True),\
StructField('PERIV',StringType(),True),\
StructField('FISCYEARPER',IntegerType(),True),\
StructField('BUDAT',StringType(),True),\
StructField('BLDAT',StringType(),True),\
StructField('BLART',StringType(),True),\
StructField('BUZEI',StringType(),True),\
StructField('ZUONR',StringType(),True),\
StructField('BSCHL',StringType(),True),\
StructField('BSTAT',StringType(),True),\
StructField('LINETYPE',StringType(),True),\
StructField('KTOSL',StringType(),True),\
StructField('SLALITTYPE',StringType(),True),\
StructField('XSPLITMOD',StringType(),True),\
StructField('USNAM',StringType(),True),\
StructField('TIMESTAMP',StringType(),True),\
StructField('EPRCTR',StringType(),True),\
StructField('RHOART',StringType(),True),\
StructField('GLACCOUNT_TYPE',StringType(),True),\
StructField('KTOPL',StringType(),True),\
StructField('LOKKT',StringType(),True),\
StructField('KTOP2',StringType(),True),\
StructField('REBZG',StringType(),True),\
StructField('REBZJ',StringType(),True),\
StructField('REBZZ',StringType(),True),\
StructField('REBZT',StringType(),True),\
StructField('RBEST',StringType(),True),\
StructField('EBELN_LOGSYS',StringType(),True),\
StructField('EBELN',StringType(),True),\
StructField('EBELP',StringType(),True),\
StructField('ZEKKN',StringType(),True),\
StructField('SGTXT',StringType(),True),\
StructField('KDAUF',StringType(),True),\
StructField('KDPOS',StringType(),True),\
StructField('MATNR',StringType(),True),\
StructField('WERKS',StringType(),True),\
StructField('LIFNR',StringType(),True),\
StructField('KUNNR',StringType(),True),\
StructField('FBUDA',StringType(),True),\
StructField('PEROP_BEG',StringType(),True),\
StructField('PEROP_END',StringType(),True),\
StructField('COCO_NUM',StringType(),True),\
StructField('WWERT',StringType(),True),\
StructField('PRCTR_DRVTN_SOURCE_TYPE',StringType(),True),\
StructField('KOART',StringType(),True),\
StructField('UMSKZ',StringType(),True),\
StructField('TAX_COUNTRY',StringType(),True),\
StructField('MWSKZ',StringType(),True),\
StructField('HBKID',StringType(),True),\
StructField('HKTID',StringType(),True),\
StructField('VALUT',StringType(),True),\
StructField('XOPVW',StringType(),True),\
StructField('AUGDT',StringType(),True),\
StructField('AUGBL',StringType(),True),\
StructField('AUGGJ',StringType(),True),\
StructField('AFABE',StringType(),True),\
StructField('ANLN1',StringType(),True),\
StructField('ANLN2',StringType(),True),\
StructField('BZDAT',StringType(),True),\
StructField('ANBWA',StringType(),True),\
StructField('MOVCAT',StringType(),True),\
StructField('DEPR_PERIOD',IntegerType(),True),\
StructField('ANLGR',StringType(),True),\
StructField('ANLGR2',StringType(),True),\
StructField('SETTLEMENT_RULE',StringType(),True),\
StructField('ANLKL',StringType(),True),\
StructField('KTOGR',StringType(),True),\
StructField('PANL1',StringType(),True),\
StructField('PANL2',StringType(),True),\
StructField('UBZDT_PN',StringType(),True),\
StructField('XVABG_PN',StringType(),True),\
StructField('PROZS_PN',StringType(),True),\
StructField('XMANPROPVAL_PN',StringType(),True),\
StructField('KALNR',StringType(),True),\
StructField('VPRSV',StringType(),True),\
StructField('MLAST',StringType(),True),\
StructField('KZBWS',StringType(),True),\
StructField('XOBEW',StringType(),True),\
StructField('SOBKZ',StringType(),True),\
StructField('VTSTAMP',DoubleType(),True),\
StructField('MAT_KDAUF',StringType(),True),\
StructField('MAT_KDPOS',StringType(),True),\
StructField('MAT_PSPNR',StringType(),True),\
StructField('MAT_PS_POSID',StringType(),True),\
StructField('MAT_LIFNR',StringType(),True),\
StructField('BWTAR',StringType(),True),\
StructField('BWKEY',StringType(),True),\
StructField('HPEINH',StringType(),True),\
StructField('KPEINH',StringType(),True),\
StructField('OPEINH',StringType(),True),\
StructField('VPEINH',StringType(),True),\
StructField('MLPTYP',StringType(),True),\
StructField('MLCATEG',StringType(),True),\
StructField('QSBVALT',StringType(),True),\
StructField('QSPROCESS',StringType(),True),\
StructField('PERART',StringType(),True),\
StructField('MLPOSNR',StringType(),True),\
StructField('INV_MOV_CATEG',StringType(),True),\
StructField('BUKRS_SENDER',StringType(),True),\
StructField('RACCT_SENDER',StringType(),True),\
StructField('ACCAS_SENDER',StringType(),True),\
StructField('ACCASTY_SENDER',StringType(),True),\
StructField('OBJNR',StringType(),True),\
StructField('HRKFT',StringType(),True),\
StructField('HKGRP',StringType(),True),\
StructField('PAROB1',StringType(),True),\
StructField('PAROBSRC',StringType(),True),\
StructField('USPOB',StringType(),True),\
StructField('CO_BELKZ',StringType(),True),\
StructField('CO_BEKNZ',StringType(),True),\
StructField('BELTP',StringType(),True),\
StructField('MUVFLG',StringType(),True),\
StructField('GKONT',StringType(),True),\
StructField('GKOAR',StringType(),True),\
StructField('ERLKZ',StringType(),True),\
StructField('PERNR',StringType(),True),\
StructField('PAOBJNR',IntegerType(),True),\
StructField('XPAOBJNR_CO_REL',StringType(),True),\
StructField('SCOPE',StringType(),True),\
StructField('LOGSYSO',StringType(),True),\
StructField('PBUKRS',StringType(),True),\
StructField('PSCOPE',StringType(),True),\
StructField('LOGSYSP',StringType(),True),\
StructField('BWSTRAT',StringType(),True),\
StructField('OBJNR_HK',StringType(),True),\
StructField('AUFNR_ORG',StringType(),True),\
StructField('UKOSTL',StringType(),True),\
StructField('ULSTAR',StringType(),True),\
StructField('UPRZNR',StringType(),True),\
StructField('UPRCTR',StringType(),True),\
StructField('UMATNR',StringType(),True),\
StructField('VARC_UACCT',StringType(),True),\
StructField('ACCAS',StringType(),True),\
StructField('ACCASTY',StringType(),True),\
StructField('LSTAR',StringType(),True),\
StructField('AUFNR',StringType(),True),\
StructField('AUTYP',IntegerType(),True),\
StructField('PS_PSP_PNR',IntegerType(),True),\
StructField('PS_POSID',StringType(),True),\
StructField('PS_PRJ_PNR',IntegerType(),True),\
StructField('PS_PSPID',StringType(),True),\
StructField('NPLNR',StringType(),True),\
StructField('NPLNR_VORGN',StringType(),True),\
StructField('PRZNR',StringType(),True),\
StructField('KSTRG',StringType(),True),\
StructField('BEMOT',StringType(),True),\
StructField('RSRCE',StringType(),True),\
StructField('QMNUM',StringType(),True),\
StructField('SERVICE_DOC_TYPE',StringType(),True),\
StructField('SERVICE_DOC_ID',StringType(),True),\
StructField('SERVICE_DOC_ITEM_ID',IntegerType(),True),\
StructField('SERVICE_CONTRACT_TYPE',StringType(),True),\
StructField('SERVICE_CONTRACT_ID',StringType(),True),\
StructField('SERVICE_CONTRACT_ITEM_ID',IntegerType(),True),\
StructField('SOLUTION_ORDER_ID',StringType(),True),\
StructField('SOLUTION_ORDER_ITEM_ID',IntegerType(),True),\
StructField('ERKRS',StringType(),True),\
StructField('PACCAS',StringType(),True),\
StructField('PACCASTY',StringType(),True),\
StructField('PLSTAR',StringType(),True),\
StructField('PAUFNR',StringType(),True),\
StructField('PAUTYP',IntegerType(),True),\
StructField('PPS_PSP_PNR',IntegerType(),True),\
StructField('PPS_POSID',StringType(),True),\
StructField('PPS_PRJ_PNR',IntegerType(),True),\
StructField('PPS_PSPID',StringType(),True),\
StructField('PKDAUF',StringType(),True),\
StructField('PKDPOS',IntegerType(),True),\
StructField('PPAOBJNR',IntegerType(),True),\
StructField('PNPLNR',StringType(),True),\
StructField('PNPLNR_VORGN',StringType(),True),\
StructField('PPRZNR',StringType(),True),\
StructField('PKSTRG',StringType(),True),\
StructField('PSERVICE_DOC_TYPE',StringType(),True),\
StructField('PSERVICE_DOC_ID',StringType(),True),\
StructField('PSERVICE_DOC_ITEM_ID',IntegerType(),True),\
StructField('CO_ACCASTY_N1',StringType(),True),\
StructField('CO_ACCASTY_N2',StringType(),True),\
StructField('CO_ACCASTY_N3',StringType(),True),\
StructField('CO_ZLENR',IntegerType(),True),\
StructField('CO_BELNR',StringType(),True),\
StructField('CO_BUZEI',IntegerType(),True),\
StructField('CO_BUZEI1',IntegerType(),True),\
StructField('CO_BUZEI2',IntegerType(),True),\
StructField('CO_BUZEI5',IntegerType(),True),\
StructField('CO_BUZEI6',IntegerType(),True),\
StructField('CO_BUZEI7',IntegerType(),True),\
StructField('CO_REFBZ',IntegerType(),True),\
StructField('CO_REFBZ1',IntegerType(),True),\
StructField('CO_REFBZ2',IntegerType(),True),\
StructField('CO_REFBZ5',IntegerType(),True),\
StructField('CO_REFBZ6',IntegerType(),True),\
StructField('CO_REFBZ7',IntegerType(),True),\
StructField('OVERTIMECAT',StringType(),True),\
StructField('WORK_ITEM_ID',StringType(),True),\
StructField('ARBID',IntegerType(),True),\
StructField('VORNR',StringType(),True),\
StructField('AUFPS',IntegerType(),True),\
StructField('UVORN',StringType(),True),\
StructField('EQUNR',StringType(),True),\
StructField('TPLNR',StringType(),True),\
StructField('ISTRU',StringType(),True),\
StructField('ILART',StringType(),True),\
StructField('PLKNZ',StringType(),True),\
StructField('ARTPR',StringType(),True),\
StructField('PRIOK',StringType(),True),\
StructField('MAUFNR',StringType(),True),\
StructField('MATKL_MM',StringType(),True),\
StructField('PAUFPS',IntegerType(),True),\
StructField('PLANNED_PARTS_WORK',StringType(),True),\
StructField('FKART',StringType(),True),\
StructField('VKORG',StringType(),True),\
StructField('VTWEG',StringType(),True),\
StructField('SPART',StringType(),True),\
StructField('MATNR_COPA',StringType(),True),\
StructField('MATKL',StringType(),True),\
StructField('KDGRP',StringType(),True),\
StructField('LAND1',StringType(),True),\
StructField('BRSCH',StringType(),True),\
StructField('BZIRK',StringType(),True),\
StructField('KUNRE',StringType(),True),\
StructField('KUNWE',StringType(),True),\
StructField('KONZS',StringType(),True),\
StructField('ACDOC_COPA_EEW_DUMMY_PA',StringType(),True),\
StructField('KMVKBU_PA',StringType(),True),\
StructField('KMVKGR_PA',StringType(),True),\
StructField('KUNNR_PA',StringType(),True),\
StructField('PAPH1_PA',StringType(),True),\
StructField('PAPH2_PA',StringType(),True),\
StructField('PAPH3_PA',StringType(),True),\
StructField('WW001_PA',StringType(),True),\
StructField('WW002_PA',StringType(),True),\
StructField('PRODH_PA',StringType(),True),\
StructField('PAPH4_PA',StringType(),True),\
StructField('KMHI01_PA',StringType(),True),\
StructField('KMHI02_PA',StringType(),True),\
StructField('KMHI03_PA',StringType(),True),\
StructField('KTGRD_PA',StringType(),True),\
StructField('PAPH5_PA',StringType(),True),\
StructField('DUMMY_MRKT_SGMNT_EEW_PS',StringType(),True),\
StructField('RE_BUKRS',StringType(),True),\
StructField('RE_ACCOUNT',StringType(),True),\
StructField('FIKRS',StringType(),True),\
StructField('FIPEX',StringType(),True),\
StructField('FISTL',StringType(),True),\
StructField('MEASURE',StringType(),True),\
StructField('RFUND',StringType(),True),\
StructField('RGRANT_NBR',StringType(),True),\
StructField('RBUDGET_PD',StringType(),True),\
StructField('SFUND',StringType(),True),\
StructField('SGRANT_NBR',StringType(),True),\
StructField('SBUDGET_PD',StringType(),True),\
StructField('BDGT_ACCOUNT',StringType(),True),\
StructField('BDGT_ACCOUNT_COCODE',StringType(),True),\
StructField('BDGT_CNSMPN_DATE',StringType(),True),\
StructField('BDGT_CNSMPN_PERIOD',IntegerType(),True),\
StructField('BDGT_CNSMPN_YEAR',IntegerType(),True),\
StructField('BDGT_RELEVANT',StringType(),True),\
StructField('BDGT_CNSMPN_TYPE',StringType(),True),\
StructField('BDGT_CNSMPN_AMOUNT_TYPE',StringType(),True),\
StructField('RSPONSORED_PROG',StringType(),True),\
StructField('RSPONSORED_CLASS',StringType(),True),\
StructField('RBDGT_VLDTY_NBR',StringType(),True),\
StructField('KBLNR',StringType(),True),\
StructField('KBLPOS',IntegerType(),True),\
StructField('VNAME',StringType(),True),\
StructField('EGRUP',StringType(),True),\
StructField('RECID',StringType(),True),\
StructField('VPTNR',StringType(),True),\
StructField('BTYPE',StringType(),True),\
StructField('ETYPE',StringType(),True),\
StructField('PRODPER',StringType(),True),\
StructField('BILLM',StringType(),True),\
StructField('POM',StringType(),True),\
StructField('CBRUNID',DoubleType(),True),\
StructField('JVACTIVITY',StringType(),True),\
StructField('PVNAME',StringType(),True),\
StructField('PEGRUP',StringType(),True),\
StructField('S_RECIND',StringType(),True),\
StructField('CBRACCT',StringType(),True),\
StructField('CBOBJNR',StringType(),True),\
StructField('SWENR',StringType(),True),\
StructField('SGENR',StringType(),True),\
StructField('SGRNR',StringType(),True),\
StructField('SMENR',StringType(),True),\
StructField('RECNNR',StringType(),True),\
StructField('SNKSL',StringType(),True),\
StructField('SEMPSL',StringType(),True),\
StructField('DABRZ',StringType(),True),\
StructField('PSWENR',StringType(),True),\
StructField('PSGENR',StringType(),True),\
StructField('PSGRNR',StringType(),True),\
StructField('PSMENR',StringType(),True),\
StructField('PRECNNR',StringType(),True),\
StructField('PSNKSL',StringType(),True),\
StructField('PSEMPSL',StringType(),True),\
StructField('PDABRZ',StringType(),True),\
StructField('ACROBJTYPE',StringType(),True),\
StructField('ACRLOGSYS',StringType(),True),\
StructField('ACROBJ_ID',StringType(),True),\
StructField('ACRSOBJ_ID',StringType(),True),\
StructField('ACRITMTYPE',StringType(),True),\
StructField('ACRVALDAT',StringType(),True),\
StructField('VALOBJTYPE',StringType(),True),\
StructField('VALOBJ_ID',StringType(),True),\
StructField('VALSOBJ_ID',StringType(),True),\
StructField('NETDT',StringType(),True),\
StructField('RISK_CLASS',StringType(),True),\
StructField('ACDOC_EEW_DUMMY',StringType(),True),\
StructField('DUMMY_INCL_EEW_COBL',StringType(),True),\
StructField('FUP_ACTION',StringType(),True),\
StructField('SDM_VERSION',StringType(),True),\
StructField('MIG_SOURCE',StringType(),True),\
StructField('MIG_DOCLN',StringType(),True),\
StructField('_DATAAGING',StringType(),True),\
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

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')).withColumn("input_file_name", input_file_name())

# COMMAND ----------

df_transform = df_add_column.na.fill(0).withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
.withColumn("ACRVALDAT", regexp_replace(df_add_column.ACRVALDAT,'-',''))\
.withColumn("AUGDT", regexp_replace(df_add_column.AUGDT,'-',''))\
.withColumn("BDGT_CNSMPN_DATE", regexp_replace(df_add_column.BDGT_CNSMPN_DATE,'-',''))\
.withColumn("BILLM", regexp_replace(df_add_column.BILLM,'-',''))\
.withColumn("BLDAT", regexp_replace(df_add_column.BLDAT,'-',''))\
.withColumn("BUDAT", regexp_replace(df_add_column.BUDAT,'-',''))\
.withColumn("BZDAT", regexp_replace(df_add_column.BZDAT,'-',''))\
.withColumn("DABRZ", regexp_replace(df_add_column.DABRZ,'-',''))\
.withColumn("FBUDA", regexp_replace(df_add_column.FBUDA,'-',''))\
.withColumn("NETDT", regexp_replace(df_add_column.NETDT,'-',''))\
.withColumn("PDABRZ", regexp_replace(df_add_column.PDABRZ,'-',''))\
.withColumn("PEROP_BEG", regexp_replace(df_add_column.PEROP_BEG,'-',''))\
.withColumn("PEROP_END", regexp_replace(df_add_column.PEROP_END,'-',''))\
.withColumn("POM", regexp_replace(df_add_column.POM,'-',''))\
.withColumn("PRODPER", regexp_replace(df_add_column.PRODPER,'-',''))\
.withColumn("UBZDT_PN", regexp_replace(df_add_column.UBZDT_PN,'-',''))\
.withColumn("VALUT", regexp_replace(df_add_column.VALUT,'-',''))\
.withColumn("WWERT", regexp_replace(df_add_column.WWERT,'-',''))\
.withColumn("_DATAAGING", regexp_replace(df_add_column._DATAAGING,'-',''))


# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.ACDOCA as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY RCLNT,RLDNR,RBUKRS,GJAHR,BELNR,DOCLN ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_ACDOCA)A where A.rn = 1 ) as S 
# MAGIC ON T.RCLNT = S.RCLNT and 
# MAGIC T.RLDNR = S.RBUKRS and
# MAGIC T.RBUKRS = S.RBUKRS and
# MAGIC T.GJAHR = S.GJAHR and
# MAGIC T.BELNR = S.BELNR and
# MAGIC T.DOCLN = S.DOCLN 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`RCLNT` =  S.`RCLNT`,
# MAGIC T.`RLDNR` =  S.`RLDNR`,
# MAGIC T.`RBUKRS` =  S.`RBUKRS`,
# MAGIC T.`GJAHR` =  S.`GJAHR`,
# MAGIC T.`BELNR` =  S.`BELNR`,
# MAGIC T.`DOCLN` =  S.`DOCLN`,
# MAGIC T.`RYEAR` =  S.`RYEAR`,
# MAGIC T.`DOCNR_LD` =  S.`DOCNR_LD`,
# MAGIC T.`RRCTY` =  S.`RRCTY`,
# MAGIC T.`RMVCT` =  S.`RMVCT`,
# MAGIC T.`VORGN` =  S.`VORGN`,
# MAGIC T.`VRGNG` =  S.`VRGNG`,
# MAGIC T.`BTTYPE` =  S.`BTTYPE`,
# MAGIC T.`CBTTYPE` =  S.`CBTTYPE`,
# MAGIC T.`AWTYP` =  S.`AWTYP`,
# MAGIC T.`AWSYS` =  S.`AWSYS`,
# MAGIC T.`AWORG` =  S.`AWORG`,
# MAGIC T.`AWREF` =  S.`AWREF`,
# MAGIC T.`AWITEM` =  S.`AWITEM`,
# MAGIC T.`AWITGRP` =  S.`AWITGRP`,
# MAGIC T.`SUBTA` =  S.`SUBTA`,
# MAGIC T.`XREVERSING` =  S.`XREVERSING`,
# MAGIC T.`XREVERSED` =  S.`XREVERSED`,
# MAGIC T.`XTRUEREV` =  S.`XTRUEREV`,
# MAGIC T.`AWTYP_REV` =  S.`AWTYP_REV`,
# MAGIC T.`AWORG_REV` =  S.`AWORG_REV`,
# MAGIC T.`AWREF_REV` =  S.`AWREF_REV`,
# MAGIC T.`AWITEM_REV` =  S.`AWITEM_REV`,
# MAGIC T.`SUBTA_REV` =  S.`SUBTA_REV`,
# MAGIC T.`XSETTLING` =  S.`XSETTLING`,
# MAGIC T.`XSETTLED` =  S.`XSETTLED`,
# MAGIC T.`PREC_AWTYP` =  S.`PREC_AWTYP`,
# MAGIC T.`PREC_AWSYS` =  S.`PREC_AWSYS`,
# MAGIC T.`PREC_AWORG` =  S.`PREC_AWORG`,
# MAGIC T.`PREC_AWREF` =  S.`PREC_AWREF`,
# MAGIC T.`PREC_AWITEM` =  S.`PREC_AWITEM`,
# MAGIC T.`PREC_SUBTA` =  S.`PREC_SUBTA`,
# MAGIC T.`PREC_AWMULT` =  S.`PREC_AWMULT`,
# MAGIC T.`PREC_BUKRS` =  S.`PREC_BUKRS`,
# MAGIC T.`PREC_GJAHR` =  S.`PREC_GJAHR`,
# MAGIC T.`PREC_BELNR` =  S.`PREC_BELNR`,
# MAGIC T.`PREC_DOCLN` =  S.`PREC_DOCLN`,
# MAGIC T.`XSECONDARY` =  S.`XSECONDARY`,
# MAGIC T.`CLOSING_RUN_ID` =  S.`CLOSING_RUN_ID`,
# MAGIC T.`ORGL_CHANGE` =  S.`ORGL_CHANGE`,
# MAGIC T.`SRC_AWTYP` =  S.`SRC_AWTYP`,
# MAGIC T.`SRC_AWSYS` =  S.`SRC_AWSYS`,
# MAGIC T.`SRC_AWORG` =  S.`SRC_AWORG`,
# MAGIC T.`SRC_AWREF` =  S.`SRC_AWREF`,
# MAGIC T.`SRC_AWITEM` =  S.`SRC_AWITEM`,
# MAGIC T.`SRC_AWSUBIT` =  S.`SRC_AWSUBIT`,
# MAGIC T.`XCOMMITMENT` =  S.`XCOMMITMENT`,
# MAGIC T.`OBS_REASON` =  S.`OBS_REASON`,
# MAGIC T.`RTCUR` =  S.`RTCUR`,
# MAGIC T.`RWCUR` =  S.`RWCUR`,
# MAGIC T.`RHCUR` =  S.`RHCUR`,
# MAGIC T.`RKCUR` =  S.`RKCUR`,
# MAGIC T.`ROCUR` =  S.`ROCUR`,
# MAGIC T.`RVCUR` =  S.`RVCUR`,
# MAGIC T.`RBCUR` =  S.`RBCUR`,
# MAGIC T.`RCCUR` =  S.`RCCUR`,
# MAGIC T.`RDCUR` =  S.`RDCUR`,
# MAGIC T.`RECUR` =  S.`RECUR`,
# MAGIC T.`RFCUR` =  S.`RFCUR`,
# MAGIC T.`RGCUR` =  S.`RGCUR`,
# MAGIC T.`RCO_OCUR` =  S.`RCO_OCUR`,
# MAGIC T.`RGM_OCUR` =  S.`RGM_OCUR`,
# MAGIC T.`RUNIT` =  S.`RUNIT`,
# MAGIC T.`RVUNIT` =  S.`RVUNIT`,
# MAGIC T.`RRUNIT` =  S.`RRUNIT`,
# MAGIC T.`RMSL_TYPE` =  S.`RMSL_TYPE`,
# MAGIC T.`RIUNIT` =  S.`RIUNIT`,
# MAGIC T.`QUNIT1` =  S.`QUNIT1`,
# MAGIC T.`QUNIT2` =  S.`QUNIT2`,
# MAGIC T.`QUNIT3` =  S.`QUNIT3`,
# MAGIC T.`CO_MEINH` =  S.`CO_MEINH`,
# MAGIC T.`RACCT` =  S.`RACCT`,
# MAGIC T.`RCNTR` =  S.`RCNTR`,
# MAGIC T.`PRCTR` =  S.`PRCTR`,
# MAGIC T.`RFAREA` =  S.`RFAREA`,
# MAGIC T.`RBUSA` =  S.`RBUSA`,
# MAGIC T.`KOKRS` =  S.`KOKRS`,
# MAGIC T.`SEGMENT` =  S.`SEGMENT`,
# MAGIC T.`SCNTR` =  S.`SCNTR`,
# MAGIC T.`PPRCTR` =  S.`PPRCTR`,
# MAGIC T.`SFAREA` =  S.`SFAREA`,
# MAGIC T.`SBUSA` =  S.`SBUSA`,
# MAGIC T.`RASSC` =  S.`RASSC`,
# MAGIC T.`PSEGMENT` =  S.`PSEGMENT`,
# MAGIC T.`TSL` =  S.`TSL`,
# MAGIC T.`WSL` =  S.`WSL`,
# MAGIC T.`WSL2` =  S.`WSL2`,
# MAGIC T.`WSL3` =  S.`WSL3`,
# MAGIC T.`HSL` =  S.`HSL`,
# MAGIC T.`KSL` =  S.`KSL`,
# MAGIC T.`OSL` =  S.`OSL`,
# MAGIC T.`VSL` =  S.`VSL`,
# MAGIC T.`BSL` =  S.`BSL`,
# MAGIC T.`CSL` =  S.`CSL`,
# MAGIC T.`DSL` =  S.`DSL`,
# MAGIC T.`ESL` =  S.`ESL`,
# MAGIC T.`FSL` =  S.`FSL`,
# MAGIC T.`GSL` =  S.`GSL`,
# MAGIC T.`KFSL` =  S.`KFSL`,
# MAGIC T.`KFSL2` =  S.`KFSL2`,
# MAGIC T.`KFSL3` =  S.`KFSL3`,
# MAGIC T.`PSL` =  S.`PSL`,
# MAGIC T.`PSL2` =  S.`PSL2`,
# MAGIC T.`PSL3` =  S.`PSL3`,
# MAGIC T.`PFSL` =  S.`PFSL`,
# MAGIC T.`PFSL2` =  S.`PFSL2`,
# MAGIC T.`PFSL3` =  S.`PFSL3`,
# MAGIC T.`CO_OSL` =  S.`CO_OSL`,
# MAGIC T.`GM_OSL` =  S.`GM_OSL`,
# MAGIC T.`HSLALT` =  S.`HSLALT`,
# MAGIC T.`KSLALT` =  S.`KSLALT`,
# MAGIC T.`OSLALT` =  S.`OSLALT`,
# MAGIC T.`VSLALT` =  S.`VSLALT`,
# MAGIC T.`BSLALT` =  S.`BSLALT`,
# MAGIC T.`CSLALT` =  S.`CSLALT`,
# MAGIC T.`DSLALT` =  S.`DSLALT`,
# MAGIC T.`ESLALT` =  S.`ESLALT`,
# MAGIC T.`FSLALT` =  S.`FSLALT`,
# MAGIC T.`GSLALT` =  S.`GSLALT`,
# MAGIC T.`HSLEXT` =  S.`HSLEXT`,
# MAGIC T.`KSLEXT` =  S.`KSLEXT`,
# MAGIC T.`OSLEXT` =  S.`OSLEXT`,
# MAGIC T.`VSLEXT` =  S.`VSLEXT`,
# MAGIC T.`BSLEXT` =  S.`BSLEXT`,
# MAGIC T.`CSLEXT` =  S.`CSLEXT`,
# MAGIC T.`DSLEXT` =  S.`DSLEXT`,
# MAGIC T.`ESLEXT` =  S.`ESLEXT`,
# MAGIC T.`FSLEXT` =  S.`FSLEXT`,
# MAGIC T.`GSLEXT` =  S.`GSLEXT`,
# MAGIC T.`HVKWRT` =  S.`HVKWRT`,
# MAGIC T.`MSL` =  S.`MSL`,
# MAGIC T.`MFSL` =  S.`MFSL`,
# MAGIC T.`VMSL` =  S.`VMSL`,
# MAGIC T.`VMFSL` =  S.`VMFSL`,
# MAGIC T.`RMSL` =  S.`RMSL`,
# MAGIC T.`QUANT1` =  S.`QUANT1`,
# MAGIC T.`QUANT2` =  S.`QUANT2`,
# MAGIC T.`QUANT3` =  S.`QUANT3`,
# MAGIC T.`CO_MEGBTR` =  S.`CO_MEGBTR`,
# MAGIC T.`CO_MEFBTR` =  S.`CO_MEFBTR`,
# MAGIC T.`HSALK3` =  S.`HSALK3`,
# MAGIC T.`KSALK3` =  S.`KSALK3`,
# MAGIC T.`OSALK3` =  S.`OSALK3`,
# MAGIC T.`VSALK3` =  S.`VSALK3`,
# MAGIC T.`HSALKV` =  S.`HSALKV`,
# MAGIC T.`KSALKV` =  S.`KSALKV`,
# MAGIC T.`OSALKV` =  S.`OSALKV`,
# MAGIC T.`VSALKV` =  S.`VSALKV`,
# MAGIC T.`HPVPRS` =  S.`HPVPRS`,
# MAGIC T.`KPVPRS` =  S.`KPVPRS`,
# MAGIC T.`OPVPRS` =  S.`OPVPRS`,
# MAGIC T.`VPVPRS` =  S.`VPVPRS`,
# MAGIC T.`HSTPRS` =  S.`HSTPRS`,
# MAGIC T.`KSTPRS` =  S.`KSTPRS`,
# MAGIC T.`OSTPRS` =  S.`OSTPRS`,
# MAGIC T.`VSTPRS` =  S.`VSTPRS`,
# MAGIC T.`HVKSAL` =  S.`HVKSAL`,
# MAGIC T.`LBKUM` =  S.`LBKUM`,
# MAGIC T.`DRCRK` =  S.`DRCRK`,
# MAGIC T.`POPER` =  S.`POPER`,
# MAGIC T.`PERIV` =  S.`PERIV`,
# MAGIC T.`FISCYEARPER` =  S.`FISCYEARPER`,
# MAGIC T.`BUDAT` =  S.`BUDAT`,
# MAGIC T.`BLDAT` =  S.`BLDAT`,
# MAGIC T.`BLART` =  S.`BLART`,
# MAGIC T.`BUZEI` =  S.`BUZEI`,
# MAGIC T.`ZUONR` =  S.`ZUONR`,
# MAGIC T.`BSCHL` =  S.`BSCHL`,
# MAGIC T.`BSTAT` =  S.`BSTAT`,
# MAGIC T.`LINETYPE` =  S.`LINETYPE`,
# MAGIC T.`KTOSL` =  S.`KTOSL`,
# MAGIC T.`SLALITTYPE` =  S.`SLALITTYPE`,
# MAGIC T.`XSPLITMOD` =  S.`XSPLITMOD`,
# MAGIC T.`USNAM` =  S.`USNAM`,
# MAGIC T.`TIMESTAMP` =  S.`TIMESTAMP`,
# MAGIC T.`EPRCTR` =  S.`EPRCTR`,
# MAGIC T.`RHOART` =  S.`RHOART`,
# MAGIC T.`GLACCOUNT_TYPE` =  S.`GLACCOUNT_TYPE`,
# MAGIC T.`KTOPL` =  S.`KTOPL`,
# MAGIC T.`LOKKT` =  S.`LOKKT`,
# MAGIC T.`KTOP2` =  S.`KTOP2`,
# MAGIC T.`REBZG` =  S.`REBZG`,
# MAGIC T.`REBZJ` =  S.`REBZJ`,
# MAGIC T.`REBZZ` =  S.`REBZZ`,
# MAGIC T.`REBZT` =  S.`REBZT`,
# MAGIC T.`RBEST` =  S.`RBEST`,
# MAGIC T.`EBELN_LOGSYS` =  S.`EBELN_LOGSYS`,
# MAGIC T.`EBELN` =  S.`EBELN`,
# MAGIC T.`EBELP` =  S.`EBELP`,
# MAGIC T.`ZEKKN` =  S.`ZEKKN`,
# MAGIC T.`SGTXT` =  S.`SGTXT`,
# MAGIC T.`KDAUF` =  S.`KDAUF`,
# MAGIC T.`KDPOS` =  S.`KDPOS`,
# MAGIC T.`MATNR` =  S.`MATNR`,
# MAGIC T.`WERKS` =  S.`WERKS`,
# MAGIC T.`LIFNR` =  S.`LIFNR`,
# MAGIC T.`KUNNR` =  S.`KUNNR`,
# MAGIC T.`FBUDA` =  S.`FBUDA`,
# MAGIC T.`PEROP_BEG` =  S.`PEROP_BEG`,
# MAGIC T.`PEROP_END` =  S.`PEROP_END`,
# MAGIC T.`COCO_NUM` =  S.`COCO_NUM`,
# MAGIC T.`WWERT` =  S.`WWERT`,
# MAGIC T.`PRCTR_DRVTN_SOURCE_TYPE` =  S.`PRCTR_DRVTN_SOURCE_TYPE`,
# MAGIC T.`KOART` =  S.`KOART`,
# MAGIC T.`UMSKZ` =  S.`UMSKZ`,
# MAGIC T.`TAX_COUNTRY` =  S.`TAX_COUNTRY`,
# MAGIC T.`MWSKZ` =  S.`MWSKZ`,
# MAGIC T.`HBKID` =  S.`HBKID`,
# MAGIC T.`HKTID` =  S.`HKTID`,
# MAGIC T.`VALUT` =  S.`VALUT`,
# MAGIC T.`XOPVW` =  S.`XOPVW`,
# MAGIC T.`AUGDT` =  S.`AUGDT`,
# MAGIC T.`AUGBL` =  S.`AUGBL`,
# MAGIC T.`AUGGJ` =  S.`AUGGJ`,
# MAGIC T.`AFABE` =  S.`AFABE`,
# MAGIC T.`ANLN1` =  S.`ANLN1`,
# MAGIC T.`ANLN2` =  S.`ANLN2`,
# MAGIC T.`BZDAT` =  S.`BZDAT`,
# MAGIC T.`ANBWA` =  S.`ANBWA`,
# MAGIC T.`MOVCAT` =  S.`MOVCAT`,
# MAGIC T.`DEPR_PERIOD` =  S.`DEPR_PERIOD`,
# MAGIC T.`ANLGR` =  S.`ANLGR`,
# MAGIC T.`ANLGR2` =  S.`ANLGR2`,
# MAGIC T.`SETTLEMENT_RULE` =  S.`SETTLEMENT_RULE`,
# MAGIC T.`ANLKL` =  S.`ANLKL`,
# MAGIC T.`KTOGR` =  S.`KTOGR`,
# MAGIC T.`PANL1` =  S.`PANL1`,
# MAGIC T.`PANL2` =  S.`PANL2`,
# MAGIC T.`UBZDT_PN` =  S.`UBZDT_PN`,
# MAGIC T.`XVABG_PN` =  S.`XVABG_PN`,
# MAGIC T.`PROZS_PN` =  S.`PROZS_PN`,
# MAGIC T.`XMANPROPVAL_PN` =  S.`XMANPROPVAL_PN`,
# MAGIC T.`KALNR` =  S.`KALNR`,
# MAGIC T.`VPRSV` =  S.`VPRSV`,
# MAGIC T.`MLAST` =  S.`MLAST`,
# MAGIC T.`KZBWS` =  S.`KZBWS`,
# MAGIC T.`XOBEW` =  S.`XOBEW`,
# MAGIC T.`SOBKZ` =  S.`SOBKZ`,
# MAGIC T.`VTSTAMP` =  S.`VTSTAMP`,
# MAGIC T.`MAT_KDAUF` =  S.`MAT_KDAUF`,
# MAGIC T.`MAT_KDPOS` =  S.`MAT_KDPOS`,
# MAGIC T.`MAT_PSPNR` =  S.`MAT_PSPNR`,
# MAGIC T.`MAT_PS_POSID` =  S.`MAT_PS_POSID`,
# MAGIC T.`MAT_LIFNR` =  S.`MAT_LIFNR`,
# MAGIC T.`BWTAR` =  S.`BWTAR`,
# MAGIC T.`BWKEY` =  S.`BWKEY`,
# MAGIC T.`HPEINH` =  S.`HPEINH`,
# MAGIC T.`KPEINH` =  S.`KPEINH`,
# MAGIC T.`OPEINH` =  S.`OPEINH`,
# MAGIC T.`VPEINH` =  S.`VPEINH`,
# MAGIC T.`MLPTYP` =  S.`MLPTYP`,
# MAGIC T.`MLCATEG` =  S.`MLCATEG`,
# MAGIC T.`QSBVALT` =  S.`QSBVALT`,
# MAGIC T.`QSPROCESS` =  S.`QSPROCESS`,
# MAGIC T.`PERART` =  S.`PERART`,
# MAGIC T.`MLPOSNR` =  S.`MLPOSNR`,
# MAGIC T.`INV_MOV_CATEG` =  S.`INV_MOV_CATEG`,
# MAGIC T.`BUKRS_SENDER` =  S.`BUKRS_SENDER`,
# MAGIC T.`RACCT_SENDER` =  S.`RACCT_SENDER`,
# MAGIC T.`ACCAS_SENDER` =  S.`ACCAS_SENDER`,
# MAGIC T.`ACCASTY_SENDER` =  S.`ACCASTY_SENDER`,
# MAGIC T.`OBJNR` =  S.`OBJNR`,
# MAGIC T.`HRKFT` =  S.`HRKFT`,
# MAGIC T.`HKGRP` =  S.`HKGRP`,
# MAGIC T.`PAROB1` =  S.`PAROB1`,
# MAGIC T.`PAROBSRC` =  S.`PAROBSRC`,
# MAGIC T.`USPOB` =  S.`USPOB`,
# MAGIC T.`CO_BELKZ` =  S.`CO_BELKZ`,
# MAGIC T.`CO_BEKNZ` =  S.`CO_BEKNZ`,
# MAGIC T.`BELTP` =  S.`BELTP`,
# MAGIC T.`MUVFLG` =  S.`MUVFLG`,
# MAGIC T.`GKONT` =  S.`GKONT`,
# MAGIC T.`GKOAR` =  S.`GKOAR`,
# MAGIC T.`ERLKZ` =  S.`ERLKZ`,
# MAGIC T.`PERNR` =  S.`PERNR`,
# MAGIC T.`PAOBJNR` =  S.`PAOBJNR`,
# MAGIC T.`XPAOBJNR_CO_REL` =  S.`XPAOBJNR_CO_REL`,
# MAGIC T.`SCOPE` =  S.`SCOPE`,
# MAGIC T.`LOGSYSO` =  S.`LOGSYSO`,
# MAGIC T.`PBUKRS` =  S.`PBUKRS`,
# MAGIC T.`PSCOPE` =  S.`PSCOPE`,
# MAGIC T.`LOGSYSP` =  S.`LOGSYSP`,
# MAGIC T.`BWSTRAT` =  S.`BWSTRAT`,
# MAGIC T.`OBJNR_HK` =  S.`OBJNR_HK`,
# MAGIC T.`AUFNR_ORG` =  S.`AUFNR_ORG`,
# MAGIC T.`UKOSTL` =  S.`UKOSTL`,
# MAGIC T.`ULSTAR` =  S.`ULSTAR`,
# MAGIC T.`UPRZNR` =  S.`UPRZNR`,
# MAGIC T.`UPRCTR` =  S.`UPRCTR`,
# MAGIC T.`UMATNR` =  S.`UMATNR`,
# MAGIC T.`VARC_UACCT` =  S.`VARC_UACCT`,
# MAGIC T.`ACCAS` =  S.`ACCAS`,
# MAGIC T.`ACCASTY` =  S.`ACCASTY`,
# MAGIC T.`LSTAR` =  S.`LSTAR`,
# MAGIC T.`AUFNR` =  S.`AUFNR`,
# MAGIC T.`AUTYP` =  S.`AUTYP`,
# MAGIC T.`PS_PSP_PNR` =  S.`PS_PSP_PNR`,
# MAGIC T.`PS_POSID` =  S.`PS_POSID`,
# MAGIC T.`PS_PRJ_PNR` =  S.`PS_PRJ_PNR`,
# MAGIC T.`PS_PSPID` =  S.`PS_PSPID`,
# MAGIC T.`NPLNR` =  S.`NPLNR`,
# MAGIC T.`NPLNR_VORGN` =  S.`NPLNR_VORGN`,
# MAGIC T.`PRZNR` =  S.`PRZNR`,
# MAGIC T.`KSTRG` =  S.`KSTRG`,
# MAGIC T.`BEMOT` =  S.`BEMOT`,
# MAGIC T.`RSRCE` =  S.`RSRCE`,
# MAGIC T.`QMNUM` =  S.`QMNUM`,
# MAGIC T.`SERVICE_DOC_TYPE` =  S.`SERVICE_DOC_TYPE`,
# MAGIC T.`SERVICE_DOC_ID` =  S.`SERVICE_DOC_ID`,
# MAGIC T.`SERVICE_DOC_ITEM_ID` =  S.`SERVICE_DOC_ITEM_ID`,
# MAGIC T.`SERVICE_CONTRACT_TYPE` =  S.`SERVICE_CONTRACT_TYPE`,
# MAGIC T.`SERVICE_CONTRACT_ID` =  S.`SERVICE_CONTRACT_ID`,
# MAGIC T.`SERVICE_CONTRACT_ITEM_ID` =  S.`SERVICE_CONTRACT_ITEM_ID`,
# MAGIC T.`SOLUTION_ORDER_ID` =  S.`SOLUTION_ORDER_ID`,
# MAGIC T.`SOLUTION_ORDER_ITEM_ID` =  S.`SOLUTION_ORDER_ITEM_ID`,
# MAGIC T.`ERKRS` =  S.`ERKRS`,
# MAGIC T.`PACCAS` =  S.`PACCAS`,
# MAGIC T.`PACCASTY` =  S.`PACCASTY`,
# MAGIC T.`PLSTAR` =  S.`PLSTAR`,
# MAGIC T.`PAUFNR` =  S.`PAUFNR`,
# MAGIC T.`PAUTYP` =  S.`PAUTYP`,
# MAGIC T.`PPS_PSP_PNR` =  S.`PPS_PSP_PNR`,
# MAGIC T.`PPS_POSID` =  S.`PPS_POSID`,
# MAGIC T.`PPS_PRJ_PNR` =  S.`PPS_PRJ_PNR`,
# MAGIC T.`PPS_PSPID` =  S.`PPS_PSPID`,
# MAGIC T.`PKDAUF` =  S.`PKDAUF`,
# MAGIC T.`PKDPOS` =  S.`PKDPOS`,
# MAGIC T.`PPAOBJNR` =  S.`PPAOBJNR`,
# MAGIC T.`PNPLNR` =  S.`PNPLNR`,
# MAGIC T.`PNPLNR_VORGN` =  S.`PNPLNR_VORGN`,
# MAGIC T.`PPRZNR` =  S.`PPRZNR`,
# MAGIC T.`PKSTRG` =  S.`PKSTRG`,
# MAGIC T.`PSERVICE_DOC_TYPE` =  S.`PSERVICE_DOC_TYPE`,
# MAGIC T.`PSERVICE_DOC_ID` =  S.`PSERVICE_DOC_ID`,
# MAGIC T.`PSERVICE_DOC_ITEM_ID` =  S.`PSERVICE_DOC_ITEM_ID`,
# MAGIC T.`CO_ACCASTY_N1` =  S.`CO_ACCASTY_N1`,
# MAGIC T.`CO_ACCASTY_N2` =  S.`CO_ACCASTY_N2`,
# MAGIC T.`CO_ACCASTY_N3` =  S.`CO_ACCASTY_N3`,
# MAGIC T.`CO_ZLENR` =  S.`CO_ZLENR`,
# MAGIC T.`CO_BELNR` =  S.`CO_BELNR`,
# MAGIC T.`CO_BUZEI` =  S.`CO_BUZEI`,
# MAGIC T.`CO_BUZEI1` =  S.`CO_BUZEI1`,
# MAGIC T.`CO_BUZEI2` =  S.`CO_BUZEI2`,
# MAGIC T.`CO_BUZEI5` =  S.`CO_BUZEI5`,
# MAGIC T.`CO_BUZEI6` =  S.`CO_BUZEI6`,
# MAGIC T.`CO_BUZEI7` =  S.`CO_BUZEI7`,
# MAGIC T.`CO_REFBZ` =  S.`CO_REFBZ`,
# MAGIC T.`CO_REFBZ1` =  S.`CO_REFBZ1`,
# MAGIC T.`CO_REFBZ2` =  S.`CO_REFBZ2`,
# MAGIC T.`CO_REFBZ5` =  S.`CO_REFBZ5`,
# MAGIC T.`CO_REFBZ6` =  S.`CO_REFBZ6`,
# MAGIC T.`CO_REFBZ7` =  S.`CO_REFBZ7`,
# MAGIC T.`OVERTIMECAT` =  S.`OVERTIMECAT`,
# MAGIC T.`WORK_ITEM_ID` =  S.`WORK_ITEM_ID`,
# MAGIC T.`ARBID` =  S.`ARBID`,
# MAGIC T.`VORNR` =  S.`VORNR`,
# MAGIC T.`AUFPS` =  S.`AUFPS`,
# MAGIC T.`UVORN` =  S.`UVORN`,
# MAGIC T.`EQUNR` =  S.`EQUNR`,
# MAGIC T.`TPLNR` =  S.`TPLNR`,
# MAGIC T.`ISTRU` =  S.`ISTRU`,
# MAGIC T.`ILART` =  S.`ILART`,
# MAGIC T.`PLKNZ` =  S.`PLKNZ`,
# MAGIC T.`ARTPR` =  S.`ARTPR`,
# MAGIC T.`PRIOK` =  S.`PRIOK`,
# MAGIC T.`MAUFNR` =  S.`MAUFNR`,
# MAGIC T.`MATKL_MM` =  S.`MATKL_MM`,
# MAGIC T.`PAUFPS` =  S.`PAUFPS`,
# MAGIC T.`PLANNED_PARTS_WORK` =  S.`PLANNED_PARTS_WORK`,
# MAGIC T.`FKART` =  S.`FKART`,
# MAGIC T.`VKORG` =  S.`VKORG`,
# MAGIC T.`VTWEG` =  S.`VTWEG`,
# MAGIC T.`SPART` =  S.`SPART`,
# MAGIC T.`MATNR_COPA` =  S.`MATNR_COPA`,
# MAGIC T.`MATKL` =  S.`MATKL`,
# MAGIC T.`KDGRP` =  S.`KDGRP`,
# MAGIC T.`LAND1` =  S.`LAND1`,
# MAGIC T.`BRSCH` =  S.`BRSCH`,
# MAGIC T.`BZIRK` =  S.`BZIRK`,
# MAGIC T.`KUNRE` =  S.`KUNRE`,
# MAGIC T.`KUNWE` =  S.`KUNWE`,
# MAGIC T.`KONZS` =  S.`KONZS`,
# MAGIC T.`ACDOC_COPA_EEW_DUMMY_PA` =  S.`ACDOC_COPA_EEW_DUMMY_PA`,
# MAGIC T.`KMVKBU_PA` =  S.`KMVKBU_PA`,
# MAGIC T.`KMVKGR_PA` =  S.`KMVKGR_PA`,
# MAGIC T.`KUNNR_PA` =  S.`KUNNR_PA`,
# MAGIC T.`PAPH1_PA` =  S.`PAPH1_PA`,
# MAGIC T.`PAPH2_PA` =  S.`PAPH2_PA`,
# MAGIC T.`PAPH3_PA` =  S.`PAPH3_PA`,
# MAGIC T.`WW001_PA` =  S.`WW001_PA`,
# MAGIC T.`WW002_PA` =  S.`WW002_PA`,
# MAGIC T.`PRODH_PA` =  S.`PRODH_PA`,
# MAGIC T.`PAPH4_PA` =  S.`PAPH4_PA`,
# MAGIC T.`KMHI01_PA` =  S.`KMHI01_PA`,
# MAGIC T.`KMHI02_PA` =  S.`KMHI02_PA`,
# MAGIC T.`KMHI03_PA` =  S.`KMHI03_PA`,
# MAGIC T.`KTGRD_PA` =  S.`KTGRD_PA`,
# MAGIC T.`PAPH5_PA` =  S.`PAPH5_PA`,
# MAGIC T.`DUMMY_MRKT_SGMNT_EEW_PS` =  S.`DUMMY_MRKT_SGMNT_EEW_PS`,
# MAGIC T.`RE_BUKRS` =  S.`RE_BUKRS`,
# MAGIC T.`RE_ACCOUNT` =  S.`RE_ACCOUNT`,
# MAGIC T.`FIKRS` =  S.`FIKRS`,
# MAGIC T.`FIPEX` =  S.`FIPEX`,
# MAGIC T.`FISTL` =  S.`FISTL`,
# MAGIC T.`MEASURE` =  S.`MEASURE`,
# MAGIC T.`RFUND` =  S.`RFUND`,
# MAGIC T.`RGRANT_NBR` =  S.`RGRANT_NBR`,
# MAGIC T.`RBUDGET_PD` =  S.`RBUDGET_PD`,
# MAGIC T.`SFUND` =  S.`SFUND`,
# MAGIC T.`SGRANT_NBR` =  S.`SGRANT_NBR`,
# MAGIC T.`SBUDGET_PD` =  S.`SBUDGET_PD`,
# MAGIC T.`BDGT_ACCOUNT` =  S.`BDGT_ACCOUNT`,
# MAGIC T.`BDGT_ACCOUNT_COCODE` =  S.`BDGT_ACCOUNT_COCODE`,
# MAGIC T.`BDGT_CNSMPN_DATE` =  S.`BDGT_CNSMPN_DATE`,
# MAGIC T.`BDGT_CNSMPN_PERIOD` =  S.`BDGT_CNSMPN_PERIOD`,
# MAGIC T.`BDGT_CNSMPN_YEAR` =  S.`BDGT_CNSMPN_YEAR`,
# MAGIC T.`BDGT_RELEVANT` =  S.`BDGT_RELEVANT`,
# MAGIC T.`BDGT_CNSMPN_TYPE` =  S.`BDGT_CNSMPN_TYPE`,
# MAGIC T.`BDGT_CNSMPN_AMOUNT_TYPE` =  S.`BDGT_CNSMPN_AMOUNT_TYPE`,
# MAGIC T.`RSPONSORED_PROG` =  S.`RSPONSORED_PROG`,
# MAGIC T.`RSPONSORED_CLASS` =  S.`RSPONSORED_CLASS`,
# MAGIC T.`RBDGT_VLDTY_NBR` =  S.`RBDGT_VLDTY_NBR`,
# MAGIC T.`KBLNR` =  S.`KBLNR`,
# MAGIC T.`KBLPOS` =  S.`KBLPOS`,
# MAGIC T.`VNAME` =  S.`VNAME`,
# MAGIC T.`EGRUP` =  S.`EGRUP`,
# MAGIC T.`RECID` =  S.`RECID`,
# MAGIC T.`VPTNR` =  S.`VPTNR`,
# MAGIC T.`BTYPE` =  S.`BTYPE`,
# MAGIC T.`ETYPE` =  S.`ETYPE`,
# MAGIC T.`PRODPER` =  S.`PRODPER`,
# MAGIC T.`BILLM` =  S.`BILLM`,
# MAGIC T.`POM` =  S.`POM`,
# MAGIC T.`CBRUNID` =  S.`CBRUNID`,
# MAGIC T.`JVACTIVITY` =  S.`JVACTIVITY`,
# MAGIC T.`PVNAME` =  S.`PVNAME`,
# MAGIC T.`PEGRUP` =  S.`PEGRUP`,
# MAGIC T.`S_RECIND` =  S.`S_RECIND`,
# MAGIC T.`CBRACCT` =  S.`CBRACCT`,
# MAGIC T.`CBOBJNR` =  S.`CBOBJNR`,
# MAGIC T.`SWENR` =  S.`SWENR`,
# MAGIC T.`SGENR` =  S.`SGENR`,
# MAGIC T.`SGRNR` =  S.`SGRNR`,
# MAGIC T.`SMENR` =  S.`SMENR`,
# MAGIC T.`RECNNR` =  S.`RECNNR`,
# MAGIC T.`SNKSL` =  S.`SNKSL`,
# MAGIC T.`SEMPSL` =  S.`SEMPSL`,
# MAGIC T.`DABRZ` =  S.`DABRZ`,
# MAGIC T.`PSWENR` =  S.`PSWENR`,
# MAGIC T.`PSGENR` =  S.`PSGENR`,
# MAGIC T.`PSGRNR` =  S.`PSGRNR`,
# MAGIC T.`PSMENR` =  S.`PSMENR`,
# MAGIC T.`PRECNNR` =  S.`PRECNNR`,
# MAGIC T.`PSNKSL` =  S.`PSNKSL`,
# MAGIC T.`PSEMPSL` =  S.`PSEMPSL`,
# MAGIC T.`PDABRZ` =  S.`PDABRZ`,
# MAGIC T.`ACROBJTYPE` =  S.`ACROBJTYPE`,
# MAGIC T.`ACRLOGSYS` =  S.`ACRLOGSYS`,
# MAGIC T.`ACROBJ_ID` =  S.`ACROBJ_ID`,
# MAGIC T.`ACRSOBJ_ID` =  S.`ACRSOBJ_ID`,
# MAGIC T.`ACRITMTYPE` =  S.`ACRITMTYPE`,
# MAGIC T.`ACRVALDAT` =  S.`ACRVALDAT`,
# MAGIC T.`VALOBJTYPE` =  S.`VALOBJTYPE`,
# MAGIC T.`VALOBJ_ID` =  S.`VALOBJ_ID`,
# MAGIC T.`VALSOBJ_ID` =  S.`VALSOBJ_ID`,
# MAGIC T.`NETDT` =  S.`NETDT`,
# MAGIC T.`RISK_CLASS` =  S.`RISK_CLASS`,
# MAGIC T.`ACDOC_EEW_DUMMY` =  S.`ACDOC_EEW_DUMMY`,
# MAGIC T.`DUMMY_INCL_EEW_COBL` =  S.`DUMMY_INCL_EEW_COBL`,
# MAGIC T.`FUP_ACTION` =  S.`FUP_ACTION`,
# MAGIC T.`SDM_VERSION` =  S.`SDM_VERSION`,
# MAGIC T.`MIG_SOURCE` =  S.`MIG_SOURCE`,
# MAGIC T.`MIG_DOCLN` =  S.`MIG_DOCLN`,
# MAGIC T.`_DATAAGING` =  S.`_DATAAGING`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now(),
# MAGIC T.DataSource = S.DataSource
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `RCLNT`,
# MAGIC `RLDNR`,
# MAGIC `RBUKRS`,
# MAGIC `GJAHR`,
# MAGIC `BELNR`,
# MAGIC `DOCLN`,
# MAGIC `RYEAR`,
# MAGIC `DOCNR_LD`,
# MAGIC `RRCTY`,
# MAGIC `RMVCT`,
# MAGIC `VORGN`,
# MAGIC `VRGNG`,
# MAGIC `BTTYPE`,
# MAGIC `CBTTYPE`,
# MAGIC `AWTYP`,
# MAGIC `AWSYS`,
# MAGIC `AWORG`,
# MAGIC `AWREF`,
# MAGIC `AWITEM`,
# MAGIC `AWITGRP`,
# MAGIC `SUBTA`,
# MAGIC `XREVERSING`,
# MAGIC `XREVERSED`,
# MAGIC `XTRUEREV`,
# MAGIC `AWTYP_REV`,
# MAGIC `AWORG_REV`,
# MAGIC `AWREF_REV`,
# MAGIC `AWITEM_REV`,
# MAGIC `SUBTA_REV`,
# MAGIC `XSETTLING`,
# MAGIC `XSETTLED`,
# MAGIC `PREC_AWTYP`,
# MAGIC `PREC_AWSYS`,
# MAGIC `PREC_AWORG`,
# MAGIC `PREC_AWREF`,
# MAGIC `PREC_AWITEM`,
# MAGIC `PREC_SUBTA`,
# MAGIC `PREC_AWMULT`,
# MAGIC `PREC_BUKRS`,
# MAGIC `PREC_GJAHR`,
# MAGIC `PREC_BELNR`,
# MAGIC `PREC_DOCLN`,
# MAGIC `XSECONDARY`,
# MAGIC `CLOSING_RUN_ID`,
# MAGIC `ORGL_CHANGE`,
# MAGIC `SRC_AWTYP`,
# MAGIC `SRC_AWSYS`,
# MAGIC `SRC_AWORG`,
# MAGIC `SRC_AWREF`,
# MAGIC `SRC_AWITEM`,
# MAGIC `SRC_AWSUBIT`,
# MAGIC `XCOMMITMENT`,
# MAGIC `OBS_REASON`,
# MAGIC `RTCUR`,
# MAGIC `RWCUR`,
# MAGIC `RHCUR`,
# MAGIC `RKCUR`,
# MAGIC `ROCUR`,
# MAGIC `RVCUR`,
# MAGIC `RBCUR`,
# MAGIC `RCCUR`,
# MAGIC `RDCUR`,
# MAGIC `RECUR`,
# MAGIC `RFCUR`,
# MAGIC `RGCUR`,
# MAGIC `RCO_OCUR`,
# MAGIC `RGM_OCUR`,
# MAGIC `RUNIT`,
# MAGIC `RVUNIT`,
# MAGIC `RRUNIT`,
# MAGIC `RMSL_TYPE`,
# MAGIC `RIUNIT`,
# MAGIC `QUNIT1`,
# MAGIC `QUNIT2`,
# MAGIC `QUNIT3`,
# MAGIC `CO_MEINH`,
# MAGIC `RACCT`,
# MAGIC `RCNTR`,
# MAGIC `PRCTR`,
# MAGIC `RFAREA`,
# MAGIC `RBUSA`,
# MAGIC `KOKRS`,
# MAGIC `SEGMENT`,
# MAGIC `SCNTR`,
# MAGIC `PPRCTR`,
# MAGIC `SFAREA`,
# MAGIC `SBUSA`,
# MAGIC `RASSC`,
# MAGIC `PSEGMENT`,
# MAGIC `TSL`,
# MAGIC `WSL`,
# MAGIC `WSL2`,
# MAGIC `WSL3`,
# MAGIC `HSL`,
# MAGIC `KSL`,
# MAGIC `OSL`,
# MAGIC `VSL`,
# MAGIC `BSL`,
# MAGIC `CSL`,
# MAGIC `DSL`,
# MAGIC `ESL`,
# MAGIC `FSL`,
# MAGIC `GSL`,
# MAGIC `KFSL`,
# MAGIC `KFSL2`,
# MAGIC `KFSL3`,
# MAGIC `PSL`,
# MAGIC `PSL2`,
# MAGIC `PSL3`,
# MAGIC `PFSL`,
# MAGIC `PFSL2`,
# MAGIC `PFSL3`,
# MAGIC `CO_OSL`,
# MAGIC `GM_OSL`,
# MAGIC `HSLALT`,
# MAGIC `KSLALT`,
# MAGIC `OSLALT`,
# MAGIC `VSLALT`,
# MAGIC `BSLALT`,
# MAGIC `CSLALT`,
# MAGIC `DSLALT`,
# MAGIC `ESLALT`,
# MAGIC `FSLALT`,
# MAGIC `GSLALT`,
# MAGIC `HSLEXT`,
# MAGIC `KSLEXT`,
# MAGIC `OSLEXT`,
# MAGIC `VSLEXT`,
# MAGIC `BSLEXT`,
# MAGIC `CSLEXT`,
# MAGIC `DSLEXT`,
# MAGIC `ESLEXT`,
# MAGIC `FSLEXT`,
# MAGIC `GSLEXT`,
# MAGIC `HVKWRT`,
# MAGIC `MSL`,
# MAGIC `MFSL`,
# MAGIC `VMSL`,
# MAGIC `VMFSL`,
# MAGIC `RMSL`,
# MAGIC `QUANT1`,
# MAGIC `QUANT2`,
# MAGIC `QUANT3`,
# MAGIC `CO_MEGBTR`,
# MAGIC `CO_MEFBTR`,
# MAGIC `HSALK3`,
# MAGIC `KSALK3`,
# MAGIC `OSALK3`,
# MAGIC `VSALK3`,
# MAGIC `HSALKV`,
# MAGIC `KSALKV`,
# MAGIC `OSALKV`,
# MAGIC `VSALKV`,
# MAGIC `HPVPRS`,
# MAGIC `KPVPRS`,
# MAGIC `OPVPRS`,
# MAGIC `VPVPRS`,
# MAGIC `HSTPRS`,
# MAGIC `KSTPRS`,
# MAGIC `OSTPRS`,
# MAGIC `VSTPRS`,
# MAGIC `HVKSAL`,
# MAGIC `LBKUM`,
# MAGIC `DRCRK`,
# MAGIC `POPER`,
# MAGIC `PERIV`,
# MAGIC `FISCYEARPER`,
# MAGIC `BUDAT`,
# MAGIC `BLDAT`,
# MAGIC `BLART`,
# MAGIC `BUZEI`,
# MAGIC `ZUONR`,
# MAGIC `BSCHL`,
# MAGIC `BSTAT`,
# MAGIC `LINETYPE`,
# MAGIC `KTOSL`,
# MAGIC `SLALITTYPE`,
# MAGIC `XSPLITMOD`,
# MAGIC `USNAM`,
# MAGIC `TIMESTAMP`,
# MAGIC `EPRCTR`,
# MAGIC `RHOART`,
# MAGIC `GLACCOUNT_TYPE`,
# MAGIC `KTOPL`,
# MAGIC `LOKKT`,
# MAGIC `KTOP2`,
# MAGIC `REBZG`,
# MAGIC `REBZJ`,
# MAGIC `REBZZ`,
# MAGIC `REBZT`,
# MAGIC `RBEST`,
# MAGIC `EBELN_LOGSYS`,
# MAGIC `EBELN`,
# MAGIC `EBELP`,
# MAGIC `ZEKKN`,
# MAGIC `SGTXT`,
# MAGIC `KDAUF`,
# MAGIC `KDPOS`,
# MAGIC `MATNR`,
# MAGIC `WERKS`,
# MAGIC `LIFNR`,
# MAGIC `KUNNR`,
# MAGIC `FBUDA`,
# MAGIC `PEROP_BEG`,
# MAGIC `PEROP_END`,
# MAGIC `COCO_NUM`,
# MAGIC `WWERT`,
# MAGIC `PRCTR_DRVTN_SOURCE_TYPE`,
# MAGIC `KOART`,
# MAGIC `UMSKZ`,
# MAGIC `TAX_COUNTRY`,
# MAGIC `MWSKZ`,
# MAGIC `HBKID`,
# MAGIC `HKTID`,
# MAGIC `VALUT`,
# MAGIC `XOPVW`,
# MAGIC `AUGDT`,
# MAGIC `AUGBL`,
# MAGIC `AUGGJ`,
# MAGIC `AFABE`,
# MAGIC `ANLN1`,
# MAGIC `ANLN2`,
# MAGIC `BZDAT`,
# MAGIC `ANBWA`,
# MAGIC `MOVCAT`,
# MAGIC `DEPR_PERIOD`,
# MAGIC `ANLGR`,
# MAGIC `ANLGR2`,
# MAGIC `SETTLEMENT_RULE`,
# MAGIC `ANLKL`,
# MAGIC `KTOGR`,
# MAGIC `PANL1`,
# MAGIC `PANL2`,
# MAGIC `UBZDT_PN`,
# MAGIC `XVABG_PN`,
# MAGIC `PROZS_PN`,
# MAGIC `XMANPROPVAL_PN`,
# MAGIC `KALNR`,
# MAGIC `VPRSV`,
# MAGIC `MLAST`,
# MAGIC `KZBWS`,
# MAGIC `XOBEW`,
# MAGIC `SOBKZ`,
# MAGIC `VTSTAMP`,
# MAGIC `MAT_KDAUF`,
# MAGIC `MAT_KDPOS`,
# MAGIC `MAT_PSPNR`,
# MAGIC `MAT_PS_POSID`,
# MAGIC `MAT_LIFNR`,
# MAGIC `BWTAR`,
# MAGIC `BWKEY`,
# MAGIC `HPEINH`,
# MAGIC `KPEINH`,
# MAGIC `OPEINH`,
# MAGIC `VPEINH`,
# MAGIC `MLPTYP`,
# MAGIC `MLCATEG`,
# MAGIC `QSBVALT`,
# MAGIC `QSPROCESS`,
# MAGIC `PERART`,
# MAGIC `MLPOSNR`,
# MAGIC `INV_MOV_CATEG`,
# MAGIC `BUKRS_SENDER`,
# MAGIC `RACCT_SENDER`,
# MAGIC `ACCAS_SENDER`,
# MAGIC `ACCASTY_SENDER`,
# MAGIC `OBJNR`,
# MAGIC `HRKFT`,
# MAGIC `HKGRP`,
# MAGIC `PAROB1`,
# MAGIC `PAROBSRC`,
# MAGIC `USPOB`,
# MAGIC `CO_BELKZ`,
# MAGIC `CO_BEKNZ`,
# MAGIC `BELTP`,
# MAGIC `MUVFLG`,
# MAGIC `GKONT`,
# MAGIC `GKOAR`,
# MAGIC `ERLKZ`,
# MAGIC `PERNR`,
# MAGIC `PAOBJNR`,
# MAGIC `XPAOBJNR_CO_REL`,
# MAGIC `SCOPE`,
# MAGIC `LOGSYSO`,
# MAGIC `PBUKRS`,
# MAGIC `PSCOPE`,
# MAGIC `LOGSYSP`,
# MAGIC `BWSTRAT`,
# MAGIC `OBJNR_HK`,
# MAGIC `AUFNR_ORG`,
# MAGIC `UKOSTL`,
# MAGIC `ULSTAR`,
# MAGIC `UPRZNR`,
# MAGIC `UPRCTR`,
# MAGIC `UMATNR`,
# MAGIC `VARC_UACCT`,
# MAGIC `ACCAS`,
# MAGIC `ACCASTY`,
# MAGIC `LSTAR`,
# MAGIC `AUFNR`,
# MAGIC `AUTYP`,
# MAGIC `PS_PSP_PNR`,
# MAGIC `PS_POSID`,
# MAGIC `PS_PRJ_PNR`,
# MAGIC `PS_PSPID`,
# MAGIC `NPLNR`,
# MAGIC `NPLNR_VORGN`,
# MAGIC `PRZNR`,
# MAGIC `KSTRG`,
# MAGIC `BEMOT`,
# MAGIC `RSRCE`,
# MAGIC `QMNUM`,
# MAGIC `SERVICE_DOC_TYPE`,
# MAGIC `SERVICE_DOC_ID`,
# MAGIC `SERVICE_DOC_ITEM_ID`,
# MAGIC `SERVICE_CONTRACT_TYPE`,
# MAGIC `SERVICE_CONTRACT_ID`,
# MAGIC `SERVICE_CONTRACT_ITEM_ID`,
# MAGIC `SOLUTION_ORDER_ID`,
# MAGIC `SOLUTION_ORDER_ITEM_ID`,
# MAGIC `ERKRS`,
# MAGIC `PACCAS`,
# MAGIC `PACCASTY`,
# MAGIC `PLSTAR`,
# MAGIC `PAUFNR`,
# MAGIC `PAUTYP`,
# MAGIC `PPS_PSP_PNR`,
# MAGIC `PPS_POSID`,
# MAGIC `PPS_PRJ_PNR`,
# MAGIC `PPS_PSPID`,
# MAGIC `PKDAUF`,
# MAGIC `PKDPOS`,
# MAGIC `PPAOBJNR`,
# MAGIC `PNPLNR`,
# MAGIC `PNPLNR_VORGN`,
# MAGIC `PPRZNR`,
# MAGIC `PKSTRG`,
# MAGIC `PSERVICE_DOC_TYPE`,
# MAGIC `PSERVICE_DOC_ID`,
# MAGIC `PSERVICE_DOC_ITEM_ID`,
# MAGIC `CO_ACCASTY_N1`,
# MAGIC `CO_ACCASTY_N2`,
# MAGIC `CO_ACCASTY_N3`,
# MAGIC `CO_ZLENR`,
# MAGIC `CO_BELNR`,
# MAGIC `CO_BUZEI`,
# MAGIC `CO_BUZEI1`,
# MAGIC `CO_BUZEI2`,
# MAGIC `CO_BUZEI5`,
# MAGIC `CO_BUZEI6`,
# MAGIC `CO_BUZEI7`,
# MAGIC `CO_REFBZ`,
# MAGIC `CO_REFBZ1`,
# MAGIC `CO_REFBZ2`,
# MAGIC `CO_REFBZ5`,
# MAGIC `CO_REFBZ6`,
# MAGIC `CO_REFBZ7`,
# MAGIC `OVERTIMECAT`,
# MAGIC `WORK_ITEM_ID`,
# MAGIC `ARBID`,
# MAGIC `VORNR`,
# MAGIC `AUFPS`,
# MAGIC `UVORN`,
# MAGIC `EQUNR`,
# MAGIC `TPLNR`,
# MAGIC `ISTRU`,
# MAGIC `ILART`,
# MAGIC `PLKNZ`,
# MAGIC `ARTPR`,
# MAGIC `PRIOK`,
# MAGIC `MAUFNR`,
# MAGIC `MATKL_MM`,
# MAGIC `PAUFPS`,
# MAGIC `PLANNED_PARTS_WORK`,
# MAGIC `FKART`,
# MAGIC `VKORG`,
# MAGIC `VTWEG`,
# MAGIC `SPART`,
# MAGIC `MATNR_COPA`,
# MAGIC `MATKL`,
# MAGIC `KDGRP`,
# MAGIC `LAND1`,
# MAGIC `BRSCH`,
# MAGIC `BZIRK`,
# MAGIC `KUNRE`,
# MAGIC `KUNWE`,
# MAGIC `KONZS`,
# MAGIC `ACDOC_COPA_EEW_DUMMY_PA`,
# MAGIC `KMVKBU_PA`,
# MAGIC `KMVKGR_PA`,
# MAGIC `KUNNR_PA`,
# MAGIC `PAPH1_PA`,
# MAGIC `PAPH2_PA`,
# MAGIC `PAPH3_PA`,
# MAGIC `WW001_PA`,
# MAGIC `WW002_PA`,
# MAGIC `PRODH_PA`,
# MAGIC `PAPH4_PA`,
# MAGIC `KMHI01_PA`,
# MAGIC `KMHI02_PA`,
# MAGIC `KMHI03_PA`,
# MAGIC `KTGRD_PA`,
# MAGIC `PAPH5_PA`,
# MAGIC `DUMMY_MRKT_SGMNT_EEW_PS`,
# MAGIC `RE_BUKRS`,
# MAGIC `RE_ACCOUNT`,
# MAGIC `FIKRS`,
# MAGIC `FIPEX`,
# MAGIC `FISTL`,
# MAGIC `MEASURE`,
# MAGIC `RFUND`,
# MAGIC `RGRANT_NBR`,
# MAGIC `RBUDGET_PD`,
# MAGIC `SFUND`,
# MAGIC `SGRANT_NBR`,
# MAGIC `SBUDGET_PD`,
# MAGIC `BDGT_ACCOUNT`,
# MAGIC `BDGT_ACCOUNT_COCODE`,
# MAGIC `BDGT_CNSMPN_DATE`,
# MAGIC `BDGT_CNSMPN_PERIOD`,
# MAGIC `BDGT_CNSMPN_YEAR`,
# MAGIC `BDGT_RELEVANT`,
# MAGIC `BDGT_CNSMPN_TYPE`,
# MAGIC `BDGT_CNSMPN_AMOUNT_TYPE`,
# MAGIC `RSPONSORED_PROG`,
# MAGIC `RSPONSORED_CLASS`,
# MAGIC `RBDGT_VLDTY_NBR`,
# MAGIC `KBLNR`,
# MAGIC `KBLPOS`,
# MAGIC `VNAME`,
# MAGIC `EGRUP`,
# MAGIC `RECID`,
# MAGIC `VPTNR`,
# MAGIC `BTYPE`,
# MAGIC `ETYPE`,
# MAGIC `PRODPER`,
# MAGIC `BILLM`,
# MAGIC `POM`,
# MAGIC `CBRUNID`,
# MAGIC `JVACTIVITY`,
# MAGIC `PVNAME`,
# MAGIC `PEGRUP`,
# MAGIC `S_RECIND`,
# MAGIC `CBRACCT`,
# MAGIC `CBOBJNR`,
# MAGIC `SWENR`,
# MAGIC `SGENR`,
# MAGIC `SGRNR`,
# MAGIC `SMENR`,
# MAGIC `RECNNR`,
# MAGIC `SNKSL`,
# MAGIC `SEMPSL`,
# MAGIC `DABRZ`,
# MAGIC `PSWENR`,
# MAGIC `PSGENR`,
# MAGIC `PSGRNR`,
# MAGIC `PSMENR`,
# MAGIC `PRECNNR`,
# MAGIC `PSNKSL`,
# MAGIC `PSEMPSL`,
# MAGIC `PDABRZ`,
# MAGIC `ACROBJTYPE`,
# MAGIC `ACRLOGSYS`,
# MAGIC `ACROBJ_ID`,
# MAGIC `ACRSOBJ_ID`,
# MAGIC `ACRITMTYPE`,
# MAGIC `ACRVALDAT`,
# MAGIC `VALOBJTYPE`,
# MAGIC `VALOBJ_ID`,
# MAGIC `VALSOBJ_ID`,
# MAGIC `NETDT`,
# MAGIC `RISK_CLASS`,
# MAGIC `ACDOC_EEW_DUMMY`,
# MAGIC `DUMMY_INCL_EEW_COBL`,
# MAGIC `FUP_ACTION`,
# MAGIC `SDM_VERSION`,
# MAGIC `MIG_SOURCE`,
# MAGIC `MIG_DOCLN`,
# MAGIC `_DATAAGING`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC Updatedon,
# MAGIC DataSource
# MAGIC )
# MAGIC values
# MAGIC (
# MAGIC S.`RCLNT`,
# MAGIC S.`RLDNR`,
# MAGIC S.`RBUKRS`,
# MAGIC S.`GJAHR`,
# MAGIC S.`BELNR`,
# MAGIC S.`DOCLN`,
# MAGIC S.`RYEAR`,
# MAGIC S.`DOCNR_LD`,
# MAGIC S.`RRCTY`,
# MAGIC S.`RMVCT`,
# MAGIC S.`VORGN`,
# MAGIC S.`VRGNG`,
# MAGIC S.`BTTYPE`,
# MAGIC S.`CBTTYPE`,
# MAGIC S.`AWTYP`,
# MAGIC S.`AWSYS`,
# MAGIC S.`AWORG`,
# MAGIC S.`AWREF`,
# MAGIC S.`AWITEM`,
# MAGIC S.`AWITGRP`,
# MAGIC S.`SUBTA`,
# MAGIC S.`XREVERSING`,
# MAGIC S.`XREVERSED`,
# MAGIC S.`XTRUEREV`,
# MAGIC S.`AWTYP_REV`,
# MAGIC S.`AWORG_REV`,
# MAGIC S.`AWREF_REV`,
# MAGIC S.`AWITEM_REV`,
# MAGIC S.`SUBTA_REV`,
# MAGIC S.`XSETTLING`,
# MAGIC S.`XSETTLED`,
# MAGIC S.`PREC_AWTYP`,
# MAGIC S.`PREC_AWSYS`,
# MAGIC S.`PREC_AWORG`,
# MAGIC S.`PREC_AWREF`,
# MAGIC S.`PREC_AWITEM`,
# MAGIC S.`PREC_SUBTA`,
# MAGIC S.`PREC_AWMULT`,
# MAGIC S.`PREC_BUKRS`,
# MAGIC S.`PREC_GJAHR`,
# MAGIC S.`PREC_BELNR`,
# MAGIC S.`PREC_DOCLN`,
# MAGIC S.`XSECONDARY`,
# MAGIC S.`CLOSING_RUN_ID`,
# MAGIC S.`ORGL_CHANGE`,
# MAGIC S.`SRC_AWTYP`,
# MAGIC S.`SRC_AWSYS`,
# MAGIC S.`SRC_AWORG`,
# MAGIC S.`SRC_AWREF`,
# MAGIC S.`SRC_AWITEM`,
# MAGIC S.`SRC_AWSUBIT`,
# MAGIC S.`XCOMMITMENT`,
# MAGIC S.`OBS_REASON`,
# MAGIC S.`RTCUR`,
# MAGIC S.`RWCUR`,
# MAGIC S.`RHCUR`,
# MAGIC S.`RKCUR`,
# MAGIC S.`ROCUR`,
# MAGIC S.`RVCUR`,
# MAGIC S.`RBCUR`,
# MAGIC S.`RCCUR`,
# MAGIC S.`RDCUR`,
# MAGIC S.`RECUR`,
# MAGIC S.`RFCUR`,
# MAGIC S.`RGCUR`,
# MAGIC S.`RCO_OCUR`,
# MAGIC S.`RGM_OCUR`,
# MAGIC S.`RUNIT`,
# MAGIC S.`RVUNIT`,
# MAGIC S.`RRUNIT`,
# MAGIC S.`RMSL_TYPE`,
# MAGIC S.`RIUNIT`,
# MAGIC S.`QUNIT1`,
# MAGIC S.`QUNIT2`,
# MAGIC S.`QUNIT3`,
# MAGIC S.`CO_MEINH`,
# MAGIC S.`RACCT`,
# MAGIC S.`RCNTR`,
# MAGIC S.`PRCTR`,
# MAGIC S.`RFAREA`,
# MAGIC S.`RBUSA`,
# MAGIC S.`KOKRS`,
# MAGIC S.`SEGMENT`,
# MAGIC S.`SCNTR`,
# MAGIC S.`PPRCTR`,
# MAGIC S.`SFAREA`,
# MAGIC S.`SBUSA`,
# MAGIC S.`RASSC`,
# MAGIC S.`PSEGMENT`,
# MAGIC S.`TSL`,
# MAGIC S.`WSL`,
# MAGIC S.`WSL2`,
# MAGIC S.`WSL3`,
# MAGIC S.`HSL`,
# MAGIC S.`KSL`,
# MAGIC S.`OSL`,
# MAGIC S.`VSL`,
# MAGIC S.`BSL`,
# MAGIC S.`CSL`,
# MAGIC S.`DSL`,
# MAGIC S.`ESL`,
# MAGIC S.`FSL`,
# MAGIC S.`GSL`,
# MAGIC S.`KFSL`,
# MAGIC S.`KFSL2`,
# MAGIC S.`KFSL3`,
# MAGIC S.`PSL`,
# MAGIC S.`PSL2`,
# MAGIC S.`PSL3`,
# MAGIC S.`PFSL`,
# MAGIC S.`PFSL2`,
# MAGIC S.`PFSL3`,
# MAGIC S.`CO_OSL`,
# MAGIC S.`GM_OSL`,
# MAGIC S.`HSLALT`,
# MAGIC S.`KSLALT`,
# MAGIC S.`OSLALT`,
# MAGIC S.`VSLALT`,
# MAGIC S.`BSLALT`,
# MAGIC S.`CSLALT`,
# MAGIC S.`DSLALT`,
# MAGIC S.`ESLALT`,
# MAGIC S.`FSLALT`,
# MAGIC S.`GSLALT`,
# MAGIC S.`HSLEXT`,
# MAGIC S.`KSLEXT`,
# MAGIC S.`OSLEXT`,
# MAGIC S.`VSLEXT`,
# MAGIC S.`BSLEXT`,
# MAGIC S.`CSLEXT`,
# MAGIC S.`DSLEXT`,
# MAGIC S.`ESLEXT`,
# MAGIC S.`FSLEXT`,
# MAGIC S.`GSLEXT`,
# MAGIC S.`HVKWRT`,
# MAGIC S.`MSL`,
# MAGIC S.`MFSL`,
# MAGIC S.`VMSL`,
# MAGIC S.`VMFSL`,
# MAGIC S.`RMSL`,
# MAGIC S.`QUANT1`,
# MAGIC S.`QUANT2`,
# MAGIC S.`QUANT3`,
# MAGIC S.`CO_MEGBTR`,
# MAGIC S.`CO_MEFBTR`,
# MAGIC S.`HSALK3`,
# MAGIC S.`KSALK3`,
# MAGIC S.`OSALK3`,
# MAGIC S.`VSALK3`,
# MAGIC S.`HSALKV`,
# MAGIC S.`KSALKV`,
# MAGIC S.`OSALKV`,
# MAGIC S.`VSALKV`,
# MAGIC S.`HPVPRS`,
# MAGIC S.`KPVPRS`,
# MAGIC S.`OPVPRS`,
# MAGIC S.`VPVPRS`,
# MAGIC S.`HSTPRS`,
# MAGIC S.`KSTPRS`,
# MAGIC S.`OSTPRS`,
# MAGIC S.`VSTPRS`,
# MAGIC S.`HVKSAL`,
# MAGIC S.`LBKUM`,
# MAGIC S.`DRCRK`,
# MAGIC S.`POPER`,
# MAGIC S.`PERIV`,
# MAGIC S.`FISCYEARPER`,
# MAGIC S.`BUDAT`,
# MAGIC S.`BLDAT`,
# MAGIC S.`BLART`,
# MAGIC S.`BUZEI`,
# MAGIC S.`ZUONR`,
# MAGIC S.`BSCHL`,
# MAGIC S.`BSTAT`,
# MAGIC S.`LINETYPE`,
# MAGIC S.`KTOSL`,
# MAGIC S.`SLALITTYPE`,
# MAGIC S.`XSPLITMOD`,
# MAGIC S.`USNAM`,
# MAGIC S.`TIMESTAMP`,
# MAGIC S.`EPRCTR`,
# MAGIC S.`RHOART`,
# MAGIC S.`GLACCOUNT_TYPE`,
# MAGIC S.`KTOPL`,
# MAGIC S.`LOKKT`,
# MAGIC S.`KTOP2`,
# MAGIC S.`REBZG`,
# MAGIC S.`REBZJ`,
# MAGIC S.`REBZZ`,
# MAGIC S.`REBZT`,
# MAGIC S.`RBEST`,
# MAGIC S.`EBELN_LOGSYS`,
# MAGIC S.`EBELN`,
# MAGIC S.`EBELP`,
# MAGIC S.`ZEKKN`,
# MAGIC S.`SGTXT`,
# MAGIC S.`KDAUF`,
# MAGIC S.`KDPOS`,
# MAGIC S.`MATNR`,
# MAGIC S.`WERKS`,
# MAGIC S.`LIFNR`,
# MAGIC S.`KUNNR`,
# MAGIC S.`FBUDA`,
# MAGIC S.`PEROP_BEG`,
# MAGIC S.`PEROP_END`,
# MAGIC S.`COCO_NUM`,
# MAGIC S.`WWERT`,
# MAGIC S.`PRCTR_DRVTN_SOURCE_TYPE`,
# MAGIC S.`KOART`,
# MAGIC S.`UMSKZ`,
# MAGIC S.`TAX_COUNTRY`,
# MAGIC S.`MWSKZ`,
# MAGIC S.`HBKID`,
# MAGIC S.`HKTID`,
# MAGIC S.`VALUT`,
# MAGIC S.`XOPVW`,
# MAGIC S.`AUGDT`,
# MAGIC S.`AUGBL`,
# MAGIC S.`AUGGJ`,
# MAGIC S.`AFABE`,
# MAGIC S.`ANLN1`,
# MAGIC S.`ANLN2`,
# MAGIC S.`BZDAT`,
# MAGIC S.`ANBWA`,
# MAGIC S.`MOVCAT`,
# MAGIC S.`DEPR_PERIOD`,
# MAGIC S.`ANLGR`,
# MAGIC S.`ANLGR2`,
# MAGIC S.`SETTLEMENT_RULE`,
# MAGIC S.`ANLKL`,
# MAGIC S.`KTOGR`,
# MAGIC S.`PANL1`,
# MAGIC S.`PANL2`,
# MAGIC S.`UBZDT_PN`,
# MAGIC S.`XVABG_PN`,
# MAGIC S.`PROZS_PN`,
# MAGIC S.`XMANPROPVAL_PN`,
# MAGIC S.`KALNR`,
# MAGIC S.`VPRSV`,
# MAGIC S.`MLAST`,
# MAGIC S.`KZBWS`,
# MAGIC S.`XOBEW`,
# MAGIC S.`SOBKZ`,
# MAGIC S.`VTSTAMP`,
# MAGIC S.`MAT_KDAUF`,
# MAGIC S.`MAT_KDPOS`,
# MAGIC S.`MAT_PSPNR`,
# MAGIC S.`MAT_PS_POSID`,
# MAGIC S.`MAT_LIFNR`,
# MAGIC S.`BWTAR`,
# MAGIC S.`BWKEY`,
# MAGIC S.`HPEINH`,
# MAGIC S.`KPEINH`,
# MAGIC S.`OPEINH`,
# MAGIC S.`VPEINH`,
# MAGIC S.`MLPTYP`,
# MAGIC S.`MLCATEG`,
# MAGIC S.`QSBVALT`,
# MAGIC S.`QSPROCESS`,
# MAGIC S.`PERART`,
# MAGIC S.`MLPOSNR`,
# MAGIC S.`INV_MOV_CATEG`,
# MAGIC S.`BUKRS_SENDER`,
# MAGIC S.`RACCT_SENDER`,
# MAGIC S.`ACCAS_SENDER`,
# MAGIC S.`ACCASTY_SENDER`,
# MAGIC S.`OBJNR`,
# MAGIC S.`HRKFT`,
# MAGIC S.`HKGRP`,
# MAGIC S.`PAROB1`,
# MAGIC S.`PAROBSRC`,
# MAGIC S.`USPOB`,
# MAGIC S.`CO_BELKZ`,
# MAGIC S.`CO_BEKNZ`,
# MAGIC S.`BELTP`,
# MAGIC S.`MUVFLG`,
# MAGIC S.`GKONT`,
# MAGIC S.`GKOAR`,
# MAGIC S.`ERLKZ`,
# MAGIC S.`PERNR`,
# MAGIC S.`PAOBJNR`,
# MAGIC S.`XPAOBJNR_CO_REL`,
# MAGIC S.`SCOPE`,
# MAGIC S.`LOGSYSO`,
# MAGIC S.`PBUKRS`,
# MAGIC S.`PSCOPE`,
# MAGIC S.`LOGSYSP`,
# MAGIC S.`BWSTRAT`,
# MAGIC S.`OBJNR_HK`,
# MAGIC S.`AUFNR_ORG`,
# MAGIC S.`UKOSTL`,
# MAGIC S.`ULSTAR`,
# MAGIC S.`UPRZNR`,
# MAGIC S.`UPRCTR`,
# MAGIC S.`UMATNR`,
# MAGIC S.`VARC_UACCT`,
# MAGIC S.`ACCAS`,
# MAGIC S.`ACCASTY`,
# MAGIC S.`LSTAR`,
# MAGIC S.`AUFNR`,
# MAGIC S.`AUTYP`,
# MAGIC S.`PS_PSP_PNR`,
# MAGIC S.`PS_POSID`,
# MAGIC S.`PS_PRJ_PNR`,
# MAGIC S.`PS_PSPID`,
# MAGIC S.`NPLNR`,
# MAGIC S.`NPLNR_VORGN`,
# MAGIC S.`PRZNR`,
# MAGIC S.`KSTRG`,
# MAGIC S.`BEMOT`,
# MAGIC S.`RSRCE`,
# MAGIC S.`QMNUM`,
# MAGIC S.`SERVICE_DOC_TYPE`,
# MAGIC S.`SERVICE_DOC_ID`,
# MAGIC S.`SERVICE_DOC_ITEM_ID`,
# MAGIC S.`SERVICE_CONTRACT_TYPE`,
# MAGIC S.`SERVICE_CONTRACT_ID`,
# MAGIC S.`SERVICE_CONTRACT_ITEM_ID`,
# MAGIC S.`SOLUTION_ORDER_ID`,
# MAGIC S.`SOLUTION_ORDER_ITEM_ID`,
# MAGIC S.`ERKRS`,
# MAGIC S.`PACCAS`,
# MAGIC S.`PACCASTY`,
# MAGIC S.`PLSTAR`,
# MAGIC S.`PAUFNR`,
# MAGIC S.`PAUTYP`,
# MAGIC S.`PPS_PSP_PNR`,
# MAGIC S.`PPS_POSID`,
# MAGIC S.`PPS_PRJ_PNR`,
# MAGIC S.`PPS_PSPID`,
# MAGIC S.`PKDAUF`,
# MAGIC S.`PKDPOS`,
# MAGIC S.`PPAOBJNR`,
# MAGIC S.`PNPLNR`,
# MAGIC S.`PNPLNR_VORGN`,
# MAGIC S.`PPRZNR`,
# MAGIC S.`PKSTRG`,
# MAGIC S.`PSERVICE_DOC_TYPE`,
# MAGIC S.`PSERVICE_DOC_ID`,
# MAGIC S.`PSERVICE_DOC_ITEM_ID`,
# MAGIC S.`CO_ACCASTY_N1`,
# MAGIC S.`CO_ACCASTY_N2`,
# MAGIC S.`CO_ACCASTY_N3`,
# MAGIC S.`CO_ZLENR`,
# MAGIC S.`CO_BELNR`,
# MAGIC S.`CO_BUZEI`,
# MAGIC S.`CO_BUZEI1`,
# MAGIC S.`CO_BUZEI2`,
# MAGIC S.`CO_BUZEI5`,
# MAGIC S.`CO_BUZEI6`,
# MAGIC S.`CO_BUZEI7`,
# MAGIC S.`CO_REFBZ`,
# MAGIC S.`CO_REFBZ1`,
# MAGIC S.`CO_REFBZ2`,
# MAGIC S.`CO_REFBZ5`,
# MAGIC S.`CO_REFBZ6`,
# MAGIC S.`CO_REFBZ7`,
# MAGIC S.`OVERTIMECAT`,
# MAGIC S.`WORK_ITEM_ID`,
# MAGIC S.`ARBID`,
# MAGIC S.`VORNR`,
# MAGIC S.`AUFPS`,
# MAGIC S.`UVORN`,
# MAGIC S.`EQUNR`,
# MAGIC S.`TPLNR`,
# MAGIC S.`ISTRU`,
# MAGIC S.`ILART`,
# MAGIC S.`PLKNZ`,
# MAGIC S.`ARTPR`,
# MAGIC S.`PRIOK`,
# MAGIC S.`MAUFNR`,
# MAGIC S.`MATKL_MM`,
# MAGIC S.`PAUFPS`,
# MAGIC S.`PLANNED_PARTS_WORK`,
# MAGIC S.`FKART`,
# MAGIC S.`VKORG`,
# MAGIC S.`VTWEG`,
# MAGIC S.`SPART`,
# MAGIC S.`MATNR_COPA`,
# MAGIC S.`MATKL`,
# MAGIC S.`KDGRP`,
# MAGIC S.`LAND1`,
# MAGIC S.`BRSCH`,
# MAGIC S.`BZIRK`,
# MAGIC S.`KUNRE`,
# MAGIC S.`KUNWE`,
# MAGIC S.`KONZS`,
# MAGIC S.`ACDOC_COPA_EEW_DUMMY_PA`,
# MAGIC S.`KMVKBU_PA`,
# MAGIC S.`KMVKGR_PA`,
# MAGIC S.`KUNNR_PA`,
# MAGIC S.`PAPH1_PA`,
# MAGIC S.`PAPH2_PA`,
# MAGIC S.`PAPH3_PA`,
# MAGIC S.`WW001_PA`,
# MAGIC S.`WW002_PA`,
# MAGIC S.`PRODH_PA`,
# MAGIC S.`PAPH4_PA`,
# MAGIC S.`KMHI01_PA`,
# MAGIC S.`KMHI02_PA`,
# MAGIC S.`KMHI03_PA`,
# MAGIC S.`KTGRD_PA`,
# MAGIC S.`PAPH5_PA`,
# MAGIC S.`DUMMY_MRKT_SGMNT_EEW_PS`,
# MAGIC S.`RE_BUKRS`,
# MAGIC S.`RE_ACCOUNT`,
# MAGIC S.`FIKRS`,
# MAGIC S.`FIPEX`,
# MAGIC S.`FISTL`,
# MAGIC S.`MEASURE`,
# MAGIC S.`RFUND`,
# MAGIC S.`RGRANT_NBR`,
# MAGIC S.`RBUDGET_PD`,
# MAGIC S.`SFUND`,
# MAGIC S.`SGRANT_NBR`,
# MAGIC S.`SBUDGET_PD`,
# MAGIC S.`BDGT_ACCOUNT`,
# MAGIC S.`BDGT_ACCOUNT_COCODE`,
# MAGIC S.`BDGT_CNSMPN_DATE`,
# MAGIC S.`BDGT_CNSMPN_PERIOD`,
# MAGIC S.`BDGT_CNSMPN_YEAR`,
# MAGIC S.`BDGT_RELEVANT`,
# MAGIC S.`BDGT_CNSMPN_TYPE`,
# MAGIC S.`BDGT_CNSMPN_AMOUNT_TYPE`,
# MAGIC S.`RSPONSORED_PROG`,
# MAGIC S.`RSPONSORED_CLASS`,
# MAGIC S.`RBDGT_VLDTY_NBR`,
# MAGIC S.`KBLNR`,
# MAGIC S.`KBLPOS`,
# MAGIC S.`VNAME`,
# MAGIC S.`EGRUP`,
# MAGIC S.`RECID`,
# MAGIC S.`VPTNR`,
# MAGIC S.`BTYPE`,
# MAGIC S.`ETYPE`,
# MAGIC S.`PRODPER`,
# MAGIC S.`BILLM`,
# MAGIC S.`POM`,
# MAGIC S.`CBRUNID`,
# MAGIC S.`JVACTIVITY`,
# MAGIC S.`PVNAME`,
# MAGIC S.`PEGRUP`,
# MAGIC S.`S_RECIND`,
# MAGIC S.`CBRACCT`,
# MAGIC S.`CBOBJNR`,
# MAGIC S.`SWENR`,
# MAGIC S.`SGENR`,
# MAGIC S.`SGRNR`,
# MAGIC S.`SMENR`,
# MAGIC S.`RECNNR`,
# MAGIC S.`SNKSL`,
# MAGIC S.`SEMPSL`,
# MAGIC S.`DABRZ`,
# MAGIC S.`PSWENR`,
# MAGIC S.`PSGENR`,
# MAGIC S.`PSGRNR`,
# MAGIC S.`PSMENR`,
# MAGIC S.`PRECNNR`,
# MAGIC S.`PSNKSL`,
# MAGIC S.`PSEMPSL`,
# MAGIC S.`PDABRZ`,
# MAGIC S.`ACROBJTYPE`,
# MAGIC S.`ACRLOGSYS`,
# MAGIC S.`ACROBJ_ID`,
# MAGIC S.`ACRSOBJ_ID`,
# MAGIC S.`ACRITMTYPE`,
# MAGIC S.`ACRVALDAT`,
# MAGIC S.`VALOBJTYPE`,
# MAGIC S.`VALOBJ_ID`,
# MAGIC S.`VALSOBJ_ID`,
# MAGIC S.`NETDT`,
# MAGIC S.`RISK_CLASS`,
# MAGIC S.`ACDOC_EEW_DUMMY`,
# MAGIC S.`DUMMY_INCL_EEW_COBL`,
# MAGIC S.`FUP_ACTION`,
# MAGIC S.`SDM_VERSION`,
# MAGIC S.`MIG_SOURCE`,
# MAGIC S.`MIG_DOCLN`,
# MAGIC S.`_DATAAGING`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


