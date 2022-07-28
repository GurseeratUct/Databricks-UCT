# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

schema = StructType([ \
  StructField("DI_SEQUENCE_NUMBER",IntegerType(),True),\
  StructField("DI_OPERATION_TYPE",StringType(),True),\
  StructField("MANDT",IntegerType(),True),\
  StructField("LIFNR",StringType(),True),\
  StructField("LAND1",StringType(),True),\
  StructField("NAME1",StringType(),True),\
  StructField("NAME2",StringType(),True),\
  StructField("NAME3",StringType(),True),\
  StructField("NAME4",StringType(),True),\
  StructField("ORT01",StringType(),True),\
  StructField("ORT02",StringType(),True),\
  StructField("PFACH",StringType(),True),\
  StructField("PSTL2",StringType(),True),\
  StructField("PSTLZ",StringType(),True),\
  StructField("REGIO",StringType(),True),\
  StructField("SORTL",StringType(),True),\
  StructField("STRAS",StringType(),True),\
  StructField("ADRNR",IntegerType(),True),\
  StructField("MCOD1",StringType(),True),\
  StructField("MCOD2",StringType(),True),\
  StructField("MCOD3",StringType(),True),\
  StructField("ANRED",StringType(),True),\
  StructField("BAHNS",StringType(),True),\
  StructField("BBBNR",IntegerType(),True),\
  StructField("BBSNR",IntegerType(),True),\
  StructField("BEGRU",StringType(),True),\
  StructField("BRSCH",StringType(),True),\
  StructField("BUBKZ",IntegerType(),True),\
  StructField("DATLT",StringType(),True),\
  StructField("DTAMS",StringType(),True),\
  StructField("DTAWS",StringType(),True),\
  StructField("ERDAT",StringType(),True),\
  StructField("ERNAM",StringType(),True),\
  StructField("ESRNR",StringType(),True),\
  StructField("KONZS",StringType(),True),\
  StructField("KTOKK",StringType(),True),\
  StructField("KUNNR",StringType(),True),\
  StructField("LNRZA",StringType(),True),\
  StructField("LOEVM",StringType(),True),\
  StructField("SPERR",StringType(),True),\
  StructField("SPERM",StringType(),True),\
  StructField("SPRAS",StringType(),True),\
  StructField("STCD1",StringType(),True),\
  StructField("STCD2",StringType(),True),\
  StructField("STKZA",StringType(),True),\
  StructField("STKZU",StringType(),True),\
  StructField("TELBX",StringType(),True),\
  StructField("TELF1",StringType(),True),\
  StructField("TELF2",StringType(),True),\
  StructField("TELFX",StringType(),True),\
  StructField("TELTX",StringType(),True),\
  StructField("TELX1",StringType(),True),\
  StructField("XCPDK",StringType(),True),\
  StructField("XZEMP",StringType(),True),\
  StructField("VBUND",StringType(),True),\
  StructField("FISKN",StringType(),True),\
  StructField("STCEG",StringType(),True),\
  StructField("STKZN",StringType(),True),\
  StructField("SPERQ",StringType(),True),\
  StructField("GBORT",StringType(),True),\
  StructField("GBDAT",StringType(),True),\
  StructField("SEXKZ",StringType(),True),\
  StructField("KRAUS",StringType(),True),\
  StructField("REVDB",StringType(),True),\
  StructField("QSSYS",StringType(),True),\
  StructField("KTOCK",StringType(),True),\
  StructField("PFORT",StringType(),True),\
  StructField("WERKS",StringType(),True),\
  StructField("LTSNA",StringType(),True),\
  StructField("WERKR",StringType(),True),\
  StructField("PLKAL",StringType(),True),\
  StructField("DUEFL",StringType(),True),\
  StructField("TXJCD",StringType(),True),\
  StructField("SPERZ",StringType(),True),\
  StructField("SCACD",StringType(),True),\
  StructField("SFRGR",StringType(),True),\
  StructField("LZONE",StringType(),True),\
  StructField("XLFZA",StringType(),True),\
  StructField("DLGRP",StringType(),True),\
  StructField("FITYP",StringType(),True),\
  StructField("STCDT",StringType(),True),\
  StructField("REGSS",StringType(),True),\
  StructField("ACTSS",StringType(),True),\
  StructField("STCD3",StringType(),True),\
  StructField("STCD4",StringType(),True),\
  StructField("STCD5",StringType(),True),\
  StructField("STCD6",StringType(),True),\
  StructField("IPISP",StringType(),True),\
  StructField("TAXBS",IntegerType(),True),\
  StructField("PROFS",StringType(),True),\
  StructField("STGDL",StringType(),True),\
  StructField("EMNFR",StringType(),True),\
  StructField("LFURL",StringType(),True),\
  StructField("J_1KFREPRE",StringType(),True),\
  StructField("J_1KFTBUS",StringType(),True),\
  StructField("J_1KFTIND",StringType(),True),\
  StructField("CONFS",StringType(),True),\
  StructField("UPDAT",StringType(),True),\
  StructField("UPTIM",TimestampType(),True),\
  StructField("NODEL",StringType(),True),\
  StructField("QSSYSDAT",StringType(),True),\
  StructField("PODKZB",StringType(),True),\
  StructField("FISKU",StringType(),True),\
  StructField("STENR",StringType(),True),\
  StructField("CARRIER_CONF",StringType(),True),\
  StructField("MIN_COMP",StringType(),True),\
  StructField("TERM_LI",StringType(),True),\
  StructField("CRC_NUM",StringType(),True),\
  StructField("CVP_XBLCK",StringType(),True),\
  StructField("WEORA",StringType(),True),\
  StructField("RG",StringType(),True),\
  StructField("EXP",StringType(),True),\
  StructField("UF",StringType(),True),\
  StructField("RGDATE",StringType(),True),\
  StructField("RIC",IntegerType(),True),\
  StructField("RNE",StringType(),True),\
  StructField("RNEDATE",StringType(),True),\
  StructField("CNAE",StringType(),True),\
  StructField("LEGALNAT",IntegerType(),True),\
  StructField("CRTN",StringType(),True),\
  StructField("ICMSTAXPAY",StringType(),True),\
  StructField("INDTYP",StringType(),True),\
  StructField("TDT",StringType(),True),\
  StructField("COMSIZE",StringType(),True),\
  StructField("DECREGPC",StringType(),True),\
  StructField("ALLOWANCE_TYPE",StringType(),True),\
  StructField("LFA1_EEW_SUPP",StringType(),True),\
  StructField("J_SC_CAPITAL",DoubleType(),True),\
  StructField("J_SC_CURRENCY",StringType(),True),\
  StructField("ALC",StringType(),True),\
  StructField("PMT_OFFICE",StringType(),True),\
  StructField("PPA_RELEVANT",StringType(),True),\
  StructField("SAM_UE_ID",StringType(),True),\
  StructField("PSOFG",StringType(),True),\
  StructField("PSOIS",StringType(),True),\
  StructField("PSON1",StringType(),True),\
  StructField("PSON2",StringType(),True),\
  StructField("PSON3",StringType(),True),\
  StructField("PSOVN",StringType(),True),\
  StructField("PSOTL",StringType(),True),\
  StructField("PSOHS",StringType(),True),\
  StructField("PSOST",StringType(),True),\
  StructField("BORGR_DATUN",StringType(),True),\
  StructField("BORGR_YEAUN",StringType(),True),\
  StructField("AU_CARRYING_ENT",StringType(),True),\
  StructField("AU_IND_UNDER_18",StringType(),True),\
  StructField("AU_PAYMENT_NOT_EXCEED_75",StringType(),True),\
  StructField("AU_WHOLLY_INP_TAXED",StringType(),True),\
  StructField("AU_PARTNER_WITHOUT_GAIN",StringType(),True),\
  StructField("AU_NOT_ENTITLED_ABN",StringType(),True),\
  StructField("AU_PAYMENT_EXEMPT",StringType(),True),\
  StructField("AU_PRIVATE_HOBBY",StringType(),True),\
  StructField("AU_DOMESTIC_NATURE",StringType(),True),\
  StructField("ADDR2_STREET",StringType(),True),\
  StructField("ADDR2_HOUSE_NUM",StringType(),True),\
  StructField("ADDR2_POST",StringType(),True),\
  StructField("ADDR2_CITY",StringType(),True),\
  StructField("ADDR2_COUNTRY",StringType(),True),\
  StructField("CATEG",StringType(),True),\
  StructField("PARTNER_NAME",StringType(),True),\
  StructField("PARTNER_UTR",StringType(),True),\
  StructField("STATUS",StringType(),True),\
  StructField("VFNUM",StringType(),True),\
  StructField("VFNID",StringType(),True),\
  StructField("CRN",StringType(),True),\
  StructField("FR_OCCUPATION",StringType(),True),\
  StructField("J_1IEXCD",StringType(),True),\
  StructField("J_1IEXRN",StringType(),True),\
  StructField("J_1IEXRG",StringType(),True),\
  StructField("J_1IEXDI",StringType(),True),\
  StructField("J_1IEXCO",StringType(),True),\
  StructField("J_1ICSTNO",StringType(),True),\
  StructField("J_1ILSTNO",StringType(),True),\
  StructField("J_1IPANNO",StringType(),True),\
  StructField("J_1IEXCIVE",StringType(),True),\
  StructField("J_1ISSIST",StringType(),True),\
  StructField("J_1IVTYP",StringType(),True),\
  StructField("J_1IVENCRE",StringType(),True),\
  StructField("AEDAT",StringType(),True),\
  StructField("USNAM",StringType(),True),\
  StructField("J_1ISERN",StringType(),True),\
  StructField("J_1IPANREF",StringType(),True),\
  StructField("J_1IPANVALDT",StringType(),True),\
  StructField("J_1I_CUSTOMS",StringType(),True),\
  StructField("J_1IDEDREF",StringType(),True),\
  StructField("VEN_CLASS",StringType(),True),\
  StructField("ENTPUB",StringType(),True),\
  StructField("ESCRIT",StringType(),True),\
  StructField("DVALSS",StringType(),True),\
  StructField("FRMCSS",StringType(),True),\
  StructField("CODCAE",IntegerType(),True),\
  StructField("AUSDIV",StringType(),True),\
  StructField("TRANSPORT_CHAIN",StringType(),True),\
  StructField("STAGING_TIME",IntegerType(),True),\
  StructField("SCHEDULING_TYPE",StringType(),True),\
  StructField("SUBMI_RELEVANT",StringType(),True),\
  StructField("ZZAGILE_CUS",StringType(),True),\
  StructField("ODQ_CHANGEMODE",StringType(),True),\
  StructField("ODQ_ENTITYCNTR",IntegerType(),True),\
  StructField("LandingFileTimeStamp",StringType(),True),\
                    ])

# COMMAND ----------

table_name = 'LFA1'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter","^") \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
                            .withColumn("ERDAT", to_date(regexp_replace(df_add_column.ERDAT,'\.','-'))) \
                            .withColumn("GBDAT", to_date(regexp_replace(df_add_column.GBDAT,'\.','-'))) \
                            .withColumn("REVDB", to_date(regexp_replace(df_add_column.REVDB,'\.','-'))) \
                            .withColumn("UPDAT", to_date(regexp_replace(df_add_column.UPDAT,'\.','-'))) \
                            .withColumn("RGDATE", to_date(regexp_replace(df_add_column.RGDATE,'\.','-'))) \
                            .withColumn("RNEDATE", to_date(regexp_replace(df_add_column.RNEDATE,'\.','-'))) \
                            .withColumn("BORGR_DATUN", to_date(regexp_replace(df_add_column.BORGR_DATUN,'\.','-'))) \
                            .withColumn("AEDAT", to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
                            .withColumn("J_1IPANVALDT", to_date(regexp_replace(df_add_column.J_1IPANVALDT,'\.','-'))) \
                            .withColumn("DVALSS", to_date(regexp_replace(df_add_column.DVALSS,'\.','-'))) \
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
# MAGIC MERGE INTO S42.LFA1 as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,LIFNR ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_LFA1 where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and 
# MAGIC T.LIFNR = S.LIFNR
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`DI_SEQUENCE_NUMBER` = S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` = S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` = S.`MANDT`,
# MAGIC T.`LIFNR` = S.`LIFNR`,
# MAGIC T.`LAND1` = S.`LAND1`,
# MAGIC T.`NAME1` = S.`NAME1`,
# MAGIC T.`NAME2` = S.`NAME2`,
# MAGIC T.`NAME3` = S.`NAME3`,
# MAGIC T.`NAME4` = S.`NAME4`,
# MAGIC T.`ORT01` = S.`ORT01`,
# MAGIC T.`ORT02` = S.`ORT02`,
# MAGIC T.`PFACH` = S.`PFACH`,
# MAGIC T.`PSTL2` = S.`PSTL2`,
# MAGIC T.`PSTLZ` = S.`PSTLZ`,
# MAGIC T.`REGIO` = S.`REGIO`,
# MAGIC T.`SORTL` = S.`SORTL`,
# MAGIC T.`STRAS` = S.`STRAS`,
# MAGIC T.`ADRNR` = S.`ADRNR`,
# MAGIC T.`MCOD1` = S.`MCOD1`,
# MAGIC T.`MCOD2` = S.`MCOD2`,
# MAGIC T.`MCOD3` = S.`MCOD3`,
# MAGIC T.`ANRED` = S.`ANRED`,
# MAGIC T.`BAHNS` = S.`BAHNS`,
# MAGIC T.`BBBNR` = S.`BBBNR`,
# MAGIC T.`BBSNR` = S.`BBSNR`,
# MAGIC T.`BEGRU` = S.`BEGRU`,
# MAGIC T.`BRSCH` = S.`BRSCH`,
# MAGIC T.`BUBKZ` = S.`BUBKZ`,
# MAGIC T.`DATLT` = S.`DATLT`,
# MAGIC T.`DTAMS` = S.`DTAMS`,
# MAGIC T.`DTAWS` = S.`DTAWS`,
# MAGIC T.`ERDAT` = S.`ERDAT`,
# MAGIC T.`ERNAM` = S.`ERNAM`,
# MAGIC T.`ESRNR` = S.`ESRNR`,
# MAGIC T.`KONZS` = S.`KONZS`,
# MAGIC T.`KTOKK` = S.`KTOKK`,
# MAGIC T.`KUNNR` = S.`KUNNR`,
# MAGIC T.`LNRZA` = S.`LNRZA`,
# MAGIC T.`LOEVM` = S.`LOEVM`,
# MAGIC T.`SPERR` = S.`SPERR`,
# MAGIC T.`SPERM` = S.`SPERM`,
# MAGIC T.`SPRAS` = S.`SPRAS`,
# MAGIC T.`STCD1` = S.`STCD1`,
# MAGIC T.`STCD2` = S.`STCD2`,
# MAGIC T.`STKZA` = S.`STKZA`,
# MAGIC T.`STKZU` = S.`STKZU`,
# MAGIC T.`TELBX` = S.`TELBX`,
# MAGIC T.`TELF1` = S.`TELF1`,
# MAGIC T.`TELF2` = S.`TELF2`,
# MAGIC T.`TELFX` = S.`TELFX`,
# MAGIC T.`TELTX` = S.`TELTX`,
# MAGIC T.`TELX1` = S.`TELX1`,
# MAGIC T.`XCPDK` = S.`XCPDK`,
# MAGIC T.`XZEMP` = S.`XZEMP`,
# MAGIC T.`VBUND` = S.`VBUND`,
# MAGIC T.`FISKN` = S.`FISKN`,
# MAGIC T.`STCEG` = S.`STCEG`,
# MAGIC T.`STKZN` = S.`STKZN`,
# MAGIC T.`SPERQ` = S.`SPERQ`,
# MAGIC T.`GBORT` = S.`GBORT`,
# MAGIC T.`GBDAT` = S.`GBDAT`,
# MAGIC T.`SEXKZ` = S.`SEXKZ`,
# MAGIC T.`KRAUS` = S.`KRAUS`,
# MAGIC T.`REVDB` = S.`REVDB`,
# MAGIC T.`QSSYS` = S.`QSSYS`,
# MAGIC T.`KTOCK` = S.`KTOCK`,
# MAGIC T.`PFORT` = S.`PFORT`,
# MAGIC T.`WERKS` = S.`WERKS`,
# MAGIC T.`LTSNA` = S.`LTSNA`,
# MAGIC T.`WERKR` = S.`WERKR`,
# MAGIC T.`PLKAL` = S.`PLKAL`,
# MAGIC T.`DUEFL` = S.`DUEFL`,
# MAGIC T.`TXJCD` = S.`TXJCD`,
# MAGIC T.`SPERZ` = S.`SPERZ`,
# MAGIC T.`SCACD` = S.`SCACD`,
# MAGIC T.`SFRGR` = S.`SFRGR`,
# MAGIC T.`LZONE` = S.`LZONE`,
# MAGIC T.`XLFZA` = S.`XLFZA`,
# MAGIC T.`DLGRP` = S.`DLGRP`,
# MAGIC T.`FITYP` = S.`FITYP`,
# MAGIC T.`STCDT` = S.`STCDT`,
# MAGIC T.`REGSS` = S.`REGSS`,
# MAGIC T.`ACTSS` = S.`ACTSS`,
# MAGIC T.`STCD3` = S.`STCD3`,
# MAGIC T.`STCD4` = S.`STCD4`,
# MAGIC T.`STCD5` = S.`STCD5`,
# MAGIC T.`STCD6` = S.`STCD6`,
# MAGIC T.`IPISP` = S.`IPISP`,
# MAGIC T.`TAXBS` = S.`TAXBS`,
# MAGIC T.`PROFS` = S.`PROFS`,
# MAGIC T.`STGDL` = S.`STGDL`,
# MAGIC T.`EMNFR` = S.`EMNFR`,
# MAGIC T.`LFURL` = S.`LFURL`,
# MAGIC T.`J_1KFREPRE` = S.`J_1KFREPRE`,
# MAGIC T.`J_1KFTBUS` = S.`J_1KFTBUS`,
# MAGIC T.`J_1KFTIND` = S.`J_1KFTIND`,
# MAGIC T.`CONFS` = S.`CONFS`,
# MAGIC T.`UPDAT` = S.`UPDAT`,
# MAGIC T.`UPTIM` = S.`UPTIM`,
# MAGIC T.`NODEL` = S.`NODEL`,
# MAGIC T.`QSSYSDAT` = S.`QSSYSDAT`,
# MAGIC T.`PODKZB` = S.`PODKZB`,
# MAGIC T.`FISKU` = S.`FISKU`,
# MAGIC T.`STENR` = S.`STENR`,
# MAGIC T.`CARRIER_CONF` = S.`CARRIER_CONF`,
# MAGIC T.`MIN_COMP` = S.`MIN_COMP`,
# MAGIC T.`TERM_LI` = S.`TERM_LI`,
# MAGIC T.`CRC_NUM` = S.`CRC_NUM`,
# MAGIC T.`CVP_XBLCK` = S.`CVP_XBLCK`,
# MAGIC T.`WEORA` = S.`WEORA`,
# MAGIC T.`RG` = S.`RG`,
# MAGIC T.`EXP` = S.`EXP`,
# MAGIC T.`UF` = S.`UF`,
# MAGIC T.`RGDATE` = S.`RGDATE`,
# MAGIC T.`RIC` = S.`RIC`,
# MAGIC T.`RNE` = S.`RNE`,
# MAGIC T.`RNEDATE` = S.`RNEDATE`,
# MAGIC T.`CNAE` = S.`CNAE`,
# MAGIC T.`LEGALNAT` = S.`LEGALNAT`,
# MAGIC T.`CRTN` = S.`CRTN`,
# MAGIC T.`ICMSTAXPAY` = S.`ICMSTAXPAY`,
# MAGIC T.`INDTYP` = S.`INDTYP`,
# MAGIC T.`TDT` = S.`TDT`,
# MAGIC T.`COMSIZE` = S.`COMSIZE`,
# MAGIC T.`DECREGPC` = S.`DECREGPC`,
# MAGIC T.`ALLOWANCE_TYPE` = S.`ALLOWANCE_TYPE`,
# MAGIC T.`LFA1_EEW_SUPP` = S.`LFA1_EEW_SUPP`,
# MAGIC T.`J_SC_CAPITAL` = S.`J_SC_CAPITAL`,
# MAGIC T.`J_SC_CURRENCY` = S.`J_SC_CURRENCY`,
# MAGIC T.`ALC` = S.`ALC`,
# MAGIC T.`PMT_OFFICE` = S.`PMT_OFFICE`,
# MAGIC T.`PPA_RELEVANT` = S.`PPA_RELEVANT`,
# MAGIC T.`SAM_UE_ID` = S.`SAM_UE_ID`,
# MAGIC T.`PSOFG` = S.`PSOFG`,
# MAGIC T.`PSOIS` = S.`PSOIS`,
# MAGIC T.`PSON1` = S.`PSON1`,
# MAGIC T.`PSON2` = S.`PSON2`,
# MAGIC T.`PSON3` = S.`PSON3`,
# MAGIC T.`PSOVN` = S.`PSOVN`,
# MAGIC T.`PSOTL` = S.`PSOTL`,
# MAGIC T.`PSOHS` = S.`PSOHS`,
# MAGIC T.`PSOST` = S.`PSOST`,
# MAGIC T.`BORGR_DATUN` = S.`BORGR_DATUN`,
# MAGIC T.`BORGR_YEAUN` = S.`BORGR_YEAUN`,
# MAGIC T.`AU_CARRYING_ENT` = S.`AU_CARRYING_ENT`,
# MAGIC T.`AU_IND_UNDER_18` = S.`AU_IND_UNDER_18`,
# MAGIC T.`AU_PAYMENT_NOT_EXCEED_75` = S.`AU_PAYMENT_NOT_EXCEED_75`,
# MAGIC T.`AU_WHOLLY_INP_TAXED` = S.`AU_WHOLLY_INP_TAXED`,
# MAGIC T.`AU_PARTNER_WITHOUT_GAIN` = S.`AU_PARTNER_WITHOUT_GAIN`,
# MAGIC T.`AU_NOT_ENTITLED_ABN` = S.`AU_NOT_ENTITLED_ABN`,
# MAGIC T.`AU_PAYMENT_EXEMPT` = S.`AU_PAYMENT_EXEMPT`,
# MAGIC T.`AU_PRIVATE_HOBBY` = S.`AU_PRIVATE_HOBBY`,
# MAGIC T.`AU_DOMESTIC_NATURE` = S.`AU_DOMESTIC_NATURE`,
# MAGIC T.`ADDR2_STREET` = S.`ADDR2_STREET`,
# MAGIC T.`ADDR2_HOUSE_NUM` = S.`ADDR2_HOUSE_NUM`,
# MAGIC T.`ADDR2_POST` = S.`ADDR2_POST`,
# MAGIC T.`ADDR2_CITY` = S.`ADDR2_CITY`,
# MAGIC T.`ADDR2_COUNTRY` = S.`ADDR2_COUNTRY`,
# MAGIC T.`CATEG` = S.`CATEG`,
# MAGIC T.`PARTNER_NAME` = S.`PARTNER_NAME`,
# MAGIC T.`PARTNER_UTR` = S.`PARTNER_UTR`,
# MAGIC T.`STATUS` = S.`STATUS`,
# MAGIC T.`VFNUM` = S.`VFNUM`,
# MAGIC T.`VFNID` = S.`VFNID`,
# MAGIC T.`CRN` = S.`CRN`,
# MAGIC T.`FR_OCCUPATION` = S.`FR_OCCUPATION`,
# MAGIC T.`J_1IEXCD` = S.`J_1IEXCD`,
# MAGIC T.`J_1IEXRN` = S.`J_1IEXRN`,
# MAGIC T.`J_1IEXRG` = S.`J_1IEXRG`,
# MAGIC T.`J_1IEXDI` = S.`J_1IEXDI`,
# MAGIC T.`J_1IEXCO` = S.`J_1IEXCO`,
# MAGIC T.`J_1ICSTNO` = S.`J_1ICSTNO`,
# MAGIC T.`J_1ILSTNO` = S.`J_1ILSTNO`,
# MAGIC T.`J_1IPANNO` = S.`J_1IPANNO`,
# MAGIC T.`J_1IEXCIVE` = S.`J_1IEXCIVE`,
# MAGIC T.`J_1ISSIST` = S.`J_1ISSIST`,
# MAGIC T.`J_1IVTYP` = S.`J_1IVTYP`,
# MAGIC T.`J_1IVENCRE` = S.`J_1IVENCRE`,
# MAGIC T.`AEDAT` = S.`AEDAT`,
# MAGIC T.`USNAM` = S.`USNAM`,
# MAGIC T.`J_1ISERN` = S.`J_1ISERN`,
# MAGIC T.`J_1IPANREF` = S.`J_1IPANREF`,
# MAGIC T.`J_1IPANVALDT` = S.`J_1IPANVALDT`,
# MAGIC T.`J_1I_CUSTOMS` = S.`J_1I_CUSTOMS`,
# MAGIC T.`J_1IDEDREF` = S.`J_1IDEDREF`,
# MAGIC T.`VEN_CLASS` = S.`VEN_CLASS`,
# MAGIC T.`ENTPUB` = S.`ENTPUB`,
# MAGIC T.`ESCRIT` = S.`ESCRIT`,
# MAGIC T.`DVALSS` = S.`DVALSS`,
# MAGIC T.`FRMCSS` = S.`FRMCSS`,
# MAGIC T.`CODCAE` = S.`CODCAE`,
# MAGIC T.`AUSDIV` = S.`AUSDIV`,
# MAGIC T.`TRANSPORT_CHAIN` = S.`TRANSPORT_CHAIN`,
# MAGIC T.`STAGING_TIME` = S.`STAGING_TIME`,
# MAGIC T.`SCHEDULING_TYPE` = S.`SCHEDULING_TYPE`,
# MAGIC T.`SUBMI_RELEVANT` = S.`SUBMI_RELEVANT`,
# MAGIC T.`ZZAGILE_CUS` = S.`ZZAGILE_CUS`,
# MAGIC T.`ODQ_CHANGEMODE` = S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` = S.`ODQ_ENTITYCNTR`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `LIFNR`,
# MAGIC `LAND1`,
# MAGIC `NAME1`,
# MAGIC `NAME2`,
# MAGIC `NAME3`,
# MAGIC `NAME4`,
# MAGIC `ORT01`,
# MAGIC `ORT02`,
# MAGIC `PFACH`,
# MAGIC `PSTL2`,
# MAGIC `PSTLZ`,
# MAGIC `REGIO`,
# MAGIC `SORTL`,
# MAGIC `STRAS`,
# MAGIC `ADRNR`,
# MAGIC `MCOD1`,
# MAGIC `MCOD2`,
# MAGIC `MCOD3`,
# MAGIC `ANRED`,
# MAGIC `BAHNS`,
# MAGIC `BBBNR`,
# MAGIC `BBSNR`,
# MAGIC `BEGRU`,
# MAGIC `BRSCH`,
# MAGIC `BUBKZ`,
# MAGIC `DATLT`,
# MAGIC `DTAMS`,
# MAGIC `DTAWS`,
# MAGIC `ERDAT`,
# MAGIC `ERNAM`,
# MAGIC `ESRNR`,
# MAGIC `KONZS`,
# MAGIC `KTOKK`,
# MAGIC `KUNNR`,
# MAGIC `LNRZA`,
# MAGIC `LOEVM`,
# MAGIC `SPERR`,
# MAGIC `SPERM`,
# MAGIC `SPRAS`,
# MAGIC `STCD1`,
# MAGIC `STCD2`,
# MAGIC `STKZA`,
# MAGIC `STKZU`,
# MAGIC `TELBX`,
# MAGIC `TELF1`,
# MAGIC `TELF2`,
# MAGIC `TELFX`,
# MAGIC `TELTX`,
# MAGIC `TELX1`,
# MAGIC `XCPDK`,
# MAGIC `XZEMP`,
# MAGIC `VBUND`,
# MAGIC `FISKN`,
# MAGIC `STCEG`,
# MAGIC `STKZN`,
# MAGIC `SPERQ`,
# MAGIC `GBORT`,
# MAGIC `GBDAT`,
# MAGIC `SEXKZ`,
# MAGIC `KRAUS`,
# MAGIC `REVDB`,
# MAGIC `QSSYS`,
# MAGIC `KTOCK`,
# MAGIC `PFORT`,
# MAGIC `WERKS`,
# MAGIC `LTSNA`,
# MAGIC `WERKR`,
# MAGIC `PLKAL`,
# MAGIC `DUEFL`,
# MAGIC `TXJCD`,
# MAGIC `SPERZ`,
# MAGIC `SCACD`,
# MAGIC `SFRGR`,
# MAGIC `LZONE`,
# MAGIC `XLFZA`,
# MAGIC `DLGRP`,
# MAGIC `FITYP`,
# MAGIC `STCDT`,
# MAGIC `REGSS`,
# MAGIC `ACTSS`,
# MAGIC `STCD3`,
# MAGIC `STCD4`,
# MAGIC `STCD5`,
# MAGIC `STCD6`,
# MAGIC `IPISP`,
# MAGIC `TAXBS`,
# MAGIC `PROFS`,
# MAGIC `STGDL`,
# MAGIC `EMNFR`,
# MAGIC `LFURL`,
# MAGIC `J_1KFREPRE`,
# MAGIC `J_1KFTBUS`,
# MAGIC `J_1KFTIND`,
# MAGIC `CONFS`,
# MAGIC `UPDAT`,
# MAGIC `UPTIM`,
# MAGIC `NODEL`,
# MAGIC `QSSYSDAT`,
# MAGIC `PODKZB`,
# MAGIC `FISKU`,
# MAGIC `STENR`,
# MAGIC `CARRIER_CONF`,
# MAGIC `MIN_COMP`,
# MAGIC `TERM_LI`,
# MAGIC `CRC_NUM`,
# MAGIC `CVP_XBLCK`,
# MAGIC `WEORA`,
# MAGIC `RG`,
# MAGIC `EXP`,
# MAGIC `UF`,
# MAGIC `RGDATE`,
# MAGIC `RIC`,
# MAGIC `RNE`,
# MAGIC `RNEDATE`,
# MAGIC `CNAE`,
# MAGIC `LEGALNAT`,
# MAGIC `CRTN`,
# MAGIC `ICMSTAXPAY`,
# MAGIC `INDTYP`,
# MAGIC `TDT`,
# MAGIC `COMSIZE`,
# MAGIC `DECREGPC`,
# MAGIC `ALLOWANCE_TYPE`,
# MAGIC `LFA1_EEW_SUPP`,
# MAGIC `J_SC_CAPITAL`,
# MAGIC `J_SC_CURRENCY`,
# MAGIC `ALC`,
# MAGIC `PMT_OFFICE`,
# MAGIC `PPA_RELEVANT`,
# MAGIC `SAM_UE_ID`,
# MAGIC `PSOFG`,
# MAGIC `PSOIS`,
# MAGIC `PSON1`,
# MAGIC `PSON2`,
# MAGIC `PSON3`,
# MAGIC `PSOVN`,
# MAGIC `PSOTL`,
# MAGIC `PSOHS`,
# MAGIC `PSOST`,
# MAGIC `BORGR_DATUN`,
# MAGIC `BORGR_YEAUN`,
# MAGIC `AU_CARRYING_ENT`,
# MAGIC `AU_IND_UNDER_18`,
# MAGIC `AU_PAYMENT_NOT_EXCEED_75`,
# MAGIC `AU_WHOLLY_INP_TAXED`,
# MAGIC `AU_PARTNER_WITHOUT_GAIN`,
# MAGIC `AU_NOT_ENTITLED_ABN`,
# MAGIC `AU_PAYMENT_EXEMPT`,
# MAGIC `AU_PRIVATE_HOBBY`,
# MAGIC `AU_DOMESTIC_NATURE`,
# MAGIC `ADDR2_STREET`,
# MAGIC `ADDR2_HOUSE_NUM`,
# MAGIC `ADDR2_POST`,
# MAGIC `ADDR2_CITY`,
# MAGIC `ADDR2_COUNTRY`,
# MAGIC `CATEG`,
# MAGIC `PARTNER_NAME`,
# MAGIC `PARTNER_UTR`,
# MAGIC `STATUS`,
# MAGIC `VFNUM`,
# MAGIC `VFNID`,
# MAGIC `CRN`,
# MAGIC `FR_OCCUPATION`,
# MAGIC `J_1IEXCD`,
# MAGIC `J_1IEXRN`,
# MAGIC `J_1IEXRG`,
# MAGIC `J_1IEXDI`,
# MAGIC `J_1IEXCO`,
# MAGIC `J_1ICSTNO`,
# MAGIC `J_1ILSTNO`,
# MAGIC `J_1IPANNO`,
# MAGIC `J_1IEXCIVE`,
# MAGIC `J_1ISSIST`,
# MAGIC `J_1IVTYP`,
# MAGIC `J_1IVENCRE`,
# MAGIC `AEDAT`,
# MAGIC `USNAM`,
# MAGIC `J_1ISERN`,
# MAGIC `J_1IPANREF`,
# MAGIC `J_1IPANVALDT`,
# MAGIC `J_1I_CUSTOMS`,
# MAGIC `J_1IDEDREF`,
# MAGIC `VEN_CLASS`,
# MAGIC `ENTPUB`,
# MAGIC `ESCRIT`,
# MAGIC `DVALSS`,
# MAGIC `FRMCSS`,
# MAGIC `CODCAE`,
# MAGIC `AUSDIV`,
# MAGIC `TRANSPORT_CHAIN`,
# MAGIC `STAGING_TIME`,
# MAGIC `SCHEDULING_TYPE`,
# MAGIC `SUBMI_RELEVANT`,
# MAGIC `ZZAGILE_CUS`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`DI_SEQUENCE_NUMBER`,
# MAGIC S.`DI_OPERATION_TYPE`,
# MAGIC S.`MANDT`,
# MAGIC S.`LIFNR`,
# MAGIC S.`LAND1`,
# MAGIC S.`NAME1`,
# MAGIC S.`NAME2`,
# MAGIC S.`NAME3`,
# MAGIC S.`NAME4`,
# MAGIC S.`ORT01`,
# MAGIC S.`ORT02`,
# MAGIC S.`PFACH`,
# MAGIC S.`PSTL2`,
# MAGIC S.`PSTLZ`,
# MAGIC S.`REGIO`,
# MAGIC S.`SORTL`,
# MAGIC S.`STRAS`,
# MAGIC S.`ADRNR`,
# MAGIC S.`MCOD1`,
# MAGIC S.`MCOD2`,
# MAGIC S.`MCOD3`,
# MAGIC S.`ANRED`,
# MAGIC S.`BAHNS`,
# MAGIC S.`BBBNR`,
# MAGIC S.`BBSNR`,
# MAGIC S.`BEGRU`,
# MAGIC S.`BRSCH`,
# MAGIC S.`BUBKZ`,
# MAGIC S.`DATLT`,
# MAGIC S.`DTAMS`,
# MAGIC S.`DTAWS`,
# MAGIC S.`ERDAT`,
# MAGIC S.`ERNAM`,
# MAGIC S.`ESRNR`,
# MAGIC S.`KONZS`,
# MAGIC S.`KTOKK`,
# MAGIC S.`KUNNR`,
# MAGIC S.`LNRZA`,
# MAGIC S.`LOEVM`,
# MAGIC S.`SPERR`,
# MAGIC S.`SPERM`,
# MAGIC S.`SPRAS`,
# MAGIC S.`STCD1`,
# MAGIC S.`STCD2`,
# MAGIC S.`STKZA`,
# MAGIC S.`STKZU`,
# MAGIC S.`TELBX`,
# MAGIC S.`TELF1`,
# MAGIC S.`TELF2`,
# MAGIC S.`TELFX`,
# MAGIC S.`TELTX`,
# MAGIC S.`TELX1`,
# MAGIC S.`XCPDK`,
# MAGIC S.`XZEMP`,
# MAGIC S.`VBUND`,
# MAGIC S.`FISKN`,
# MAGIC S.`STCEG`,
# MAGIC S.`STKZN`,
# MAGIC S.`SPERQ`,
# MAGIC S.`GBORT`,
# MAGIC S.`GBDAT`,
# MAGIC S.`SEXKZ`,
# MAGIC S.`KRAUS`,
# MAGIC S.`REVDB`,
# MAGIC S.`QSSYS`,
# MAGIC S.`KTOCK`,
# MAGIC S.`PFORT`,
# MAGIC S.`WERKS`,
# MAGIC S.`LTSNA`,
# MAGIC S.`WERKR`,
# MAGIC S.`PLKAL`,
# MAGIC S.`DUEFL`,
# MAGIC S.`TXJCD`,
# MAGIC S.`SPERZ`,
# MAGIC S.`SCACD`,
# MAGIC S.`SFRGR`,
# MAGIC S.`LZONE`,
# MAGIC S.`XLFZA`,
# MAGIC S.`DLGRP`,
# MAGIC S.`FITYP`,
# MAGIC S.`STCDT`,
# MAGIC S.`REGSS`,
# MAGIC S.`ACTSS`,
# MAGIC S.`STCD3`,
# MAGIC S.`STCD4`,
# MAGIC S.`STCD5`,
# MAGIC S.`STCD6`,
# MAGIC S.`IPISP`,
# MAGIC S.`TAXBS`,
# MAGIC S.`PROFS`,
# MAGIC S.`STGDL`,
# MAGIC S.`EMNFR`,
# MAGIC S.`LFURL`,
# MAGIC S.`J_1KFREPRE`,
# MAGIC S.`J_1KFTBUS`,
# MAGIC S.`J_1KFTIND`,
# MAGIC S.`CONFS`,
# MAGIC S.`UPDAT`,
# MAGIC S.`UPTIM`,
# MAGIC S.`NODEL`,
# MAGIC S.`QSSYSDAT`,
# MAGIC S.`PODKZB`,
# MAGIC S.`FISKU`,
# MAGIC S.`STENR`,
# MAGIC S.`CARRIER_CONF`,
# MAGIC S.`MIN_COMP`,
# MAGIC S.`TERM_LI`,
# MAGIC S.`CRC_NUM`,
# MAGIC S.`CVP_XBLCK`,
# MAGIC S.`WEORA`,
# MAGIC S.`RG`,
# MAGIC S.`EXP`,
# MAGIC S.`UF`,
# MAGIC S.`RGDATE`,
# MAGIC S.`RIC`,
# MAGIC S.`RNE`,
# MAGIC S.`RNEDATE`,
# MAGIC S.`CNAE`,
# MAGIC S.`LEGALNAT`,
# MAGIC S.`CRTN`,
# MAGIC S.`ICMSTAXPAY`,
# MAGIC S.`INDTYP`,
# MAGIC S.`TDT`,
# MAGIC S.`COMSIZE`,
# MAGIC S.`DECREGPC`,
# MAGIC S.`ALLOWANCE_TYPE`,
# MAGIC S.`LFA1_EEW_SUPP`,
# MAGIC S.`J_SC_CAPITAL`,
# MAGIC S.`J_SC_CURRENCY`,
# MAGIC S.`ALC`,
# MAGIC S.`PMT_OFFICE`,
# MAGIC S.`PPA_RELEVANT`,
# MAGIC S.`SAM_UE_ID`,
# MAGIC S.`PSOFG`,
# MAGIC S.`PSOIS`,
# MAGIC S.`PSON1`,
# MAGIC S.`PSON2`,
# MAGIC S.`PSON3`,
# MAGIC S.`PSOVN`,
# MAGIC S.`PSOTL`,
# MAGIC S.`PSOHS`,
# MAGIC S.`PSOST`,
# MAGIC S.`BORGR_DATUN`,
# MAGIC S.`BORGR_YEAUN`,
# MAGIC S.`AU_CARRYING_ENT`,
# MAGIC S.`AU_IND_UNDER_18`,
# MAGIC S.`AU_PAYMENT_NOT_EXCEED_75`,
# MAGIC S.`AU_WHOLLY_INP_TAXED`,
# MAGIC S.`AU_PARTNER_WITHOUT_GAIN`,
# MAGIC S.`AU_NOT_ENTITLED_ABN`,
# MAGIC S.`AU_PAYMENT_EXEMPT`,
# MAGIC S.`AU_PRIVATE_HOBBY`,
# MAGIC S.`AU_DOMESTIC_NATURE`,
# MAGIC S.`ADDR2_STREET`,
# MAGIC S.`ADDR2_HOUSE_NUM`,
# MAGIC S.`ADDR2_POST`,
# MAGIC S.`ADDR2_CITY`,
# MAGIC S.`ADDR2_COUNTRY`,
# MAGIC S.`CATEG`,
# MAGIC S.`PARTNER_NAME`,
# MAGIC S.`PARTNER_UTR`,
# MAGIC S.`STATUS`,
# MAGIC S.`VFNUM`,
# MAGIC S.`VFNID`,
# MAGIC S.`CRN`,
# MAGIC S.`FR_OCCUPATION`,
# MAGIC S.`J_1IEXCD`,
# MAGIC S.`J_1IEXRN`,
# MAGIC S.`J_1IEXRG`,
# MAGIC S.`J_1IEXDI`,
# MAGIC S.`J_1IEXCO`,
# MAGIC S.`J_1ICSTNO`,
# MAGIC S.`J_1ILSTNO`,
# MAGIC S.`J_1IPANNO`,
# MAGIC S.`J_1IEXCIVE`,
# MAGIC S.`J_1ISSIST`,
# MAGIC S.`J_1IVTYP`,
# MAGIC S.`J_1IVENCRE`,
# MAGIC S.`AEDAT`,
# MAGIC S.`USNAM`,
# MAGIC S.`J_1ISERN`,
# MAGIC S.`J_1IPANREF`,
# MAGIC S.`J_1IPANVALDT`,
# MAGIC S.`J_1I_CUSTOMS`,
# MAGIC S.`J_1IDEDREF`,
# MAGIC S.`VEN_CLASS`,
# MAGIC S.`ENTPUB`,
# MAGIC S.`ESCRIT`,
# MAGIC S.`DVALSS`,
# MAGIC S.`FRMCSS`,
# MAGIC S.`CODCAE`,
# MAGIC S.`AUSDIV`,
# MAGIC S.`TRANSPORT_CHAIN`,
# MAGIC S.`STAGING_TIME`,
# MAGIC S.`SCHEDULING_TYPE`,
# MAGIC S.`SUBMI_RELEVANT`,
# MAGIC S.`ZZAGILE_CUS`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


