# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'KNA1'
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
StructField('KUNNR',StringType(),True),\
StructField('LAND1',StringType(),True),\
StructField('NAME1',StringType(),True),\
StructField('NAME2',StringType(),True),\
StructField('ORT01',StringType(),True),\
StructField('PSTLZ',StringType(),True),\
StructField('REGIO',StringType(),True),\
StructField('SORTL',StringType(),True),\
StructField('STRAS',StringType(),True),\
StructField('TELF1',StringType(),True),\
StructField('TELFX',StringType(),True),\
StructField('XCPDK',StringType(),True),\
StructField('ADRNR',StringType(),True),\
StructField('MCOD1',StringType(),True),\
StructField('MCOD2',StringType(),True),\
StructField('MCOD3',StringType(),True),\
StructField('ANRED',StringType(),True),\
StructField('AUFSD',StringType(),True),\
StructField('BAHNE',StringType(),True),\
StructField('BAHNS',StringType(),True),\
StructField('BBBNR',StringType(),True),\
StructField('BBSNR',StringType(),True),\
StructField('BEGRU',StringType(),True),\
StructField('BRSCH',StringType(),True),\
StructField('BUBKZ',StringType(),True),\
StructField('DATLT',StringType(),True),\
StructField('ERDAT',StringType(),True),\
StructField('ERNAM',StringType(),True),\
StructField('EXABL',StringType(),True),\
StructField('FAKSD',StringType(),True),\
StructField('FISKN',StringType(),True),\
StructField('KNAZK',StringType(),True),\
StructField('KNRZA',StringType(),True),\
StructField('KONZS',StringType(),True),\
StructField('KTOKD',StringType(),True),\
StructField('KUKLA',StringType(),True),\
StructField('LIFNR',StringType(),True),\
StructField('LIFSD',StringType(),True),\
StructField('LOCCO',StringType(),True),\
StructField('LOEVM',StringType(),True),\
StructField('NAME3',StringType(),True),\
StructField('NAME4',StringType(),True),\
StructField('NIELS',StringType(),True),\
StructField('ORT02',StringType(),True),\
StructField('PFACH',StringType(),True),\
StructField('PSTL2',StringType(),True),\
StructField('COUNC',StringType(),True),\
StructField('CITYC',StringType(),True),\
StructField('RPMKR',StringType(),True),\
StructField('SPERR',StringType(),True),\
StructField('SPRAS',StringType(),True),\
StructField('STCD1',StringType(),True),\
StructField('STCD2',StringType(),True),\
StructField('STKZA',StringType(),True),\
StructField('STKZU',StringType(),True),\
StructField('TELBX',StringType(),True),\
StructField('TELF2',StringType(),True),\
StructField('TELTX',StringType(),True),\
StructField('TELX1',StringType(),True),\
StructField('LZONE',StringType(),True),\
StructField('XZEMP',StringType(),True),\
StructField('VBUND',StringType(),True),\
StructField('STCEG',StringType(),True),\
StructField('DEAR1',StringType(),True),\
StructField('DEAR2',StringType(),True),\
StructField('DEAR3',StringType(),True),\
StructField('DEAR4',StringType(),True),\
StructField('DEAR5',StringType(),True),\
StructField('GFORM',StringType(),True),\
StructField('BRAN1',StringType(),True),\
StructField('BRAN2',StringType(),True),\
StructField('BRAN3',StringType(),True),\
StructField('BRAN4',StringType(),True),\
StructField('BRAN5',StringType(),True),\
StructField('EKONT',StringType(),True),\
StructField('UMSAT',StringType(),True),\
StructField('UMJAH',StringType(),True),\
StructField('UWAER',StringType(),True),\
StructField('JMZAH',StringType(),True),\
StructField('JMJAH',StringType(),True),\
StructField('KATR1',StringType(),True),\
StructField('KATR2',StringType(),True),\
StructField('KATR3',StringType(),True),\
StructField('KATR4',StringType(),True),\
StructField('KATR5',StringType(),True),\
StructField('KATR6',StringType(),True),\
StructField('KATR7',StringType(),True),\
StructField('KATR8',StringType(),True),\
StructField('KATR9',StringType(),True),\
StructField('KATR10',StringType(),True),\
StructField('STKZN',StringType(),True),\
StructField('UMSA1',StringType(),True),\
StructField('TXJCD',StringType(),True),\
StructField('PERIV',StringType(),True),\
StructField('ABRVW',StringType(),True),\
StructField('INSPBYDEBI',StringType(),True),\
StructField('INSPATDEBI',StringType(),True),\
StructField('KTOCD',StringType(),True),\
StructField('PFORT',StringType(),True),\
StructField('WERKS',StringType(),True),\
StructField('DTAMS',StringType(),True),\
StructField('DTAWS',StringType(),True),\
StructField('DUEFL',StringType(),True),\
StructField('HZUOR',StringType(),True),\
StructField('SPERZ',StringType(),True),\
StructField('ETIKG',StringType(),True),\
StructField('CIVVE',StringType(),True),\
StructField('MILVE',StringType(),True),\
StructField('KDKG1',StringType(),True),\
StructField('KDKG2',StringType(),True),\
StructField('KDKG3',StringType(),True),\
StructField('KDKG4',StringType(),True),\
StructField('KDKG5',StringType(),True),\
StructField('XKNZA',StringType(),True),\
StructField('FITYP',StringType(),True),\
StructField('STCDT',StringType(),True),\
StructField('STCD3',StringType(),True),\
StructField('STCD4',StringType(),True),\
StructField('STCD5',StringType(),True),\
StructField('STCD6',StringType(),True),\
StructField('XICMS',StringType(),True),\
StructField('XXIPI',StringType(),True),\
StructField('XSUBT',StringType(),True),\
StructField('CFOPC',StringType(),True),\
StructField('TXLW1',StringType(),True),\
StructField('TXLW2',StringType(),True),\
StructField('CCC01',StringType(),True),\
StructField('CCC02',StringType(),True),\
StructField('CCC03',StringType(),True),\
StructField('CCC04',StringType(),True),\
StructField('BONDED_AREA_CONFIRM',StringType(),True),\
StructField('DONATE_MARK',StringType(),True),\
StructField('CONSOLIDATE_INVOICE',StringType(),True),\
StructField('ALLOWANCE_TYPE',StringType(),True),\
StructField('EINVOICE_MODE',StringType(),True),\
StructField('CASSD',StringType(),True),\
StructField('KNURL',StringType(),True),\
StructField('J_1KFREPRE',StringType(),True),\
StructField('J_1KFTBUS',StringType(),True),\
StructField('J_1KFTIND',StringType(),True),\
StructField('CONFS',StringType(),True),\
StructField('UPDAT',StringType(),True),\
StructField('UPTIM',StringType(),True),\
StructField('NODEL',StringType(),True),\
StructField('DEAR6',StringType(),True),\
StructField('DELIVERY_DATE_RULE',StringType(),True),\
StructField('CVP_XBLCK',StringType(),True),\
StructField('SUFRAMA',StringType(),True),\
StructField('RG',StringType(),True),\
StructField('EXP',StringType(),True),\
StructField('UF',StringType(),True),\
StructField('RGDATE',StringType(),True),\
StructField('RIC',StringType(),True),\
StructField('RNE',StringType(),True),\
StructField('RNEDATE',StringType(),True),\
StructField('CNAE',StringType(),True),\
StructField('LEGALNAT',StringType(),True),\
StructField('CRTN',StringType(),True),\
StructField('ICMSTAXPAY',StringType(),True),\
StructField('INDTYP',StringType(),True),\
StructField('TDT',StringType(),True),\
StructField('COMSIZE',StringType(),True),\
StructField('DECREGPC',StringType(),True),\
StructField('PH_BIZ_STYLE',StringType(),True),\
StructField('KNA1_EEW_CUST',StringType(),True),\
StructField('RULE_EXCLUSION',StringType(),True),\
StructField('/VSO/R_PALHGT',StringType(),True),\
StructField('/VSO/R_PAL_UL',StringType(),True),\
StructField('/VSO/R_PK_MAT',StringType(),True),\
StructField('/VSO/R_MATPAL',StringType(),True),\
StructField('/VSO/R_I_NO_LYR',StringType(),True),\
StructField('/VSO/R_ONE_MAT',StringType(),True),\
StructField('/VSO/R_ONE_SORT',StringType(),True),\
StructField('/VSO/R_ULD_SIDE',StringType(),True),\
StructField('/VSO/R_LOAD_PREF',StringType(),True),\
StructField('/VSO/R_DPOINT',StringType(),True),\
StructField('ALC',StringType(),True),\
StructField('PMT_OFFICE',StringType(),True),\
StructField('FEE_SCHEDULE',StringType(),True),\
StructField('DUNS',StringType(),True),\
StructField('DUNS4',StringType(),True),\
StructField('SAM_UE_ID',StringType(),True),\
StructField('SAM_EFT_IND',StringType(),True),\
StructField('PSOFG',StringType(),True),\
StructField('PSOIS',StringType(),True),\
StructField('PSON1',StringType(),True),\
StructField('PSON2',StringType(),True),\
StructField('PSON3',StringType(),True),\
StructField('PSOVN',StringType(),True),\
StructField('PSOTL',StringType(),True),\
StructField('PSOHS',StringType(),True),\
StructField('PSOST',StringType(),True),\
StructField('PSOO1',StringType(),True),\
StructField('PSOO2',StringType(),True),\
StructField('PSOO3',StringType(),True),\
StructField('PSOO4',StringType(),True),\
StructField('PSOO5',StringType(),True),\
StructField('J_1IEXCD',StringType(),True),\
StructField('J_1IEXRN',StringType(),True),\
StructField('J_1IEXRG',StringType(),True),\
StructField('J_1IEXDI',StringType(),True),\
StructField('J_1IEXCO',StringType(),True),\
StructField('J_1ICSTNO',StringType(),True),\
StructField('J_1ILSTNO',StringType(),True),\
StructField('J_1IPANNO',StringType(),True),\
StructField('J_1IEXCICU',StringType(),True),\
StructField('AEDAT',StringType(),True),\
StructField('USNAM',StringType(),True),\
StructField('J_1ISERN',StringType(),True),\
StructField('J_1IPANREF',StringType(),True),\
StructField('GST_TDS',StringType(),True),\
StructField('J_3GETYP',StringType(),True),\
StructField('J_3GREFTYP',StringType(),True),\
StructField('PSPNR',StringType(),True),\
StructField('COAUFNR',StringType(),True),\
StructField('J_3GAGEXT',StringType(),True),\
StructField('J_3GAGINT',StringType(),True),\
StructField('J_3GAGDUMI',StringType(),True),\
StructField('J_3GAGSTDI',StringType(),True),\
StructField('LGORT',StringType(),True),\
StructField('KOKRS',StringType(),True),\
StructField('KOSTL',StringType(),True),\
StructField('J_3GABGLG',StringType(),True),\
StructField('J_3GABGVG',StringType(),True),\
StructField('J_3GABRART',StringType(),True),\
StructField('J_3GSTDMON',StringType(),True),\
StructField('J_3GSTDTAG',StringType(),True),\
StructField('J_3GTAGMON',StringType(),True),\
StructField('J_3GZUGTAG',StringType(),True),\
StructField('J_3GMASCHB',StringType(),True),\
StructField('J_3GMEINSA',StringType(),True),\
StructField('J_3GKEINSA',StringType(),True),\
StructField('J_3GBLSPER',StringType(),True),\
StructField('J_3GKLEIVO',StringType(),True),\
StructField('J_3GCALID',StringType(),True),\
StructField('J_3GVMONAT',StringType(),True),\
StructField('J_3GABRKEN',StringType(),True),\
StructField('J_3GLABRECH',StringType(),True),\
StructField('J_3GAABRECH',StringType(),True),\
StructField('J_3GZUTVHLG',StringType(),True),\
StructField('J_3GNEGMEN',StringType(),True),\
StructField('J_3GFRISTLO',StringType(),True),\
StructField('J_3GEMINBE',StringType(),True),\
StructField('J_3GFMGUE',StringType(),True),\
StructField('J_3GZUSCHUE',StringType(),True),\
StructField('J_3GSCHPRS',StringType(),True),\
StructField('J_3GINVSTA',StringType(),True),\
StructField('/SAPCEM/DBER',StringType(),True),\
StructField('/SAPCEM/KVMEQ',StringType(),True),\
StructField('ZZAGILE_CUS',StringType(),True),\
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
.withColumn( "ERDAT",to_date(regexp_replace(df_add_column.ERDAT,'\.','-'))) \
.withColumn( "UPDAT",to_date(regexp_replace(df_add_column.UPDAT,'\.','-'))) \
.withColumn( "RGDATE",to_date(regexp_replace(df_add_column.RGDATE,'\.','-'))) \
.withColumn( "RNEDATE",to_date(regexp_replace(df_add_column.RNEDATE,'\.','-'))) \
.withColumn( "AEDAT",to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
.withColumn( "J_3GLABRECH",to_date(regexp_replace(df_add_column.J_3GLABRECH,'\.','-'))) \
.withColumn( "J_3GAABRECH",to_date(regexp_replace(df_add_column.J_3GAABRECH,'\.','-'))) \
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
# MAGIC MERGE INTO S42.KNA1 as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY MANDT,KUNNR ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_KNA1)A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and
# MAGIC T.KUNNR = S.KUNNR
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`MANDT`	 = S.`MANDT`,
# MAGIC T.`KUNNR`	 = S.`KUNNR`,
# MAGIC T.`LAND1`	 = S.`LAND1`,
# MAGIC T.`NAME1`	 = S.`NAME1`,
# MAGIC T.`NAME2`	 = S.`NAME2`,
# MAGIC T.`ORT01`	 = S.`ORT01`,
# MAGIC T.`PSTLZ`	 = S.`PSTLZ`,
# MAGIC T.`REGIO`	 = S.`REGIO`,
# MAGIC T.`SORTL`	 = S.`SORTL`,
# MAGIC T.`STRAS`	 = S.`STRAS`,
# MAGIC T.`TELF1`	 = S.`TELF1`,
# MAGIC T.`TELFX`	 = S.`TELFX`,
# MAGIC T.`XCPDK`	 = S.`XCPDK`,
# MAGIC T.`ADRNR`	 = S.`ADRNR`,
# MAGIC T.`MCOD1`	 = S.`MCOD1`,
# MAGIC T.`MCOD2`	 = S.`MCOD2`,
# MAGIC T.`MCOD3`	 = S.`MCOD3`,
# MAGIC T.`ANRED`	 = S.`ANRED`,
# MAGIC T.`AUFSD`	 = S.`AUFSD`,
# MAGIC T.`BAHNE`	 = S.`BAHNE`,
# MAGIC T.`BAHNS`	 = S.`BAHNS`,
# MAGIC T.`BBBNR`	 = S.`BBBNR`,
# MAGIC T.`BBSNR`	 = S.`BBSNR`,
# MAGIC T.`BEGRU`	 = S.`BEGRU`,
# MAGIC T.`BRSCH`	 = S.`BRSCH`,
# MAGIC T.`BUBKZ`	 = S.`BUBKZ`,
# MAGIC T.`DATLT`	 = S.`DATLT`,
# MAGIC T.`ERDAT`	 = S.`ERDAT`,
# MAGIC T.`ERNAM`	 = S.`ERNAM`,
# MAGIC T.`EXABL`	 = S.`EXABL`,
# MAGIC T.`FAKSD`	 = S.`FAKSD`,
# MAGIC T.`FISKN`	 = S.`FISKN`,
# MAGIC T.`KNAZK`	 = S.`KNAZK`,
# MAGIC T.`KNRZA`	 = S.`KNRZA`,
# MAGIC T.`KONZS`	 = S.`KONZS`,
# MAGIC T.`KTOKD`	 = S.`KTOKD`,
# MAGIC T.`KUKLA`	 = S.`KUKLA`,
# MAGIC T.`LIFNR`	 = S.`LIFNR`,
# MAGIC T.`LIFSD`	 = S.`LIFSD`,
# MAGIC T.`LOCCO`	 = S.`LOCCO`,
# MAGIC T.`LOEVM`	 = S.`LOEVM`,
# MAGIC T.`NAME3`	 = S.`NAME3`,
# MAGIC T.`NAME4`	 = S.`NAME4`,
# MAGIC T.`NIELS`	 = S.`NIELS`,
# MAGIC T.`ORT02`	 = S.`ORT02`,
# MAGIC T.`PFACH`	 = S.`PFACH`,
# MAGIC T.`PSTL2`	 = S.`PSTL2`,
# MAGIC T.`COUNC`	 = S.`COUNC`,
# MAGIC T.`CITYC`	 = S.`CITYC`,
# MAGIC T.`RPMKR`	 = S.`RPMKR`,
# MAGIC T.`SPERR`	 = S.`SPERR`,
# MAGIC T.`SPRAS`	 = S.`SPRAS`,
# MAGIC T.`STCD1`	 = S.`STCD1`,
# MAGIC T.`STCD2`	 = S.`STCD2`,
# MAGIC T.`STKZA`	 = S.`STKZA`,
# MAGIC T.`STKZU`	 = S.`STKZU`,
# MAGIC T.`TELBX`	 = S.`TELBX`,
# MAGIC T.`TELF2`	 = S.`TELF2`,
# MAGIC T.`TELTX`	 = S.`TELTX`,
# MAGIC T.`TELX1`	 = S.`TELX1`,
# MAGIC T.`LZONE`	 = S.`LZONE`,
# MAGIC T.`XZEMP`	 = S.`XZEMP`,
# MAGIC T.`VBUND`	 = S.`VBUND`,
# MAGIC T.`STCEG`	 = S.`STCEG`,
# MAGIC T.`DEAR1`	 = S.`DEAR1`,
# MAGIC T.`DEAR2`	 = S.`DEAR2`,
# MAGIC T.`DEAR3`	 = S.`DEAR3`,
# MAGIC T.`DEAR4`	 = S.`DEAR4`,
# MAGIC T.`DEAR5`	 = S.`DEAR5`,
# MAGIC T.`GFORM`	 = S.`GFORM`,
# MAGIC T.`BRAN1`	 = S.`BRAN1`,
# MAGIC T.`BRAN2`	 = S.`BRAN2`,
# MAGIC T.`BRAN3`	 = S.`BRAN3`,
# MAGIC T.`BRAN4`	 = S.`BRAN4`,
# MAGIC T.`BRAN5`	 = S.`BRAN5`,
# MAGIC T.`EKONT`	 = S.`EKONT`,
# MAGIC T.`UMSAT`	 = S.`UMSAT`,
# MAGIC T.`UMJAH`	 = S.`UMJAH`,
# MAGIC T.`UWAER`	 = S.`UWAER`,
# MAGIC T.`JMZAH`	 = S.`JMZAH`,
# MAGIC T.`JMJAH`	 = S.`JMJAH`,
# MAGIC T.`KATR1`	 = S.`KATR1`,
# MAGIC T.`KATR2`	 = S.`KATR2`,
# MAGIC T.`KATR3`	 = S.`KATR3`,
# MAGIC T.`KATR4`	 = S.`KATR4`,
# MAGIC T.`KATR5`	 = S.`KATR5`,
# MAGIC T.`KATR6`	 = S.`KATR6`,
# MAGIC T.`KATR7`	 = S.`KATR7`,
# MAGIC T.`KATR8`	 = S.`KATR8`,
# MAGIC T.`KATR9`	 = S.`KATR9`,
# MAGIC T.`KATR10`	 = S.`KATR10`,
# MAGIC T.`STKZN`	 = S.`STKZN`,
# MAGIC T.`UMSA1`	 = S.`UMSA1`,
# MAGIC T.`TXJCD`	 = S.`TXJCD`,
# MAGIC T.`PERIV`	 = S.`PERIV`,
# MAGIC T.`ABRVW`	 = S.`ABRVW`,
# MAGIC T.`INSPBYDEBI`	 = S.`INSPBYDEBI`,
# MAGIC T.`INSPATDEBI`	 = S.`INSPATDEBI`,
# MAGIC T.`KTOCD`	 = S.`KTOCD`,
# MAGIC T.`PFORT`	 = S.`PFORT`,
# MAGIC T.`WERKS`	 = S.`WERKS`,
# MAGIC T.`DTAMS`	 = S.`DTAMS`,
# MAGIC T.`DTAWS`	 = S.`DTAWS`,
# MAGIC T.`DUEFL`	 = S.`DUEFL`,
# MAGIC T.`HZUOR`	 = S.`HZUOR`,
# MAGIC T.`SPERZ`	 = S.`SPERZ`,
# MAGIC T.`ETIKG`	 = S.`ETIKG`,
# MAGIC T.`CIVVE`	 = S.`CIVVE`,
# MAGIC T.`MILVE`	 = S.`MILVE`,
# MAGIC T.`KDKG1`	 = S.`KDKG1`,
# MAGIC T.`KDKG2`	 = S.`KDKG2`,
# MAGIC T.`KDKG3`	 = S.`KDKG3`,
# MAGIC T.`KDKG4`	 = S.`KDKG4`,
# MAGIC T.`KDKG5`	 = S.`KDKG5`,
# MAGIC T.`XKNZA`	 = S.`XKNZA`,
# MAGIC T.`FITYP`	 = S.`FITYP`,
# MAGIC T.`STCDT`	 = S.`STCDT`,
# MAGIC T.`STCD3`	 = S.`STCD3`,
# MAGIC T.`STCD4`	 = S.`STCD4`,
# MAGIC T.`STCD5`	 = S.`STCD5`,
# MAGIC T.`STCD6`	 = S.`STCD6`,
# MAGIC T.`XICMS`	 = S.`XICMS`,
# MAGIC T.`XXIPI`	 = S.`XXIPI`,
# MAGIC T.`XSUBT`	 = S.`XSUBT`,
# MAGIC T.`CFOPC`	 = S.`CFOPC`,
# MAGIC T.`TXLW1`	 = S.`TXLW1`,
# MAGIC T.`TXLW2`	 = S.`TXLW2`,
# MAGIC T.`CCC01`	 = S.`CCC01`,
# MAGIC T.`CCC02`	 = S.`CCC02`,
# MAGIC T.`CCC03`	 = S.`CCC03`,
# MAGIC T.`CCC04`	 = S.`CCC04`,
# MAGIC T.`BONDED_AREA_CONFIRM`	 = S.`BONDED_AREA_CONFIRM`,
# MAGIC T.`DONATE_MARK`	 = S.`DONATE_MARK`,
# MAGIC T.`CONSOLIDATE_INVOICE`	 = S.`CONSOLIDATE_INVOICE`,
# MAGIC T.`ALLOWANCE_TYPE`	 = S.`ALLOWANCE_TYPE`,
# MAGIC T.`EINVOICE_MODE`	 = S.`EINVOICE_MODE`,
# MAGIC T.`CASSD`	 = S.`CASSD`,
# MAGIC T.`KNURL`	 = S.`KNURL`,
# MAGIC T.`J_1KFREPRE`	 = S.`J_1KFREPRE`,
# MAGIC T.`J_1KFTBUS`	 = S.`J_1KFTBUS`,
# MAGIC T.`J_1KFTIND`	 = S.`J_1KFTIND`,
# MAGIC T.`CONFS`	 = S.`CONFS`,
# MAGIC T.`UPDAT`	 = S.`UPDAT`,
# MAGIC T.`UPTIM`	 = S.`UPTIM`,
# MAGIC T.`NODEL`	 = S.`NODEL`,
# MAGIC T.`DEAR6`	 = S.`DEAR6`,
# MAGIC T.`DELIVERY_DATE_RULE`	 = S.`DELIVERY_DATE_RULE`,
# MAGIC T.`CVP_XBLCK`	 = S.`CVP_XBLCK`,
# MAGIC T.`SUFRAMA`	 = S.`SUFRAMA`,
# MAGIC T.`RG`	 = S.`RG`,
# MAGIC T.`EXP`	 = S.`EXP`,
# MAGIC T.`UF`	 = S.`UF`,
# MAGIC T.`RGDATE`	 = S.`RGDATE`,
# MAGIC T.`RIC`	 = S.`RIC`,
# MAGIC T.`RNE`	 = S.`RNE`,
# MAGIC T.`RNEDATE`	 = S.`RNEDATE`,
# MAGIC T.`CNAE`	 = S.`CNAE`,
# MAGIC T.`LEGALNAT`	 = S.`LEGALNAT`,
# MAGIC T.`CRTN`	 = S.`CRTN`,
# MAGIC T.`ICMSTAXPAY`	 = S.`ICMSTAXPAY`,
# MAGIC T.`INDTYP`	 = S.`INDTYP`,
# MAGIC T.`TDT`	 = S.`TDT`,
# MAGIC T.`COMSIZE`	 = S.`COMSIZE`,
# MAGIC T.`DECREGPC`	 = S.`DECREGPC`,
# MAGIC T.`PH_BIZ_STYLE`	 = S.`PH_BIZ_STYLE`,
# MAGIC T.`KNA1_EEW_CUST`	 = S.`KNA1_EEW_CUST`,
# MAGIC T.`RULE_EXCLUSION`	 = S.`RULE_EXCLUSION`,
# MAGIC T.`/VSO/R_PALHGT`	 = S.`/VSO/R_PALHGT`,
# MAGIC T.`/VSO/R_PAL_UL`	 = S.`/VSO/R_PAL_UL`,
# MAGIC T.`/VSO/R_PK_MAT`	 = S.`/VSO/R_PK_MAT`,
# MAGIC T.`/VSO/R_MATPAL`	 = S.`/VSO/R_MATPAL`,
# MAGIC T.`/VSO/R_I_NO_LYR`	 = S.`/VSO/R_I_NO_LYR`,
# MAGIC T.`/VSO/R_ONE_MAT`	 = S.`/VSO/R_ONE_MAT`,
# MAGIC T.`/VSO/R_ONE_SORT`	 = S.`/VSO/R_ONE_SORT`,
# MAGIC T.`/VSO/R_ULD_SIDE`	 = S.`/VSO/R_ULD_SIDE`,
# MAGIC T.`/VSO/R_LOAD_PREF`	 = S.`/VSO/R_LOAD_PREF`,
# MAGIC T.`/VSO/R_DPOINT`	 = S.`/VSO/R_DPOINT`,
# MAGIC T.`ALC`	 = S.`ALC`,
# MAGIC T.`PMT_OFFICE`	 = S.`PMT_OFFICE`,
# MAGIC T.`FEE_SCHEDULE`	 = S.`FEE_SCHEDULE`,
# MAGIC T.`DUNS`	 = S.`DUNS`,
# MAGIC T.`DUNS4`	 = S.`DUNS4`,
# MAGIC T.`SAM_UE_ID`	 = S.`SAM_UE_ID`,
# MAGIC T.`SAM_EFT_IND`	 = S.`SAM_EFT_IND`,
# MAGIC T.`PSOFG`	 = S.`PSOFG`,
# MAGIC T.`PSOIS`	 = S.`PSOIS`,
# MAGIC T.`PSON1`	 = S.`PSON1`,
# MAGIC T.`PSON2`	 = S.`PSON2`,
# MAGIC T.`PSON3`	 = S.`PSON3`,
# MAGIC T.`PSOVN`	 = S.`PSOVN`,
# MAGIC T.`PSOTL`	 = S.`PSOTL`,
# MAGIC T.`PSOHS`	 = S.`PSOHS`,
# MAGIC T.`PSOST`	 = S.`PSOST`,
# MAGIC T.`PSOO1`	 = S.`PSOO1`,
# MAGIC T.`PSOO2`	 = S.`PSOO2`,
# MAGIC T.`PSOO3`	 = S.`PSOO3`,
# MAGIC T.`PSOO4`	 = S.`PSOO4`,
# MAGIC T.`PSOO5`	 = S.`PSOO5`,
# MAGIC T.`J_1IEXCD`	 = S.`J_1IEXCD`,
# MAGIC T.`J_1IEXRN`	 = S.`J_1IEXRN`,
# MAGIC T.`J_1IEXRG`	 = S.`J_1IEXRG`,
# MAGIC T.`J_1IEXDI`	 = S.`J_1IEXDI`,
# MAGIC T.`J_1IEXCO`	 = S.`J_1IEXCO`,
# MAGIC T.`J_1ICSTNO`	 = S.`J_1ICSTNO`,
# MAGIC T.`J_1ILSTNO`	 = S.`J_1ILSTNO`,
# MAGIC T.`J_1IPANNO`	 = S.`J_1IPANNO`,
# MAGIC T.`J_1IEXCICU`	 = S.`J_1IEXCICU`,
# MAGIC T.`AEDAT`	 = S.`AEDAT`,
# MAGIC T.`USNAM`	 = S.`USNAM`,
# MAGIC T.`J_1ISERN`	 = S.`J_1ISERN`,
# MAGIC T.`J_1IPANREF`	 = S.`J_1IPANREF`,
# MAGIC T.`GST_TDS`	 = S.`GST_TDS`,
# MAGIC T.`J_3GETYP`	 = S.`J_3GETYP`,
# MAGIC T.`J_3GREFTYP`	 = S.`J_3GREFTYP`,
# MAGIC T.`PSPNR`	 = S.`PSPNR`,
# MAGIC T.`COAUFNR`	 = S.`COAUFNR`,
# MAGIC T.`J_3GAGEXT`	 = S.`J_3GAGEXT`,
# MAGIC T.`J_3GAGINT`	 = S.`J_3GAGINT`,
# MAGIC T.`J_3GAGDUMI`	 = S.`J_3GAGDUMI`,
# MAGIC T.`J_3GAGSTDI`	 = S.`J_3GAGSTDI`,
# MAGIC T.`LGORT`	 = S.`LGORT`,
# MAGIC T.`KOKRS`	 = S.`KOKRS`,
# MAGIC T.`KOSTL`	 = S.`KOSTL`,
# MAGIC T.`J_3GABGLG`	 = S.`J_3GABGLG`,
# MAGIC T.`J_3GABGVG`	 = S.`J_3GABGVG`,
# MAGIC T.`J_3GABRART`	 = S.`J_3GABRART`,
# MAGIC T.`J_3GSTDMON`	 = S.`J_3GSTDMON`,
# MAGIC T.`J_3GSTDTAG`	 = S.`J_3GSTDTAG`,
# MAGIC T.`J_3GTAGMON`	 = S.`J_3GTAGMON`,
# MAGIC T.`J_3GZUGTAG`	 = S.`J_3GZUGTAG`,
# MAGIC T.`J_3GMASCHB`	 = S.`J_3GMASCHB`,
# MAGIC T.`J_3GMEINSA`	 = S.`J_3GMEINSA`,
# MAGIC T.`J_3GKEINSA`	 = S.`J_3GKEINSA`,
# MAGIC T.`J_3GBLSPER`	 = S.`J_3GBLSPER`,
# MAGIC T.`J_3GKLEIVO`	 = S.`J_3GKLEIVO`,
# MAGIC T.`J_3GCALID`	 = S.`J_3GCALID`,
# MAGIC T.`J_3GVMONAT`	 = S.`J_3GVMONAT`,
# MAGIC T.`J_3GABRKEN`	 = S.`J_3GABRKEN`,
# MAGIC T.`J_3GLABRECH`	 = S.`J_3GLABRECH`,
# MAGIC T.`J_3GAABRECH`	 = S.`J_3GAABRECH`,
# MAGIC T.`J_3GZUTVHLG`	 = S.`J_3GZUTVHLG`,
# MAGIC T.`J_3GNEGMEN`	 = S.`J_3GNEGMEN`,
# MAGIC T.`J_3GFRISTLO`	 = S.`J_3GFRISTLO`,
# MAGIC T.`J_3GEMINBE`	 = S.`J_3GEMINBE`,
# MAGIC T.`J_3GFMGUE`	 = S.`J_3GFMGUE`,
# MAGIC T.`J_3GZUSCHUE`	 = S.`J_3GZUSCHUE`,
# MAGIC T.`J_3GSCHPRS`	 = S.`J_3GSCHPRS`,
# MAGIC T.`J_3GINVSTA`	 = S.`J_3GINVSTA`,
# MAGIC T.`/SAPCEM/DBER`	 = S.`/SAPCEM/DBER`,
# MAGIC T.`/SAPCEM/KVMEQ`	 = S.`/SAPCEM/KVMEQ`,
# MAGIC T.`ZZAGILE_CUS`	 = S.`ZZAGILE_CUS`,
# MAGIC T.`ODQ_CHANGEMODE`	 = S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR`	 = S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp`	 = S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `KUNNR`,
# MAGIC `LAND1`,
# MAGIC `NAME1`,
# MAGIC `NAME2`,
# MAGIC `ORT01`,
# MAGIC `PSTLZ`,
# MAGIC `REGIO`,
# MAGIC `SORTL`,
# MAGIC `STRAS`,
# MAGIC `TELF1`,
# MAGIC `TELFX`,
# MAGIC `XCPDK`,
# MAGIC `ADRNR`,
# MAGIC `MCOD1`,
# MAGIC `MCOD2`,
# MAGIC `MCOD3`,
# MAGIC `ANRED`,
# MAGIC `AUFSD`,
# MAGIC `BAHNE`,
# MAGIC `BAHNS`,
# MAGIC `BBBNR`,
# MAGIC `BBSNR`,
# MAGIC `BEGRU`,
# MAGIC `BRSCH`,
# MAGIC `BUBKZ`,
# MAGIC `DATLT`,
# MAGIC `ERDAT`,
# MAGIC `ERNAM`,
# MAGIC `EXABL`,
# MAGIC `FAKSD`,
# MAGIC `FISKN`,
# MAGIC `KNAZK`,
# MAGIC `KNRZA`,
# MAGIC `KONZS`,
# MAGIC `KTOKD`,
# MAGIC `KUKLA`,
# MAGIC `LIFNR`,
# MAGIC `LIFSD`,
# MAGIC `LOCCO`,
# MAGIC `LOEVM`,
# MAGIC `NAME3`,
# MAGIC `NAME4`,
# MAGIC `NIELS`,
# MAGIC `ORT02`,
# MAGIC `PFACH`,
# MAGIC `PSTL2`,
# MAGIC `COUNC`,
# MAGIC `CITYC`,
# MAGIC `RPMKR`,
# MAGIC `SPERR`,
# MAGIC `SPRAS`,
# MAGIC `STCD1`,
# MAGIC `STCD2`,
# MAGIC `STKZA`,
# MAGIC `STKZU`,
# MAGIC `TELBX`,
# MAGIC `TELF2`,
# MAGIC `TELTX`,
# MAGIC `TELX1`,
# MAGIC `LZONE`,
# MAGIC `XZEMP`,
# MAGIC `VBUND`,
# MAGIC `STCEG`,
# MAGIC `DEAR1`,
# MAGIC `DEAR2`,
# MAGIC `DEAR3`,
# MAGIC `DEAR4`,
# MAGIC `DEAR5`,
# MAGIC `GFORM`,
# MAGIC `BRAN1`,
# MAGIC `BRAN2`,
# MAGIC `BRAN3`,
# MAGIC `BRAN4`,
# MAGIC `BRAN5`,
# MAGIC `EKONT`,
# MAGIC `UMSAT`,
# MAGIC `UMJAH`,
# MAGIC `UWAER`,
# MAGIC `JMZAH`,
# MAGIC `JMJAH`,
# MAGIC `KATR1`,
# MAGIC `KATR2`,
# MAGIC `KATR3`,
# MAGIC `KATR4`,
# MAGIC `KATR5`,
# MAGIC `KATR6`,
# MAGIC `KATR7`,
# MAGIC `KATR8`,
# MAGIC `KATR9`,
# MAGIC `KATR10`,
# MAGIC `STKZN`,
# MAGIC `UMSA1`,
# MAGIC `TXJCD`,
# MAGIC `PERIV`,
# MAGIC `ABRVW`,
# MAGIC `INSPBYDEBI`,
# MAGIC `INSPATDEBI`,
# MAGIC `KTOCD`,
# MAGIC `PFORT`,
# MAGIC `WERKS`,
# MAGIC `DTAMS`,
# MAGIC `DTAWS`,
# MAGIC `DUEFL`,
# MAGIC `HZUOR`,
# MAGIC `SPERZ`,
# MAGIC `ETIKG`,
# MAGIC `CIVVE`,
# MAGIC `MILVE`,
# MAGIC `KDKG1`,
# MAGIC `KDKG2`,
# MAGIC `KDKG3`,
# MAGIC `KDKG4`,
# MAGIC `KDKG5`,
# MAGIC `XKNZA`,
# MAGIC `FITYP`,
# MAGIC `STCDT`,
# MAGIC `STCD3`,
# MAGIC `STCD4`,
# MAGIC `STCD5`,
# MAGIC `STCD6`,
# MAGIC `XICMS`,
# MAGIC `XXIPI`,
# MAGIC `XSUBT`,
# MAGIC `CFOPC`,
# MAGIC `TXLW1`,
# MAGIC `TXLW2`,
# MAGIC `CCC01`,
# MAGIC `CCC02`,
# MAGIC `CCC03`,
# MAGIC `CCC04`,
# MAGIC `BONDED_AREA_CONFIRM`,
# MAGIC `DONATE_MARK`,
# MAGIC `CONSOLIDATE_INVOICE`,
# MAGIC `ALLOWANCE_TYPE`,
# MAGIC `EINVOICE_MODE`,
# MAGIC `CASSD`,
# MAGIC `KNURL`,
# MAGIC `J_1KFREPRE`,
# MAGIC `J_1KFTBUS`,
# MAGIC `J_1KFTIND`,
# MAGIC `CONFS`,
# MAGIC `UPDAT`,
# MAGIC `UPTIM`,
# MAGIC `NODEL`,
# MAGIC `DEAR6`,
# MAGIC `DELIVERY_DATE_RULE`,
# MAGIC `CVP_XBLCK`,
# MAGIC `SUFRAMA`,
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
# MAGIC `PH_BIZ_STYLE`,
# MAGIC `KNA1_EEW_CUST`,
# MAGIC `RULE_EXCLUSION`,
# MAGIC `/VSO/R_PALHGT`,
# MAGIC `/VSO/R_PAL_UL`,
# MAGIC `/VSO/R_PK_MAT`,
# MAGIC `/VSO/R_MATPAL`,
# MAGIC `/VSO/R_I_NO_LYR`,
# MAGIC `/VSO/R_ONE_MAT`,
# MAGIC `/VSO/R_ONE_SORT`,
# MAGIC `/VSO/R_ULD_SIDE`,
# MAGIC `/VSO/R_LOAD_PREF`,
# MAGIC `/VSO/R_DPOINT`,
# MAGIC `ALC`,
# MAGIC `PMT_OFFICE`,
# MAGIC `FEE_SCHEDULE`,
# MAGIC `DUNS`,
# MAGIC `DUNS4`,
# MAGIC `SAM_UE_ID`,
# MAGIC `SAM_EFT_IND`,
# MAGIC `PSOFG`,
# MAGIC `PSOIS`,
# MAGIC `PSON1`,
# MAGIC `PSON2`,
# MAGIC `PSON3`,
# MAGIC `PSOVN`,
# MAGIC `PSOTL`,
# MAGIC `PSOHS`,
# MAGIC `PSOST`,
# MAGIC `PSOO1`,
# MAGIC `PSOO2`,
# MAGIC `PSOO3`,
# MAGIC `PSOO4`,
# MAGIC `PSOO5`,
# MAGIC `J_1IEXCD`,
# MAGIC `J_1IEXRN`,
# MAGIC `J_1IEXRG`,
# MAGIC `J_1IEXDI`,
# MAGIC `J_1IEXCO`,
# MAGIC `J_1ICSTNO`,
# MAGIC `J_1ILSTNO`,
# MAGIC `J_1IPANNO`,
# MAGIC `J_1IEXCICU`,
# MAGIC `AEDAT`,
# MAGIC `USNAM`,
# MAGIC `J_1ISERN`,
# MAGIC `J_1IPANREF`,
# MAGIC `GST_TDS`,
# MAGIC `J_3GETYP`,
# MAGIC `J_3GREFTYP`,
# MAGIC `PSPNR`,
# MAGIC `COAUFNR`,
# MAGIC `J_3GAGEXT`,
# MAGIC `J_3GAGINT`,
# MAGIC `J_3GAGDUMI`,
# MAGIC `J_3GAGSTDI`,
# MAGIC `LGORT`,
# MAGIC `KOKRS`,
# MAGIC `KOSTL`,
# MAGIC `J_3GABGLG`,
# MAGIC `J_3GABGVG`,
# MAGIC `J_3GABRART`,
# MAGIC `J_3GSTDMON`,
# MAGIC `J_3GSTDTAG`,
# MAGIC `J_3GTAGMON`,
# MAGIC `J_3GZUGTAG`,
# MAGIC `J_3GMASCHB`,
# MAGIC `J_3GMEINSA`,
# MAGIC `J_3GKEINSA`,
# MAGIC `J_3GBLSPER`,
# MAGIC `J_3GKLEIVO`,
# MAGIC `J_3GCALID`,
# MAGIC `J_3GVMONAT`,
# MAGIC `J_3GABRKEN`,
# MAGIC `J_3GLABRECH`,
# MAGIC `J_3GAABRECH`,
# MAGIC `J_3GZUTVHLG`,
# MAGIC `J_3GNEGMEN`,
# MAGIC `J_3GFRISTLO`,
# MAGIC `J_3GEMINBE`,
# MAGIC `J_3GFMGUE`,
# MAGIC `J_3GZUSCHUE`,
# MAGIC `J_3GSCHPRS`,
# MAGIC `J_3GINVSTA`,
# MAGIC `/SAPCEM/DBER`,
# MAGIC `/SAPCEM/KVMEQ`,
# MAGIC `ZZAGILE_CUS`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC VALUES
# MAGIC (S.`MANDT`,
# MAGIC S.`KUNNR`,
# MAGIC S.`LAND1`,
# MAGIC S.`NAME1`,
# MAGIC S.`NAME2`,
# MAGIC S.`ORT01`,
# MAGIC S.`PSTLZ`,
# MAGIC S.`REGIO`,
# MAGIC S.`SORTL`,
# MAGIC S.`STRAS`,
# MAGIC S.`TELF1`,
# MAGIC S.`TELFX`,
# MAGIC S.`XCPDK`,
# MAGIC S.`ADRNR`,
# MAGIC S.`MCOD1`,
# MAGIC S.`MCOD2`,
# MAGIC S.`MCOD3`,
# MAGIC S.`ANRED`,
# MAGIC S.`AUFSD`,
# MAGIC S.`BAHNE`,
# MAGIC S.`BAHNS`,
# MAGIC S.`BBBNR`,
# MAGIC S.`BBSNR`,
# MAGIC S.`BEGRU`,
# MAGIC S.`BRSCH`,
# MAGIC S.`BUBKZ`,
# MAGIC S.`DATLT`,
# MAGIC S.`ERDAT`,
# MAGIC S.`ERNAM`,
# MAGIC S.`EXABL`,
# MAGIC S.`FAKSD`,
# MAGIC S.`FISKN`,
# MAGIC S.`KNAZK`,
# MAGIC S.`KNRZA`,
# MAGIC S.`KONZS`,
# MAGIC S.`KTOKD`,
# MAGIC S.`KUKLA`,
# MAGIC S.`LIFNR`,
# MAGIC S.`LIFSD`,
# MAGIC S.`LOCCO`,
# MAGIC S.`LOEVM`,
# MAGIC S.`NAME3`,
# MAGIC S.`NAME4`,
# MAGIC S.`NIELS`,
# MAGIC S.`ORT02`,
# MAGIC S.`PFACH`,
# MAGIC S.`PSTL2`,
# MAGIC S.`COUNC`,
# MAGIC S.`CITYC`,
# MAGIC S.`RPMKR`,
# MAGIC S.`SPERR`,
# MAGIC S.`SPRAS`,
# MAGIC S.`STCD1`,
# MAGIC S.`STCD2`,
# MAGIC S.`STKZA`,
# MAGIC S.`STKZU`,
# MAGIC S.`TELBX`,
# MAGIC S.`TELF2`,
# MAGIC S.`TELTX`,
# MAGIC S.`TELX1`,
# MAGIC S.`LZONE`,
# MAGIC S.`XZEMP`,
# MAGIC S.`VBUND`,
# MAGIC S.`STCEG`,
# MAGIC S.`DEAR1`,
# MAGIC S.`DEAR2`,
# MAGIC S.`DEAR3`,
# MAGIC S.`DEAR4`,
# MAGIC S.`DEAR5`,
# MAGIC S.`GFORM`,
# MAGIC S.`BRAN1`,
# MAGIC S.`BRAN2`,
# MAGIC S.`BRAN3`,
# MAGIC S.`BRAN4`,
# MAGIC S.`BRAN5`,
# MAGIC S.`EKONT`,
# MAGIC S.`UMSAT`,
# MAGIC S.`UMJAH`,
# MAGIC S.`UWAER`,
# MAGIC S.`JMZAH`,
# MAGIC S.`JMJAH`,
# MAGIC S.`KATR1`,
# MAGIC S.`KATR2`,
# MAGIC S.`KATR3`,
# MAGIC S.`KATR4`,
# MAGIC S.`KATR5`,
# MAGIC S.`KATR6`,
# MAGIC S.`KATR7`,
# MAGIC S.`KATR8`,
# MAGIC S.`KATR9`,
# MAGIC S.`KATR10`,
# MAGIC S.`STKZN`,
# MAGIC S.`UMSA1`,
# MAGIC S.`TXJCD`,
# MAGIC S.`PERIV`,
# MAGIC S.`ABRVW`,
# MAGIC S.`INSPBYDEBI`,
# MAGIC S.`INSPATDEBI`,
# MAGIC S.`KTOCD`,
# MAGIC S.`PFORT`,
# MAGIC S.`WERKS`,
# MAGIC S.`DTAMS`,
# MAGIC S.`DTAWS`,
# MAGIC S.`DUEFL`,
# MAGIC S.`HZUOR`,
# MAGIC S.`SPERZ`,
# MAGIC S.`ETIKG`,
# MAGIC S.`CIVVE`,
# MAGIC S.`MILVE`,
# MAGIC S.`KDKG1`,
# MAGIC S.`KDKG2`,
# MAGIC S.`KDKG3`,
# MAGIC S.`KDKG4`,
# MAGIC S.`KDKG5`,
# MAGIC S.`XKNZA`,
# MAGIC S.`FITYP`,
# MAGIC S.`STCDT`,
# MAGIC S.`STCD3`,
# MAGIC S.`STCD4`,
# MAGIC S.`STCD5`,
# MAGIC S.`STCD6`,
# MAGIC S.`XICMS`,
# MAGIC S.`XXIPI`,
# MAGIC S.`XSUBT`,
# MAGIC S.`CFOPC`,
# MAGIC S.`TXLW1`,
# MAGIC S.`TXLW2`,
# MAGIC S.`CCC01`,
# MAGIC S.`CCC02`,
# MAGIC S.`CCC03`,
# MAGIC S.`CCC04`,
# MAGIC S.`BONDED_AREA_CONFIRM`,
# MAGIC S.`DONATE_MARK`,
# MAGIC S.`CONSOLIDATE_INVOICE`,
# MAGIC S.`ALLOWANCE_TYPE`,
# MAGIC S.`EINVOICE_MODE`,
# MAGIC S.`CASSD`,
# MAGIC S.`KNURL`,
# MAGIC S.`J_1KFREPRE`,
# MAGIC S.`J_1KFTBUS`,
# MAGIC S.`J_1KFTIND`,
# MAGIC S.`CONFS`,
# MAGIC S.`UPDAT`,
# MAGIC S.`UPTIM`,
# MAGIC S.`NODEL`,
# MAGIC S.`DEAR6`,
# MAGIC S.`DELIVERY_DATE_RULE`,
# MAGIC S.`CVP_XBLCK`,
# MAGIC S.`SUFRAMA`,
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
# MAGIC S.`PH_BIZ_STYLE`,
# MAGIC S.`KNA1_EEW_CUST`,
# MAGIC S.`RULE_EXCLUSION`,
# MAGIC S.`/VSO/R_PALHGT`,
# MAGIC S.`/VSO/R_PAL_UL`,
# MAGIC S.`/VSO/R_PK_MAT`,
# MAGIC S.`/VSO/R_MATPAL`,
# MAGIC S.`/VSO/R_I_NO_LYR`,
# MAGIC S.`/VSO/R_ONE_MAT`,
# MAGIC S.`/VSO/R_ONE_SORT`,
# MAGIC S.`/VSO/R_ULD_SIDE`,
# MAGIC S.`/VSO/R_LOAD_PREF`,
# MAGIC S.`/VSO/R_DPOINT`,
# MAGIC S.`ALC`,
# MAGIC S.`PMT_OFFICE`,
# MAGIC S.`FEE_SCHEDULE`,
# MAGIC S.`DUNS`,
# MAGIC S.`DUNS4`,
# MAGIC S.`SAM_UE_ID`,
# MAGIC S.`SAM_EFT_IND`,
# MAGIC S.`PSOFG`,
# MAGIC S.`PSOIS`,
# MAGIC S.`PSON1`,
# MAGIC S.`PSON2`,
# MAGIC S.`PSON3`,
# MAGIC S.`PSOVN`,
# MAGIC S.`PSOTL`,
# MAGIC S.`PSOHS`,
# MAGIC S.`PSOST`,
# MAGIC S.`PSOO1`,
# MAGIC S.`PSOO2`,
# MAGIC S.`PSOO3`,
# MAGIC S.`PSOO4`,
# MAGIC S.`PSOO5`,
# MAGIC S.`J_1IEXCD`,
# MAGIC S.`J_1IEXRN`,
# MAGIC S.`J_1IEXRG`,
# MAGIC S.`J_1IEXDI`,
# MAGIC S.`J_1IEXCO`,
# MAGIC S.`J_1ICSTNO`,
# MAGIC S.`J_1ILSTNO`,
# MAGIC S.`J_1IPANNO`,
# MAGIC S.`J_1IEXCICU`,
# MAGIC S.`AEDAT`,
# MAGIC S.`USNAM`,
# MAGIC S.`J_1ISERN`,
# MAGIC S.`J_1IPANREF`,
# MAGIC S.`GST_TDS`,
# MAGIC S.`J_3GETYP`,
# MAGIC S.`J_3GREFTYP`,
# MAGIC S.`PSPNR`,
# MAGIC S.`COAUFNR`,
# MAGIC S.`J_3GAGEXT`,
# MAGIC S.`J_3GAGINT`,
# MAGIC S.`J_3GAGDUMI`,
# MAGIC S.`J_3GAGSTDI`,
# MAGIC S.`LGORT`,
# MAGIC S.`KOKRS`,
# MAGIC S.`KOSTL`,
# MAGIC S.`J_3GABGLG`,
# MAGIC S.`J_3GABGVG`,
# MAGIC S.`J_3GABRART`,
# MAGIC S.`J_3GSTDMON`,
# MAGIC S.`J_3GSTDTAG`,
# MAGIC S.`J_3GTAGMON`,
# MAGIC S.`J_3GZUGTAG`,
# MAGIC S.`J_3GMASCHB`,
# MAGIC S.`J_3GMEINSA`,
# MAGIC S.`J_3GKEINSA`,
# MAGIC S.`J_3GBLSPER`,
# MAGIC S.`J_3GKLEIVO`,
# MAGIC S.`J_3GCALID`,
# MAGIC S.`J_3GVMONAT`,
# MAGIC S.`J_3GABRKEN`,
# MAGIC S.`J_3GLABRECH`,
# MAGIC S.`J_3GAABRECH`,
# MAGIC S.`J_3GZUTVHLG`,
# MAGIC S.`J_3GNEGMEN`,
# MAGIC S.`J_3GFRISTLO`,
# MAGIC S.`J_3GEMINBE`,
# MAGIC S.`J_3GFMGUE`,
# MAGIC S.`J_3GZUSCHUE`,
# MAGIC S.`J_3GSCHPRS`,
# MAGIC S.`J_3GINVSTA`,
# MAGIC S.`/SAPCEM/DBER`,
# MAGIC S.`/SAPCEM/KVMEQ`,
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


