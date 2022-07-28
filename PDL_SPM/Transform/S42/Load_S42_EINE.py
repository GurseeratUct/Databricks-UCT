# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'EINE'
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
                        StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('INFNR',LongType(),True) ,\
                        StructField('EKORG',StringType(),True) ,\
                        StructField('ESOKZ',IntegerType(),True) ,\
                        StructField('WERKS',StringType(),True) ,\
                        StructField('LOEKZ',StringType(),True) ,\
                        StructField('ERDAT',StringType(),True) ,\
                        StructField('ERNAM',StringType(),True) ,\
                        StructField('EKGRP',StringType(),True) ,\
                        StructField('WAERS',StringType(),True) ,\
                        StructField('BONUS',StringType(),True) ,\
                        StructField('MGBON',StringType(),True) ,\
                        StructField('MINBM',DoubleType(),True) ,\
                        StructField('NORBM',DoubleType(),True) ,\
                        StructField('APLFZ',IntegerType(),True) ,\
                        StructField('UEBTO',DoubleType(),True) ,\
                        StructField('UEBTK',StringType(),True) ,\
                        StructField('UNTTO',DoubleType(),True) ,\
                        StructField('ANGNR',StringType(),True) ,\
                        StructField('ANGDT',StringType(),True) ,\
                        StructField('ANFNR',StringType(),True) ,\
                        StructField('ANFPS',IntegerType(),True) ,\
                        StructField('ABSKZ',StringType(),True) ,\
                        StructField('AMODV',StringType(),True) ,\
                        StructField('AMODB',StringType(),True) ,\
                        StructField('AMOBM',DoubleType(),True) ,\
                        StructField('AMOBW',DoubleType(),True) ,\
                        StructField('AMOAM',DoubleType(),True) ,\
                        StructField('AMOAW',DoubleType(),True) ,\
                        StructField('AMORS',StringType(),True) ,\
                        StructField('BSTYP',StringType(),True) ,\
                        StructField('EBELN',StringType(),True) ,\
                        StructField('EBELP',IntegerType(),True) ,\
                        StructField('DATLB',StringType(),True) ,\
                        StructField('NETPR',DoubleType(),True) ,\
                        StructField('PEINH',IntegerType(),True) ,\
                        StructField('BPRME',StringType(),True) ,\
                        StructField('PRDAT',StringType(),True) ,\
                        StructField('BPUMZ',IntegerType(),True) ,\
                        StructField('BPUMN',IntegerType(),True) ,\
                        StructField('MTXNO',StringType(),True) ,\
                        StructField('WEBRE',StringType(),True) ,\
                        StructField('EFFPR',DoubleType(),True) ,\
                        StructField('EKKOL',StringType(),True) ,\
                        StructField('SKTOF',StringType(),True) ,\
                        StructField('KZABS',StringType(),True) ,\
                        StructField('MWSKZ',StringType(),True) ,\
                        StructField('TXDAT_FROM',StringType(),True) ,\
                        StructField('TAX_COUNTRY',StringType(),True) ,\
                        StructField('BWTAR',StringType(),True) ,\
                        StructField('EBONU',StringType(),True) ,\
                        StructField('EVERS',StringType(),True) ,\
                        StructField('EXPRF',StringType(),True) ,\
                        StructField('BSTAE',StringType(),True) ,\
                        StructField('MEPRF',StringType(),True) ,\
                        StructField('INCO1',StringType(),True) ,\
                        StructField('INCO2',StringType(),True) ,\
                        StructField('XERSN',StringType(),True) ,\
                        StructField('EBON2',StringType(),True) ,\
                        StructField('EBON3',StringType(),True) ,\
                        StructField('EBONF',StringType(),True) ,\
                        StructField('MHDRZ',IntegerType(),True) ,\
                        StructField('VERID',StringType(),True) ,\
                        StructField('BSTMA',DoubleType(),True) ,\
                        StructField('RDPRF',StringType(),True) ,\
                        StructField('MEGRU',StringType(),True) ,\
                        StructField('J_1BNBM',StringType(),True) ,\
                        StructField('SPE_CRE_REF_DOC',StringType(),True) ,\
                        StructField('IPRKZ',StringType(),True) ,\
                        StructField('CO_ORDER',StringType(),True) ,\
                        StructField('VENDOR_RMA_REQ',StringType(),True) ,\
                        StructField('DIFF_INVOICE',StringType(),True) ,\
                        StructField('INCOV',StringType(),True) ,\
                        StructField('INCO2_L',StringType(),True) ,\
                        StructField('INCO3_L',StringType(),True) ,\
                        StructField('AUT_SOURCE',StringType(),True) ,\
                        StructField('DUMMY_EINE_INCL_EEW_PS',StringType(),True) ,\
                        StructField('ISEOPBLOCKED',StringType(),True) ,\
                        StructField('FSH_DCI_CORR',StringType(),True) ,\
                        StructField('FSH_RLT',IntegerType(),True) ,\
                        StructField('FSH_MLT',IntegerType(),True) ,\
                        StructField('FSH_PLT',IntegerType(),True) ,\
                        StructField('FSH_TLT',IntegerType(),True) ,\
                        StructField('MRPIND',StringType(),True) ,\
                        StructField('SGT_SSREL',StringType(),True) ,\
                        StructField('TRANSPORT_CHAIN',StringType(),True) ,\
                        StructField('STAGING_TIME',IntegerType(),True) ,\
                        StructField('ZZEAU',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
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

df_transform = df_add_column.withColumn("ERDAT", to_date(regexp_replace(df_add_column.ERDAT,'\.','-'))) \
                            .withColumn("ANGDT", to_date(regexp_replace(df_add_column.ANGDT,'\.','-'))) \
                            .withColumn("AMODV", to_date(regexp_replace(df_add_column.AMODV,'\.','-'))) \
                            .withColumn("AMODB", to_date(regexp_replace(df_add_column.AMODB,'\.','-'))) \
                            .withColumn("DATLB", to_date(regexp_replace(df_add_column.DATLB,'\.','-'))) \
                            .withColumn("PRDAT", to_date(regexp_replace(df_add_column.PRDAT,'\.','-'))) \
                            .withColumn("TXDAT_FROM", to_date(regexp_replace(df_add_column.TXDAT_FROM,'\.','-'))) \
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
# MAGIC MERGE INTO S42.EINE as S
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY INFNR,EKORG,ESOKZ,WERKS ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_EINE)A where A.rn = 1 ) as T 
# MAGIC ON S.INFNR = T.INFNR and
# MAGIC S.EKORG = T.EKORG and
# MAGIC S.ESOKZ = T.ESOKZ  AND
# MAGIC S.WERKS = T.WERKS
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  S.DI_SEQUENCE_NUMBER = T.DI_SEQUENCE_NUMBER,
# MAGIC S.DI_OPERATION_TYPE = T.DI_OPERATION_TYPE,
# MAGIC  S.MANDT = T.MANDT,
# MAGIC S.INFNR = T.INFNR,
# MAGIC S.EKORG = T.EKORG,
# MAGIC S.ESOKZ = T.ESOKZ,
# MAGIC S.WERKS = T.WERKS,
# MAGIC S.LOEKZ = T.LOEKZ,
# MAGIC S.ERDAT = T.ERDAT,
# MAGIC S.ERNAM = T.ERNAM,
# MAGIC S.EKGRP = T.EKGRP,
# MAGIC S.WAERS = T.WAERS,
# MAGIC S.BONUS = T.BONUS,
# MAGIC S.MGBON = T.MGBON,
# MAGIC S.MINBM = T.MINBM,
# MAGIC S.NORBM = T.NORBM,
# MAGIC S.APLFZ = T.APLFZ,
# MAGIC S.UEBTO = T.UEBTO,
# MAGIC S.UEBTK = T.UEBTK,
# MAGIC S.UNTTO = T.UNTTO,
# MAGIC S.ANGNR = T.ANGNR,
# MAGIC S.ANGDT = T.ANGDT,
# MAGIC S.ANFNR = T.ANFNR,
# MAGIC S.ANFPS = T.ANFPS,
# MAGIC S.ABSKZ = T.ABSKZ,
# MAGIC S.AMODV = T.AMODV,
# MAGIC S.AMODB = T.AMODB,
# MAGIC S.AMOBM = T.AMOBM,
# MAGIC S.AMOBW = T.AMOBW,
# MAGIC S.AMOAM = T.AMOAM,
# MAGIC S.AMOAW = T.AMOAW,
# MAGIC S.AMORS = T.AMORS,
# MAGIC S.BSTYP = T.BSTYP,
# MAGIC S.EBELN = T.EBELN,
# MAGIC S.EBELP = T.EBELP,
# MAGIC S.DATLB = T.DATLB,
# MAGIC S.NETPR = T.NETPR,
# MAGIC S.PEINH = T.PEINH,
# MAGIC S.BPRME = T.BPRME,
# MAGIC S.PRDAT = T.PRDAT,
# MAGIC S.BPUMZ = T.BPUMZ,
# MAGIC S.BPUMN = T.BPUMN,
# MAGIC S.MTXNO = T.MTXNO,
# MAGIC S.WEBRE = T.WEBRE,
# MAGIC S.EFFPR = T.EFFPR,
# MAGIC S.EKKOL = T.EKKOL,
# MAGIC S.SKTOF = T.SKTOF,
# MAGIC S.KZABS = T.KZABS,
# MAGIC S.MWSKZ = T.MWSKZ,
# MAGIC S.TXDAT_FROM = T.TXDAT_FROM,
# MAGIC S.TAX_COUNTRY = T.TAX_COUNTRY,
# MAGIC S.BWTAR = T.BWTAR,
# MAGIC S.EBONU = T.EBONU,
# MAGIC S.EVERS = T.EVERS,
# MAGIC S.EXPRF = T.EXPRF,
# MAGIC S.BSTAE = T.BSTAE,
# MAGIC S.MEPRF = T.MEPRF,
# MAGIC S.INCO1 = T.INCO1,
# MAGIC S.INCO2 = T.INCO2,
# MAGIC S.XERSN = T.XERSN,
# MAGIC S.EBON2 = T.EBON2,
# MAGIC S.EBON3 = T.EBON3,
# MAGIC S.EBONF = T.EBONF,
# MAGIC S.MHDRZ = T.MHDRZ,
# MAGIC S.VERID = T.VERID,
# MAGIC S.BSTMA = T.BSTMA,
# MAGIC S.RDPRF = T.RDPRF,
# MAGIC S.MEGRU = T.MEGRU,
# MAGIC S.J_1BNBM = T.J_1BNBM,
# MAGIC S.SPE_CRE_REF_DOC = T.SPE_CRE_REF_DOC,
# MAGIC S.IPRKZ = T.IPRKZ,
# MAGIC S.CO_ORDER = T.CO_ORDER,
# MAGIC S.VENDOR_RMA_REQ = T.VENDOR_RMA_REQ,
# MAGIC S.DIFF_INVOICE = T.DIFF_INVOICE,
# MAGIC S.INCOV = T.INCOV,
# MAGIC S.INCO2_L = T.INCO2_L,
# MAGIC S.INCO3_L = T.INCO3_L,
# MAGIC S.AUT_SOURCE = T.AUT_SOURCE,
# MAGIC S.DUMMY_EINE_INCL_EEW_PS = T.DUMMY_EINE_INCL_EEW_PS,
# MAGIC S.ISEOPBLOCKED = T.ISEOPBLOCKED,
# MAGIC S.FSH_DCI_CORR = T.FSH_DCI_CORR,
# MAGIC S.FSH_RLT = T.FSH_RLT,
# MAGIC S.FSH_MLT = T.FSH_MLT,
# MAGIC S.FSH_PLT = T.FSH_PLT,
# MAGIC S.FSH_TLT = T.FSH_TLT,
# MAGIC S.MRPIND = T.MRPIND,
# MAGIC S.SGT_SSREL = T.SGT_SSREL,
# MAGIC S.TRANSPORT_CHAIN = T.TRANSPORT_CHAIN,
# MAGIC S.STAGING_TIME = T.STAGING_TIME,
# MAGIC S.ZZEAU = T.ZZEAU,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT 
# MAGIC  (
# MAGIC  DI_SEQUENCE_NUMBER,
# MAGIC  DI_OPERATION_TYPE,
# MAGIC  MANDT,
# MAGIC INFNR,
# MAGIC EKORG,
# MAGIC ESOKZ,
# MAGIC WERKS,
# MAGIC LOEKZ,
# MAGIC ERDAT,
# MAGIC ERNAM,
# MAGIC EKGRP,
# MAGIC WAERS,
# MAGIC BONUS,
# MAGIC MGBON,
# MAGIC MINBM,
# MAGIC NORBM,
# MAGIC APLFZ,
# MAGIC UEBTO,
# MAGIC UEBTK,
# MAGIC UNTTO,
# MAGIC ANGNR,
# MAGIC ANGDT,
# MAGIC ANFNR,
# MAGIC ANFPS,
# MAGIC ABSKZ,
# MAGIC AMODV,
# MAGIC AMODB,
# MAGIC AMOBM,
# MAGIC AMOBW,
# MAGIC AMOAM,
# MAGIC AMOAW,
# MAGIC AMORS,
# MAGIC BSTYP,
# MAGIC EBELN,
# MAGIC EBELP,
# MAGIC DATLB,
# MAGIC NETPR,
# MAGIC PEINH,
# MAGIC BPRME,
# MAGIC PRDAT,
# MAGIC BPUMZ,
# MAGIC BPUMN,
# MAGIC MTXNO,
# MAGIC WEBRE,
# MAGIC EFFPR,
# MAGIC EKKOL,
# MAGIC SKTOF,
# MAGIC KZABS,
# MAGIC MWSKZ,
# MAGIC TXDAT_FROM,
# MAGIC TAX_COUNTRY,
# MAGIC BWTAR,
# MAGIC EBONU,
# MAGIC EVERS,
# MAGIC EXPRF,
# MAGIC BSTAE,
# MAGIC MEPRF,
# MAGIC INCO1,
# MAGIC INCO2,
# MAGIC XERSN,
# MAGIC EBON2,
# MAGIC EBON3,
# MAGIC EBONF,
# MAGIC MHDRZ,
# MAGIC VERID,
# MAGIC BSTMA,
# MAGIC RDPRF,
# MAGIC MEGRU,
# MAGIC J_1BNBM,
# MAGIC SPE_CRE_REF_DOC,
# MAGIC IPRKZ,
# MAGIC CO_ORDER,
# MAGIC VENDOR_RMA_REQ,
# MAGIC DIFF_INVOICE,
# MAGIC INCOV,
# MAGIC INCO2_L,
# MAGIC INCO3_L,
# MAGIC AUT_SOURCE,
# MAGIC DUMMY_EINE_INCL_EEW_PS,
# MAGIC ISEOPBLOCKED,
# MAGIC FSH_DCI_CORR,
# MAGIC FSH_RLT,
# MAGIC FSH_MLT,
# MAGIC FSH_PLT,
# MAGIC FSH_TLT,
# MAGIC MRPIND,
# MAGIC SGT_SSREL,
# MAGIC TRANSPORT_CHAIN,
# MAGIC STAGING_TIME,
# MAGIC ZZEAU,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC  )
# MAGIC  VALUES
# MAGIC  (
# MAGIC  T.DI_SEQUENCE_NUMBER,
# MAGIC  T.DI_OPERATION_TYPE,
# MAGIC  T.MANDT ,
# MAGIC T.INFNR ,
# MAGIC T.EKORG ,
# MAGIC T.ESOKZ ,
# MAGIC T.WERKS ,
# MAGIC T.LOEKZ ,
# MAGIC T.ERDAT ,
# MAGIC T.ERNAM ,
# MAGIC T.EKGRP ,
# MAGIC T.WAERS ,
# MAGIC T.BONUS ,
# MAGIC T.MGBON ,
# MAGIC T.MINBM ,
# MAGIC T.NORBM ,
# MAGIC T.APLFZ ,
# MAGIC T.UEBTO ,
# MAGIC T.UEBTK ,
# MAGIC T.UNTTO ,
# MAGIC T.ANGNR ,
# MAGIC T.ANGDT ,
# MAGIC T.ANFNR ,
# MAGIC T.ANFPS ,
# MAGIC T.ABSKZ ,
# MAGIC T.AMODV ,
# MAGIC T.AMODB ,
# MAGIC T.AMOBM ,
# MAGIC T.AMOBW ,
# MAGIC T.AMOAM ,
# MAGIC T.AMOAW ,
# MAGIC T.AMORS ,
# MAGIC T.BSTYP ,
# MAGIC T.EBELN ,
# MAGIC T.EBELP ,
# MAGIC T.DATLB ,
# MAGIC T.NETPR ,
# MAGIC T.PEINH ,
# MAGIC T.BPRME ,
# MAGIC T.PRDAT ,
# MAGIC T.BPUMZ ,
# MAGIC T.BPUMN ,
# MAGIC T.MTXNO ,
# MAGIC T.WEBRE ,
# MAGIC T.EFFPR ,
# MAGIC T.EKKOL ,
# MAGIC T.SKTOF ,
# MAGIC T.KZABS ,
# MAGIC T.MWSKZ ,
# MAGIC T.TXDAT_FROM ,
# MAGIC T.TAX_COUNTRY ,
# MAGIC T.BWTAR ,
# MAGIC T.EBONU ,
# MAGIC T.EVERS ,
# MAGIC T.EXPRF ,
# MAGIC T.BSTAE ,
# MAGIC T.MEPRF ,
# MAGIC T.INCO1 ,
# MAGIC T.INCO2 ,
# MAGIC T.XERSN ,
# MAGIC T.EBON2 ,
# MAGIC T.EBON3 ,
# MAGIC T.EBONF ,
# MAGIC T.MHDRZ ,
# MAGIC T.VERID ,
# MAGIC T.BSTMA ,
# MAGIC T.RDPRF ,
# MAGIC T.MEGRU ,
# MAGIC T.J_1BNBM ,
# MAGIC T.SPE_CRE_REF_DOC ,
# MAGIC T.IPRKZ ,
# MAGIC T.CO_ORDER ,
# MAGIC T.VENDOR_RMA_REQ ,
# MAGIC T.DIFF_INVOICE ,
# MAGIC T.INCOV ,
# MAGIC T.INCO2_L ,
# MAGIC T.INCO3_L ,
# MAGIC T.AUT_SOURCE ,
# MAGIC T.DUMMY_EINE_INCL_EEW_PS ,
# MAGIC T.ISEOPBLOCKED ,
# MAGIC T.FSH_DCI_CORR ,
# MAGIC T.FSH_RLT ,
# MAGIC T.FSH_MLT ,
# MAGIC T.FSH_PLT ,
# MAGIC T.FSH_TLT ,
# MAGIC T.MRPIND ,
# MAGIC T.SGT_SSREL ,
# MAGIC T.TRANSPORT_CHAIN ,
# MAGIC T.STAGING_TIME ,
# MAGIC T.ZZEAU ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC 'S42'
# MAGIC  )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


