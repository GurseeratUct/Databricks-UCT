# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'PLMZ'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

schema =  StructType([\
                        StructField('DI_SEQUENCE_NUMBER',StringType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',StringType(),True) ,\
                        StructField('PLNTY',StringType(),True) ,\
                        StructField('PLNNR',StringType(),True) ,\
                        StructField('ZUONR',StringType(),True) ,\
                        StructField('ZAEHL',StringType(),True) ,\
                        StructField('DATUV',StringType(),True) ,\
                        StructField('TECHV',StringType(),True) ,\
                        StructField('AENNR',StringType(),True) ,\
                        StructField('LOEKZ',StringType(),True) ,\
                        StructField('PARKZ',StringType(),True) ,\
                        StructField('PLNAL',StringType(),True) ,\
                        StructField('PLNFL',StringType(),True) ,\
                        StructField('PLNKN',StringType(),True) ,\
                        StructField('STLTY',StringType(),True) ,\
                        StructField('STLNR',StringType(),True) ,\
                        StructField('STLAL',StringType(),True) ,\
                        StructField('STLKN',StringType(),True) ,\
                        StructField('WERK_STL',StringType(),True) ,\
                        StructField('ZUDIV',StringType(),True) ,\
                        StructField('ZUMS1',StringType(),True) ,\
                        StructField('ZUMS2',StringType(),True) ,\
                        StructField('ZUMS3',StringType(),True) ,\
                        StructField('ZUMEI',StringType(),True) ,\
                        StructField('IMENG',StringType(),True) ,\
                        StructField('IMEIN',StringType(),True) ,\
                        StructField('ANDAT',StringType(),True) ,\
                        StructField('ANNAM',StringType(),True) ,\
                        StructField('AEDAT',StringType(),True) ,\
                        StructField('AENAM',StringType(),True) ,\
                        StructField('RGEKZ',StringType(),True) ,\
                        StructField('STLST',StringType(),True) ,\
                        StructField('STLWG',StringType(),True) ,\
                        StructField('REFKN',StringType(),True) ,\
                        StructField('GP_MATNR',StringType(),True) ,\
                        StructField('GP_WERKS',StringType(),True) ,\
                        StructField('GP_UVORN',StringType(),True) ,\
                        StructField('GP_KRIT1',StringType(),True) ,\
                        StructField('GP_FREET',StringType(),True) ,\
                        StructField('AOBAR',StringType(),True) ,\
                        StructField('ZEINH',StringType(),True) ,\
                        StructField('DAUER',StringType(),True) ,\
                        StructField('DMENG',StringType(),True) ,\
                        StructField('KNTTP',StringType(),True) ,\
                        StructField('FLGEX',StringType(),True) ,\
                        StructField('VORAB',StringType(),True) ,\
                        StructField('STRECKE',StringType(),True) ,\
                        StructField('STLTY_W',StringType(),True) ,\
                        StructField('STLNR_W',StringType(),True) ,\
                        StructField('STLAL_W',StringType(),True) ,\
                        StructField('KANTE',StringType(),True) ,\
                        StructField('LGORT',StringType(),True) ,\
                        StructField('DISP',StringType(),True) ,\
                        StructField('PRODFLOWID',StringType(),True) ,\
                        StructField('BEIKZ',StringType(),True) ,\
                        StructField('ABLAD',StringType(),True) ,\
                        StructField('WEMPF',StringType(),True) ,\
                        StructField('VALID_TO',StringType(),True) ,\
                        StructField('LOEKZ_INHERITED',StringType(),True) ,\
                        StructField('VERSN',StringType(),True) ,\
                        StructField('VERSN_SOURCE',StringType(),True) ,\
                        StructField('VERSN_SOURCE_ZUONR',StringType(),True) ,\
                        StructField('BOM_VERSN',StringType(),True) ,\
                        StructField('BOM_VERSN_W',StringType(),True) ,\
                        StructField('LOG_COMP',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) ])

# COMMAND ----------

df_PLMZ = spark.read.format(read_format) \
          .option("header", True) \
          .option("delimiter","^") \
          .schema(schema)  \
          .load(read_path)

# COMMAND ----------

df_PLMZ.count()

# COMMAND ----------

df_add_column = df_PLMZ.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(regexp_replace(df_add_column.LandingFileTimeStamp, '_', ''),'-','')) \
                            .withColumn( "DATUV",to_date(regexp_replace(df_add_column.DATUV,'\.','-'))) \
                            .withColumn( "ANDAT",to_date(regexp_replace(df_add_column.ANDAT,'\.','-'))) \
                            .withColumn( "AEDAT",to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
                            .withColumn( "VALID_TO",to_date(regexp_replace(df_add_column.VALID_TO,'\.','-'))) 



# COMMAND ----------

df_transform.display()

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO S42.PLMZ  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,PLNTY,PLNNR,ZUONR,ZAEHL  ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_PLMZ)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.MANDT = T.MANDT 
# MAGIC and 
# MAGIC S.PLNTY = T.PLNTY
# MAGIC and
# MAGIC S.PLNNR = T.PLNNR
# MAGIC and
# MAGIC S.ZUONR = T.ZUONR
# MAGIC and 
# MAGIC S.ZAEHL = T.ZAEHL
# MAGIC WHEN  MATCHED 
# MAGIC THEN UPDATE SET
# MAGIC S.DI_SEQUENCE_NUMBER = T.DI_SEQUENCE_NUMBER,
# MAGIC S.DI_OPERATION_TYPE = T.DI_OPERATION_TYPE,
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.PLNTY = T.PLNTY,
# MAGIC S.PLNNR = T.PLNNR,
# MAGIC S.ZUONR = T.ZUONR,
# MAGIC S.ZAEHL = T.ZAEHL,
# MAGIC S.DATUV = T.DATUV,
# MAGIC S.TECHV = T.TECHV,
# MAGIC S.AENNR = T.AENNR,
# MAGIC S.LOEKZ = T.LOEKZ,
# MAGIC S.PARKZ = T.PARKZ,
# MAGIC S.PLNAL = T.PLNAL,
# MAGIC S.PLNFL = T.PLNFL,
# MAGIC S.PLNKN = T.PLNKN,
# MAGIC S.STLTY = T.STLTY,
# MAGIC S.STLNR = T.STLNR,
# MAGIC S.STLAL = T.STLAL,
# MAGIC S.STLKN = T.STLKN,
# MAGIC S.WERK_STL = T.WERK_STL,
# MAGIC S.ZUDIV = T.ZUDIV,
# MAGIC S.ZUMS1 = T.ZUMS1,
# MAGIC S.ZUMS2 = T.ZUMS2,
# MAGIC S.ZUMS3 = T.ZUMS3,
# MAGIC S.ZUMEI = T.ZUMEI,
# MAGIC S.IMENG = T.IMENG,
# MAGIC S.IMEIN = T.IMEIN,
# MAGIC S.ANDAT = T.ANDAT,
# MAGIC S.ANNAM = T.ANNAM,
# MAGIC S.AEDAT = T.AEDAT,
# MAGIC S.AENAM = T.AENAM,
# MAGIC S.RGEKZ = T.RGEKZ,
# MAGIC S.STLST = T.STLST,
# MAGIC S.STLWG = T.STLWG,
# MAGIC S.REFKN = T.REFKN,
# MAGIC S.GP_MATNR = T.GP_MATNR,
# MAGIC S.GP_WERKS = T.GP_WERKS,
# MAGIC S.GP_UVORN = T.GP_UVORN,
# MAGIC S.GP_KRIT1 = T.GP_KRIT1,
# MAGIC S.GP_FREET = T.GP_FREET,
# MAGIC S.AOBAR = T.AOBAR,
# MAGIC S.ZEINH = T.ZEINH,
# MAGIC S.DAUER = T.DAUER,
# MAGIC S.DMENG = T.DMENG,
# MAGIC S.KNTTP = T.KNTTP,
# MAGIC S.FLGEX = T.FLGEX,
# MAGIC S.VORAB = T.VORAB,
# MAGIC S.STRECKE = T.STRECKE,
# MAGIC S.STLTY_W = T.STLTY_W,
# MAGIC S.STLNR_W = T.STLNR_W,
# MAGIC S.STLAL_W = T.STLAL_W,
# MAGIC S.KANTE = T.KANTE,
# MAGIC S.LGORT = T.LGORT,
# MAGIC S.DISP = T.DISP,
# MAGIC S.PRODFLOWID = T.PRODFLOWID,
# MAGIC S.BEIKZ = T.BEIKZ,
# MAGIC S.ABLAD = T.ABLAD,
# MAGIC S.WEMPF = T.WEMPF,
# MAGIC S.VALID_TO = T.VALID_TO,
# MAGIC S.LOEKZ_INHERITED = T.LOEKZ_INHERITED,
# MAGIC S.VERSN = T.VERSN,
# MAGIC S.VERSN_SOURCE = T.VERSN_SOURCE,
# MAGIC S.VERSN_SOURCE_ZUONR = T.VERSN_SOURCE_ZUONR,
# MAGIC S.BOM_VERSN = T.BOM_VERSN,
# MAGIC S.BOM_VERSN_W = T.BOM_VERSN_W,
# MAGIC S.LOG_COMP = T.LOG_COMP,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED THEN INSERT
# MAGIC (
# MAGIC DI_SEQUENCE_NUMBER,
# MAGIC DI_OPERATION_TYPE,
# MAGIC MANDT,
# MAGIC PLNTY,
# MAGIC PLNNR,
# MAGIC ZUONR,
# MAGIC ZAEHL,
# MAGIC DATUV,
# MAGIC TECHV,
# MAGIC AENNR,
# MAGIC LOEKZ,
# MAGIC PARKZ,
# MAGIC PLNAL,
# MAGIC PLNFL,
# MAGIC PLNKN,
# MAGIC STLTY,
# MAGIC STLNR,
# MAGIC STLAL,
# MAGIC STLKN,
# MAGIC WERK_STL,
# MAGIC ZUDIV,
# MAGIC ZUMS1,
# MAGIC ZUMS2,
# MAGIC ZUMS3,
# MAGIC ZUMEI,
# MAGIC IMENG,
# MAGIC IMEIN,
# MAGIC ANDAT,
# MAGIC ANNAM,
# MAGIC AEDAT,
# MAGIC AENAM,
# MAGIC RGEKZ,
# MAGIC STLST,
# MAGIC STLWG,
# MAGIC REFKN,
# MAGIC GP_MATNR,
# MAGIC GP_WERKS,
# MAGIC GP_UVORN,
# MAGIC GP_KRIT1,
# MAGIC GP_FREET,
# MAGIC AOBAR,
# MAGIC ZEINH,
# MAGIC DAUER,
# MAGIC DMENG,
# MAGIC KNTTP,
# MAGIC FLGEX,
# MAGIC VORAB,
# MAGIC STRECKE,
# MAGIC STLTY_W,
# MAGIC STLNR_W,
# MAGIC STLAL_W,
# MAGIC KANTE,
# MAGIC LGORT,
# MAGIC DISP,
# MAGIC PRODFLOWID,
# MAGIC BEIKZ,
# MAGIC ABLAD,
# MAGIC WEMPF,
# MAGIC VALID_TO,
# MAGIC LOEKZ_INHERITED,
# MAGIC VERSN,
# MAGIC VERSN_SOURCE,
# MAGIC VERSN_SOURCE_ZUONR,
# MAGIC BOM_VERSN,
# MAGIC BOM_VERSN_W,
# MAGIC LOG_COMP,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.DI_SEQUENCE_NUMBER ,
# MAGIC T.DI_OPERATION_TYPE ,
# MAGIC T.MANDT ,
# MAGIC T.PLNTY ,
# MAGIC T.PLNNR ,
# MAGIC T.ZUONR ,
# MAGIC T.ZAEHL ,
# MAGIC T.DATUV ,
# MAGIC T.TECHV ,
# MAGIC T.AENNR ,
# MAGIC T.LOEKZ ,
# MAGIC T.PARKZ ,
# MAGIC T.PLNAL ,
# MAGIC T.PLNFL ,
# MAGIC T.PLNKN ,
# MAGIC T.STLTY ,
# MAGIC T.STLNR ,
# MAGIC T.STLAL ,
# MAGIC T.STLKN ,
# MAGIC T.WERK_STL ,
# MAGIC T.ZUDIV ,
# MAGIC T.ZUMS1 ,
# MAGIC T.ZUMS2 ,
# MAGIC T.ZUMS3 ,
# MAGIC T.ZUMEI ,
# MAGIC T.IMENG ,
# MAGIC T.IMEIN ,
# MAGIC T.ANDAT ,
# MAGIC T.ANNAM ,
# MAGIC T.AEDAT ,
# MAGIC T.AENAM ,
# MAGIC T.RGEKZ ,
# MAGIC T.STLST ,
# MAGIC T.STLWG ,
# MAGIC T.REFKN ,
# MAGIC T.GP_MATNR ,
# MAGIC T.GP_WERKS ,
# MAGIC T.GP_UVORN ,
# MAGIC T.GP_KRIT1 ,
# MAGIC T.GP_FREET ,
# MAGIC T.AOBAR ,
# MAGIC T.ZEINH ,
# MAGIC T.DAUER ,
# MAGIC T.DMENG ,
# MAGIC T.KNTTP ,
# MAGIC T.FLGEX ,
# MAGIC T.VORAB ,
# MAGIC T.STRECKE ,
# MAGIC T.STLTY_W ,
# MAGIC T.STLNR_W ,
# MAGIC T.STLAL_W ,
# MAGIC T.KANTE ,
# MAGIC T.LGORT ,
# MAGIC T.DISP ,
# MAGIC T.PRODFLOWID ,
# MAGIC T.BEIKZ ,
# MAGIC T.ABLAD ,
# MAGIC T.WEMPF ,
# MAGIC T.VALID_TO ,
# MAGIC T.LOEKZ_INHERITED ,
# MAGIC T.VERSN ,
# MAGIC T.VERSN_SOURCE ,
# MAGIC T.VERSN_SOURCE_ZUONR ,
# MAGIC T.BOM_VERSN ,
# MAGIC T.BOM_VERSN_W ,
# MAGIC T.LOG_COMP ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from s42.plmz
# MAGIC where 
# MAGIC plnnr in 
# MAGIC (
# MAGIC '50060492',
# MAGIC '50060493',
# MAGIC '50060494',
# MAGIC '50060495',
# MAGIC '50060496'
# MAGIC 
# MAGIC );

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


