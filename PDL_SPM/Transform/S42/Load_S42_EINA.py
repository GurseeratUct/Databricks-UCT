# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'EINA'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

schema =  StructType([
                        StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('INFNR',LongType(),True) ,\
                        StructField('MATNR',StringType(),True) ,\
                        StructField('MATKL',StringType(),True) ,\
                        StructField('LIFNR',StringType(),True) ,\
                        StructField('LOEKZ',StringType(),True) ,\
                        StructField('ERDAT',StringType(),True) ,\
                        StructField('ERNAM',StringType(),True) ,\
                        StructField('TXZ01',StringType(),True) ,\
                        StructField('SORTL',StringType(),True) ,\
                        StructField('MEINS',StringType(),True) ,\
                        StructField('UMREZ',IntegerType(),True) ,\
                        StructField('UMREN',IntegerType(),True) ,\
                        StructField('IDNLF',StringType(),True) ,\
                        StructField('VERKF',StringType(),True) ,\
                        StructField('TELF1',StringType(),True) ,\
                        StructField('MAHN1',StringType(),True) ,\
                        StructField('MAHN2',IntegerType(),True) ,\
                        StructField('MAHN3',IntegerType(),True) ,\
                        StructField('URZNR',StringType(),True) ,\
                        StructField('URZDT',StringType(),True) ,\
                        StructField('URZLA',StringType(),True) ,\
                        StructField('URZTP',StringType(),True) ,\
                        StructField('URZZT',StringType(),True) ,\
                        StructField('LMEIN',StringType(),True) ,\
                        StructField('REGIO',StringType(),True) ,\
                        StructField('VABME',StringType(),True) ,\
                        StructField('LTSNR',StringType(),True) ,\
                        StructField('LTSSF',StringType(),True) ,\
                        StructField('WGLIF',StringType(),True) ,\
                        StructField('RUECK',StringType(),True) ,\
                        StructField('LIFAB',StringType(),True) ,\
                        StructField('LIFBI',StringType(),True) ,\
                        StructField('KOLIF',StringType(),True) ,\
                        StructField('ANZPU',StringType(),True) ,\
                        StructField('PUNEI',StringType(),True) ,\
                        StructField('RELIF',StringType(),True) ,\
                        StructField('MFRNR',StringType(),True) ,\
                        StructField('DUMMY_EINA_INCL_EEW_PS',StringType(),True) ,\
                        StructField('LASTCHANGEDATETIME',StringType(),True) ,\
                        StructField('ISEOPBLOCKED',StringType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',StringType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
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

df_transform = df_add_column.withColumn("ERDAT", to_date(regexp_replace(df_add_column.ERDAT,'\.','-'))) \
                            .withColumn("URZDT", to_date(regexp_replace(df_add_column.URZDT,'\.','-'))) \
                            .withColumn("LIFAB", to_date(regexp_replace(df_add_column.LIFAB,'\.','-'))) \
                            .withColumn("LIFBI", to_date(regexp_replace(df_add_column.LIFBI,'\.','-'))) \
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
# MAGIC MERGE INTO S42.EINA as S
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY INFNR ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_EINA)A where A.rn = 1 ) as T 
# MAGIC ON S.INFNR = T.INFNR 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC S.DI_SEQUENCE_NUMBER = T.DI_SEQUENCE_NUMBER,
# MAGIC S.DI_OPERATION_TYPE = T.DI_OPERATION_TYPE,
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.INFNR = T.INFNR,
# MAGIC S.MATNR = T.MATNR,
# MAGIC S.MATKL = T.MATKL,
# MAGIC S.LIFNR = T.LIFNR,
# MAGIC S.LOEKZ = T.LOEKZ,
# MAGIC S.ERDAT = T.ERDAT,
# MAGIC S.ERNAM = T.ERNAM,
# MAGIC S.TXZ01 = T.TXZ01,
# MAGIC S.SORTL = T.SORTL,
# MAGIC S.MEINS = T.MEINS,
# MAGIC S.UMREZ = T.UMREZ,
# MAGIC S.UMREN = T.UMREN,
# MAGIC S.IDNLF = T.IDNLF,
# MAGIC S.VERKF = T.VERKF,
# MAGIC S.TELF1 = T.TELF1,
# MAGIC S.MAHN1 = T.MAHN1,
# MAGIC S.MAHN2 = T.MAHN2,
# MAGIC S.MAHN3 = T.MAHN3,
# MAGIC S.URZNR = T.URZNR,
# MAGIC S.URZDT = T.URZDT,
# MAGIC S.URZLA = T.URZLA,
# MAGIC S.URZTP = T.URZTP,
# MAGIC S.URZZT = T.URZZT,
# MAGIC S.LMEIN = T.LMEIN,
# MAGIC S.REGIO = T.REGIO,
# MAGIC S.VABME = T.VABME,
# MAGIC S.LTSNR = T.LTSNR,
# MAGIC S.LTSSF = T.LTSSF,
# MAGIC S.WGLIF = T.WGLIF,
# MAGIC S.RUECK = T.RUECK,
# MAGIC S.LIFAB = T.LIFAB,
# MAGIC S.LIFBI = T.LIFBI,
# MAGIC S.KOLIF = T.KOLIF,
# MAGIC S.ANZPU = T.ANZPU,
# MAGIC S.PUNEI = T.PUNEI,
# MAGIC S.RELIF = T.RELIF,
# MAGIC S.MFRNR = T.MFRNR,
# MAGIC S.DUMMY_EINA_INCL_EEW_PS = T.DUMMY_EINA_INCL_EEW_PS,
# MAGIC S.LASTCHANGEDATETIME = T.LASTCHANGEDATETIME,
# MAGIC S.ISEOPBLOCKED = T.ISEOPBLOCKED,
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
# MAGIC MATNR,
# MAGIC MATKL,
# MAGIC LIFNR,
# MAGIC LOEKZ,
# MAGIC ERDAT,
# MAGIC ERNAM,
# MAGIC TXZ01,
# MAGIC SORTL,
# MAGIC MEINS,
# MAGIC UMREZ,
# MAGIC UMREN,
# MAGIC IDNLF,
# MAGIC VERKF,
# MAGIC TELF1,
# MAGIC MAHN1,
# MAGIC MAHN2,
# MAGIC MAHN3,
# MAGIC URZNR,
# MAGIC URZDT,
# MAGIC URZLA,
# MAGIC URZTP,
# MAGIC URZZT,
# MAGIC LMEIN,
# MAGIC REGIO,
# MAGIC VABME,
# MAGIC LTSNR,
# MAGIC LTSSF,
# MAGIC WGLIF,
# MAGIC RUECK,
# MAGIC LIFAB,
# MAGIC LIFBI,
# MAGIC KOLIF,
# MAGIC ANZPU,
# MAGIC PUNEI,
# MAGIC RELIF,
# MAGIC MFRNR,
# MAGIC DUMMY_EINA_INCL_EEW_PS,
# MAGIC LASTCHANGEDATETIME,
# MAGIC ISEOPBLOCKED,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC  )
# MAGIC  VALUES
# MAGIC  (
# MAGIC T.DI_SEQUENCE_NUMBER,
# MAGIC T.DI_OPERATION_TYPE,
# MAGIC T.MANDT ,
# MAGIC T.INFNR ,
# MAGIC T.MATNR ,
# MAGIC T.MATKL ,
# MAGIC T.LIFNR ,
# MAGIC T.LOEKZ ,
# MAGIC T.ERDAT ,
# MAGIC T.ERNAM ,
# MAGIC T.TXZ01 ,
# MAGIC T.SORTL ,
# MAGIC T.MEINS ,
# MAGIC T.UMREZ ,
# MAGIC T.UMREN ,
# MAGIC T.IDNLF ,
# MAGIC T.VERKF ,
# MAGIC T.TELF1 ,
# MAGIC T.MAHN1 ,
# MAGIC T.MAHN2 ,
# MAGIC T.MAHN3 ,
# MAGIC T.URZNR ,
# MAGIC T.URZDT ,
# MAGIC T.URZLA ,
# MAGIC T.URZTP ,
# MAGIC T.URZZT ,
# MAGIC T.LMEIN ,
# MAGIC T.REGIO ,
# MAGIC T.VABME ,
# MAGIC T.LTSNR ,
# MAGIC T.LTSSF ,
# MAGIC T.WGLIF ,
# MAGIC T.RUECK ,
# MAGIC T.LIFAB ,
# MAGIC T.LIFBI ,
# MAGIC T.KOLIF ,
# MAGIC T.ANZPU ,
# MAGIC T.PUNEI ,
# MAGIC T.RELIF ,
# MAGIC T.MFRNR ,
# MAGIC T.DUMMY_EINA_INCL_EEW_PS ,
# MAGIC T.LASTCHANGEDATETIME ,
# MAGIC T.ISEOPBLOCKED ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now() ,
# MAGIC 'SAP' 
# MAGIC )
# MAGIC  

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path,True)

