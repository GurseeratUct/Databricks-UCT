# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'EIPA'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

schema  = StructType([ \
                        StructField('DI_SEQUENCE_NUMBER',IntegerType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',IntegerType(),True) ,\
                        StructField('INFNR',LongType(),True) ,\
                        StructField('EBELN',LongType(),True) ,\
                        StructField('EBELP',IntegerType(),True) ,\
                        StructField('ESOKZ',IntegerType(),True) ,\
                        StructField('WERKS',StringType(),True) ,\
                        StructField('EKORG',StringType(),True) ,\
                        StructField('BEDAT',StringType(),True) ,\
                        StructField('PREIS',DoubleType(),True) ,\
                        StructField('PEINH',IntegerType(),True) ,\
                        StructField('BPRME',StringType(),True) ,\
                        StructField('BWAER',StringType(),True) ,\
                        StructField('LPREI',DoubleType(),True) ,\
                        StructField('LPEIN',IntegerType(),True) ,\
                        StructField('LMEIN',StringType(),True) ,\
                        StructField('LWAER',StringType(),True) ,\
                        StructField('MENGE',DoubleType(),True) ,\
                        StructField('ODQ_CHANGEMODE',StringType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True)])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('S42')) 

# COMMAND ----------

df_transform = df_add_column.withColumn("BEDAT", to_date(regexp_replace(df_add_column.BEDAT,'\.','-'))) \
                            .withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
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
# MAGIC MERGE INTO S42.EIPA as S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,INFNR,EBELN,EBELP ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_EIPA)A where A.rn = 1 ) as T 
# MAGIC ON S.MANDT = T.MANDT and
# MAGIC S.INFNR = T.INFNR and
# MAGIC S.EBELN = T.EBELN  AND
# MAGIC S.EBELP = T.EBELP
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.INFNR = T.INFNR,
# MAGIC S.EBELN = T.EBELN,
# MAGIC S.EBELP = T.EBELP,
# MAGIC S.ESOKZ = T.ESOKZ,
# MAGIC S.WERKS = T.WERKS,
# MAGIC S.EKORG = T.EKORG,
# MAGIC S.BEDAT = T.BEDAT,
# MAGIC S.PREIS = T.PREIS,
# MAGIC S.PEINH = T.PEINH,
# MAGIC S.BPRME = T.BPRME,
# MAGIC S.BWAER = T.BWAER,
# MAGIC S.LPREI = T.LPREI,
# MAGIC S.LPEIN = T.LPEIN,
# MAGIC S.LMEIN = T.LMEIN,
# MAGIC S.LWAER = T.LWAER,
# MAGIC S.MENGE = T.MENGE,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT 
# MAGIC     (MANDT,
# MAGIC INFNR,
# MAGIC EBELN,
# MAGIC EBELP,
# MAGIC ESOKZ,
# MAGIC WERKS,
# MAGIC EKORG,
# MAGIC BEDAT,
# MAGIC PREIS,
# MAGIC PEINH,
# MAGIC BPRME,
# MAGIC BWAER,
# MAGIC LPREI,
# MAGIC LPEIN,
# MAGIC LMEIN,
# MAGIC LWAER,
# MAGIC MENGE,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource)
# MAGIC VALUES
# MAGIC (T.MANDT ,
# MAGIC T.INFNR ,
# MAGIC T.EBELN ,
# MAGIC T.EBELP ,
# MAGIC T.ESOKZ ,
# MAGIC T.WERKS ,
# MAGIC T.EKORG ,
# MAGIC T.BEDAT ,
# MAGIC T.PREIS ,
# MAGIC T.PEINH ,
# MAGIC T.BPRME ,
# MAGIC T.BWAER ,
# MAGIC T.LPREI ,
# MAGIC T.LPEIN ,
# MAGIC T.LMEIN ,
# MAGIC T.LWAER ,
# MAGIC T.MENGE ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC 'SAP') ;

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


