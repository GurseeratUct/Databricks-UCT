# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'T16FB'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
#read_path = '/mnt/uct-landing-gen-dev/SAP/'+table_name+'/'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

schema  =  StructType([ \
                        StructField('DI_SEQUENCE_NUMBER',StringType(),True) ,\
                        StructField('DI_OPERATION_TYPE',StringType(),True) ,\
                        StructField('MANDT',StringType(),True) ,\
                        StructField('FRGKE',StringType(),True) ,\
                        StructField('KZFRE',StringType(),True) ,\
                        StructField('KZFAE',IntegerType(),True) ,\
                        StructField('TLFAE',IntegerType(),True) ,\
                        StructField('ODQ_CHANGEMODE',IntegerType(),True) ,\
                        StructField('ODQ_ENTITYCNTR',IntegerType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
                       ])

# COMMAND ----------

df_T16FB = spark.read.format(read_format) \
          .option("header", True) \
          .option("delimiter","^") \
          .schema(schema) \
          .load(read_path)

# COMMAND ----------

df_T16FB.show()

# COMMAND ----------

df_add_column = df_T16FB.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(regexp_replace(df_add_column.LandingFileTimeStamp, '_', ''),'-',''))  

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
# MAGIC MERGE INTO S42.T16FB  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY MANDT,FRGKE ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T16FB)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.MANDT = T.MANDT 
# MAGIC and 
# MAGIC S.FRGKE = T.FRGKE
# MAGIC WHEN MATCHED THEN UPDATE 
# MAGIC SET
# MAGIC S.DI_SEQUENCE_NUMBER = T.DI_SEQUENCE_NUMBER,
# MAGIC S.DI_OPERATION_TYPE = T.DI_OPERATION_TYPE,
# MAGIC S.MANDT = T.MANDT,
# MAGIC S.FRGKE = T.FRGKE,
# MAGIC S.KZFRE = T.KZFRE,
# MAGIC S.KZFAE = T.KZFAE,
# MAGIC S.TLFAE = T.TLFAE,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC DI_SEQUENCE_NUMBER,
# MAGIC DI_OPERATION_TYPE,
# MAGIC MANDT,
# MAGIC FRGKE,
# MAGIC KZFRE,
# MAGIC KZFAE,
# MAGIC TLFAE,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.DI_SEQUENCE_NUMBER ,
# MAGIC T.DI_OPERATION_TYPE ,
# MAGIC T.MANDT ,
# MAGIC T.FRGKE ,
# MAGIC T.KZFRE ,
# MAGIC T.KZFAE ,
# MAGIC T.TLFAE ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


