# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'ZTC2P_MAT_MAP_H'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

#df = spark.read.format(read_format) \
#      .option("header", True) \
#      .option("delimiter","^") \
#      .option("inferschema",True) \
#      .load(read_path)

# COMMAND ----------

schema = StructType([ \
                     StructField('MANDT',IntegerType(),True),\
StructField('MATNR',StringType(),True),\
StructField('REVLV',StringType(),True),\
StructField('AGILE_REV',StringType(),True),\
StructField('CE_FLAG',StringType(),True),\
StructField('CP_FLAG',StringType(),True),\
StructField('SL_FLAG',StringType(),True),\
StructField('ERDAT',StringType(),True),\
StructField('INT_REV',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
StructField('LandingFileTimeStamp',StringType(),True)
                    ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter","^") \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp, '-','')) \
                            .withColumn("ERDAT", to_date(regexp_replace(df_add_column.ERDAT,'\.','-')))
                            

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.ZTC2P_MAT_MAP_H as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,MATNR,REVLV ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_ZTC2P_MAT_MAP_H where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON 
# MAGIC T.MANDT = S.MANDT and 
# MAGIC T.MATNR = S.MATNR and
# MAGIC T.REVLV = S.REVLV 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`MANDT` =  S.`MANDT`,
# MAGIC T.`MATNR` =  S.`MATNR`,
# MAGIC T.`REVLV` =  S.`REVLV`,
# MAGIC T.`AGILE_REV` =  S.`AGILE_REV`,
# MAGIC T.`CE_FLAG` =  S.`CE_FLAG`,
# MAGIC T.`CP_FLAG` =  S.`CP_FLAG`,
# MAGIC T.`SL_FLAG` =  S.`SL_FLAG`,
# MAGIC T.`ERDAT` =  S.`ERDAT`,
# MAGIC T.`INT_REV` =  S.`INT_REV`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC  `MANDT`,
# MAGIC `MATNR`,
# MAGIC `REVLV`,
# MAGIC `AGILE_REV`,
# MAGIC `CE_FLAG`,
# MAGIC `CP_FLAG`,
# MAGIC `SL_FLAG`,
# MAGIC `ERDAT`,
# MAGIC `INT_REV`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC ) VALUES (
# MAGIC S.`MANDT`,
# MAGIC S.`MATNR`,
# MAGIC S.`REVLV`,
# MAGIC S.`AGILE_REV`,
# MAGIC S.`CE_FLAG`,
# MAGIC S.`CP_FLAG`,
# MAGIC S.`SL_FLAG`,
# MAGIC S.`ERDAT`,
# MAGIC S.`INT_REV`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


