# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'PART_UNITS_CONV'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE70'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

schema = StructType([ \
                        StructField('ROWID',IntegerType(),True) ,\
                        StructField('PART_ID',StringType(),True) ,\
                        StructField('FROM_UM',StringType(),True) ,\
                        StructField('TO_UM',StringType(),True) ,\
                        StructField('CONVERSION_FACTOR',DoubleType(),True) ,\
                        StructField('LandingFileTimeStamp',StringType(),True) \
                       ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE70'))

# COMMAND ----------

df_transform = df_add_column.na.fill(0).withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-',''))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE70.PART_UNITS_CONV as S
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY PART_ID, FROM_UM, TO_UM ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_PART_UNITS_CONV)A where A.rn = 1 ) as T
# MAGIC ON S.PART_ID = T.PART_ID 
# MAGIC AND S.FROM_UM = T.FROM_UM
# MAGIC AND  S.TO_UM = T.TO_UM
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  S.ROWID = T.ROWID,
# MAGIC S.PART_ID = T.PART_ID,
# MAGIC S.FROM_UM = T.FROM_UM,
# MAGIC S.TO_UM = T.TO_UM,
# MAGIC S.CONVERSION_FACTOR = T.CONVERSION_FACTOR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT
# MAGIC (
# MAGIC   ROWID,
# MAGIC   PART_ID,
# MAGIC   FROM_UM,
# MAGIC   TO_UM,
# MAGIC   CONVERSION_FACTOR,
# MAGIC   LandingFileTimeStamp,
# MAGIC   UpdatedOn,
# MAGIC   DataSource
# MAGIC )
# MAGIC VALUES 
# MAGIC (
# MAGIC   T.ROWID ,
# MAGIC   T.PART_ID ,
# MAGIC   T.FROM_UM ,
# MAGIC   T.TO_UM ,
# MAGIC   T.CONVERSION_FACTOR ,
# MAGIC   T.LandingFileTimeStamp ,
# MAGIC   now(),
# MAGIC   'VE70'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


