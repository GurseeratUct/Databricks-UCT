# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'CURRENCY_EXCHANGE'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE72'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.options(header='True', inferSchema='True', delimiter='^').csv(read_path)

# COMMAND ----------

schema = StructType([ \
StructField('ROWID',StringType(),True),\
StructField('CURRENCY_ID',StringType(),True),\
StructField('EFFECTIVE_DATE',StringType(),True),\
StructField('SELL_RATE',StringType(),True),\
StructField('BUY_RATE',StringType(),True),\
                    ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE72')).withColumn('LandingFileTimeStamp',lit(current_timestamp()))

# COMMAND ----------

df_transform = df_add_column.na.fill(0).withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
                                       .withColumn("EFFECTIVE_DATE", to_timestamp(regexp_replace(df_add_column.EFFECTIVE_DATE,'\.','-'))) \
                                        

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE72.CURRENCY_EXCHANGE as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY CURRENCY_ID,EFFECTIVE_DATE ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_CURRENCY_EXCHANGE)A where A.rn = 1) as S 
# MAGIC ON T.CURRENCY_ID = S.CURRENCY_ID
# MAGIC and T.EFFECTIVE_DATE = S.EFFECTIVE_DATE
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.ROWID = S.ROWID,
# MAGIC  T.CURRENCY_ID = S.CURRENCY_ID,
# MAGIC  T.EFFECTIVE_DATE = S.EFFECTIVE_DATE,
# MAGIC  T.SELL_RATE = S.SELL_RATE,
# MAGIC  T.BUY_RATE = S.BUY_RATE,
# MAGIC  T.LandingFileTimeStamp =  S.LandingFileTimeStamp,
# MAGIC  T.UpdatedOn =  now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC ROWID,
# MAGIC CURRENCY_ID,
# MAGIC EFFECTIVE_DATE,
# MAGIC SELL_RATE,
# MAGIC BUY_RATE,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.ROWID,
# MAGIC S.CURRENCY_ID,
# MAGIC S.EFFECTIVE_DATE,
# MAGIC S.SELL_RATE,
# MAGIC S.BUY_RATE,
# MAGIC S.LandingFileTimeStamp,
# MAGIC now(),
# MAGIC 'VE70'
# MAGIC )
# MAGIC     
# MAGIC 
# MAGIC     

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)
