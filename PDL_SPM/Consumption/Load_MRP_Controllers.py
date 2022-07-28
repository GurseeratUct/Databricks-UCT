# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'MRP_Controllers'
table_name = 'T024D' 
read_format = 'delta'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
stage_table = 'stg_'+table_name
database_name = 'FEDW'

# COMMAND ----------

col_names = [
'MANDT',
'WERKS',
'DISPO',
'DSNAM',
'DSTEL',
'EKGRP',
'MEMPF',
'GSBER',
'PRCTR',
'USRTYP',
'USRKEY',
'LandingFileTimeStamp',
'UpdatedOn',
'DataSource',
]

# COMMAND ----------

df = spark.sql("select * from S42.T024D where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T024D')")

# COMMAND ----------


df_ts = df.agg({"UpdatedOn": "max"}).collect()[0]
ts = df_ts["max(UpdatedOn)"]
print(ts)

# COMMAND ----------

df_select_col = df.select(col_names)
df_rename =    df_select_col.withColumnRenamed('MANDT','Client') \
                            .withColumnRenamed('WERKS','Plant') \
                            .withColumnRenamed('DISPO','MRPController') \
                            .withColumnRenamed('DSNAM','MRPControllerName') \
                            .withColumnRenamed('DSTEL','MRPControllerTel') \
                            .withColumnRenamed('EKGRP','PurchasingGroup') \
                            .withColumnRenamed('MEMPF','RecipientName') \
                            .withColumnRenamed('GSBER','BusinessArea') \
                            .withColumnRenamed('PRCTR','ProfitCenter') \
                            .withColumnRenamed('USRTYP','RecipientType') \
                            .withColumnRenamed('USRKEY','RecipientObjectId') \
                            .withColumnRenamed('LandingFileTimeStamp','LandingFileTimeStamp') \
                            .withColumnRenamed('UpdatedOn','UpdatedOn') \
                            .withColumn('DataSource',lit('SAP'))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_rename.write.format(write_format).mode("overwrite").save(write_path) 
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_rename.createOrReplaceTempView('stg_mrp_controllers')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fedw.mrp_controllers S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY Client,Plant,MRPController ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_mrp_controllers)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.Client = T.Client 
# MAGIC and 
# MAGIC S.Plant = T.Plant
# MAGIC and
# MAGIC S.MRPController = T.MRPController
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  S.Client = T.Client,
# MAGIC S.Plant = T.Plant,
# MAGIC S.MRPController = T.MRPController,
# MAGIC S.MRPControllerName = T.MRPControllerName,
# MAGIC S.MRPControllerTel = T.MRPControllerTel,
# MAGIC S.PurchasingGroup = T.PurchasingGroup,
# MAGIC S.RecipientName = T.RecipientName,
# MAGIC S.BusinessArea = T.BusinessArea,
# MAGIC S.ProfitCenter = T.ProfitCenter,
# MAGIC S.RecipientType = T.RecipientType,
# MAGIC S.RecipientObjectId = T.RecipientObjectId,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC Client,
# MAGIC Plant,
# MAGIC MRPController,
# MAGIC MRPControllerName,
# MAGIC MRPControllerTel,
# MAGIC PurchasingGroup,
# MAGIC RecipientName,
# MAGIC BusinessArea,
# MAGIC ProfitCenter,
# MAGIC RecipientType,
# MAGIC RecipientObjectId,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.Client ,
# MAGIC T.Plant ,
# MAGIC T.MRPController ,
# MAGIC T.MRPControllerName ,
# MAGIC T.MRPControllerTel ,
# MAGIC T.PurchasingGroup ,
# MAGIC T.RecipientName ,
# MAGIC T.BusinessArea ,
# MAGIC T.ProfitCenter ,
# MAGIC T.RecipientType ,
# MAGIC T.RecipientObjectId ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC T.UpdatedOn,
# MAGIC T.DataSource
# MAGIC )

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T024D'".format(ts))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.MRP_Controllers where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'MRP_Controllers')")

# COMMAND ----------

sfUrl = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUrl'").first()[0]
sfUser = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUser'").first()[0]
sfPassword = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfPassword'").first()[0]
sfDatabase = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfDatabase'").first()[0]
sfSchema = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfSchema'").first()[0]
sfWarehouse = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfWarehouse'").first()[0]
sfRole = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfRole'").first()[0]
insecureMode = spark.sql("select variable_value from Config.config_constant where variable_name = 'insecureMode'").first()[0]

options = {
  "sfUrl": sfUrl,
  "sfUser": sfUser,
  "sfPassword": sfPassword,
  "sfDatabase": sfDatabase,
  "sfSchema": sfSchema,
  "sfWarehouse": sfWarehouse,
  "sfRole": sfRole,
  "insecureMode": insecureMode
}

# COMMAND ----------

df_sf.write \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("dbtable", "MRP_Controllers") \
  .mode("OVERWRITE") \
  .save()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val sfUrl = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUrl'").first().getString(0)
# MAGIC val sfUser = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUser'").first().getString(0)
# MAGIC val sfPassword = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfPassword'").first().getString(0)
# MAGIC val sfDatabase = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfDatabase'").first().getString(0)
# MAGIC val sfSchema = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfSchema'").first().getString(0)
# MAGIC val sfWarehouse = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfWarehouse'").first().getString(0)
# MAGIC val sfRole = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfRole'").first().getString(0)
# MAGIC val insecureMode = spark.sql("select variable_value from Config.config_constant where variable_name = 'insecureMode'").first().getString(0)
# MAGIC 
# MAGIC 
# MAGIC val options = Map(
# MAGIC   "sfUrl" -> sfUrl,
# MAGIC   "sfUser" -> sfUser,
# MAGIC   "sfPassword" -> sfPassword,
# MAGIC   "sfDatabase" -> sfDatabase,
# MAGIC   "sfSchema" -> sfSchema,
# MAGIC   "sfWarehouse" -> sfWarehouse,
# MAGIC   "sfRole" -> sfRole,
# MAGIC    "insecureMode" -> insecureMode
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC  
# MAGIC Utils.runQuery(options, """call SP_LOAD_MRP_Controllers()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'MRP_Controllers'".format(ts))

# COMMAND ----------


