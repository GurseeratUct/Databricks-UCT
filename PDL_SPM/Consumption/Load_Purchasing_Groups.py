# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Purchasing_Groups'
table_name = 'T024' 
read_format = 'delta'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
stage_table = 'stg_'+table_name
database_name = 'FEDW'

# COMMAND ----------

col_names = [
'MANDT',
'EKGRP',
'EKNAM',
'EKTEL',
'LDEST',
'TELFX',
'TEL_NUMBER',
'TEL_EXTENS',
'SMTP_ADDR',
'LandingFileTimeStamp',
'UpdatedOn',
'DataSource'
]

# COMMAND ----------

df = spark.sql("select * from S42.T024 where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T024')")

# COMMAND ----------

df_ts = df.agg({"UpdatedOn": "max"}).collect()[0]
ts = df_ts["max(UpdatedOn)"]
print(ts)

# COMMAND ----------

df_select_col = df.select(col_names)

# COMMAND ----------

df_rename =    df_select_col.withColumnRenamed('MANDT','Client') \
                            .withColumnRenamed('EKGRP','PurchasingGroup') \
                            .withColumnRenamed('EKNAM','PurchasingGroupDesc') \
                            .withColumnRenamed('EKTEL','PurchasingGroupTel') \
                            .withColumnRenamed('LDEST','OutputDevice') \
                            .withColumnRenamed('TELFX','PurchasingGroupFax') \
                            .withColumnRenamed('TEL_NUMBER','TelephoneNo') \
                            .withColumnRenamed('TEL_EXTENS','TelephoneExtension') \
                            .withColumnRenamed('SMTP_ADDR','EMailAddress') \
                            .withColumnRenamed('LandingFileTimeStamp','LandingFileTimeStamp') \
                            .withColumnRenamed('UpdatedOn','UpdatedOn') \
                            .withColumn('DataSource',lit('SAP')) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
    df_rename.write.format(write_format).mode("overwrite").save(write_path) 
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_rename.createOrReplaceTempView('stg_purchasing_groups')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fedw.Purchasing_Groups S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY Client,PurchasingGroup ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_purchasing_groups)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.Client = T.Client 
# MAGIC and 
# MAGIC S.PurchasingGroup = T.PurchasingGroup
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET
# MAGIC S.Client = T.Client,
# MAGIC S.PurchasingGroup = T.PurchasingGroup,
# MAGIC S.PurchasingGroupDesc = T.PurchasingGroupDesc,
# MAGIC S.PurchasingGroupTel = T.PurchasingGroupTel,
# MAGIC S.OutputDevice = T.OutputDevice,
# MAGIC S.PurchasingGroupFax = T.PurchasingGroupFax,
# MAGIC S.TelephoneNo = T.TelephoneNo,
# MAGIC S.TelephoneExtension = T.TelephoneExtension,
# MAGIC S.EMailAddress = T.EMailAddress,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT
# MAGIC (
# MAGIC Client,
# MAGIC PurchasingGroup,
# MAGIC PurchasingGroupDesc,
# MAGIC PurchasingGroupTel,
# MAGIC OutputDevice,
# MAGIC PurchasingGroupFax,
# MAGIC TelephoneNo,
# MAGIC TelephoneExtension,
# MAGIC EMailAddress,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.Client ,
# MAGIC T.PurchasingGroup ,
# MAGIC T.PurchasingGroupDesc ,
# MAGIC T.PurchasingGroupTel ,
# MAGIC T.OutputDevice ,
# MAGIC T.PurchasingGroupFax ,
# MAGIC T.TelephoneNo ,
# MAGIC T.TelephoneExtension ,
# MAGIC T.EMailAddress ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC T.DataSource
# MAGIC )

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T024'".format(ts))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Purchasing_Groups where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchasing_Groups')")


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
  .option("dbtable", "Purchasing_Groups") \
  .mode("OVERWRITE") \
  .save()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql._
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_Purchasing_Groups()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchasing_Groups'".format(ts))

# COMMAND ----------


