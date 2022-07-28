# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Purchase_Document_Type'
table_name = 'T161T' 
read_format = 'delta'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
stage_table = 'stg_'+table_name
database_name = 'FEDW'

# COMMAND ----------

col_names = [ 'MANDT',
                'SPRAS',
                'BSART',
                'BSTYP',
                'BATXT',
                'LandingFileTimeStamp',
                'UpdatedOn',
                'DataSource',
                ]

# COMMAND ----------

df = spark.sql("select * from S42.T161T where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T161T')")

# COMMAND ----------


df_ts = df.agg({"UpdatedOn": "max"}).collect()[0]
ts = df_ts["max(UpdatedOn)"]
print(ts)

# COMMAND ----------

df_select_col = df.select(col_names)

# COMMAND ----------

df_rename =    df_select_col.withColumnRenamed('MANDT','Client') \
                            .withColumnRenamed('SPRAS','LanguageKey') \
                            .withColumnRenamed('BSART','PurchasingDocumentType') \
                            .withColumnRenamed('BSTYP','PurchasingDocumentCategory') \
                            .withColumnRenamed('BATXT','PurchasingDocTypeDescShort') \
                            .withColumnRenamed('LandingFileTimeStamp','LandingFileTimeStamp') \
                            .withColumnRenamed('UpdatedOn','UpdatedOn') \
                            .withColumn('DataSource',lit('SAP'))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_rename.write.format(write_format).mode("overwrite").save(write_path) 
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_rename.createOrReplaceTempView('stg_Purchase_Document_Type')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fedw.Purchase_Document_Type  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY Client,LanguageKey,PurchasingDocumentType,PurchasingDocumentCategory ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_Purchase_Document_Type)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.Client = T.Client 
# MAGIC and 
# MAGIC S.LanguageKey = T.LanguageKey
# MAGIC and
# MAGIC S.PurchasingDocumentType = T.PurchasingDocumentType
# MAGIC and
# MAGIC S.PurchasingDocumentCategory = T.PurchasingDocumentCategory
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC S.Client = T.Client,
# MAGIC S.LanguageKey = T.LanguageKey,
# MAGIC S.PurchasingDocumentType = T.PurchasingDocumentType,
# MAGIC S.PurchasingDocumentCategory = T.PurchasingDocumentCategory,
# MAGIC S.PurchasingDocTypeDescShort = T.PurchasingDocTypeDescShort,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC  (
# MAGIC   Client,
# MAGIC   LanguageKey,
# MAGIC   PurchasingDocumentType,
# MAGIC   PurchasingDocumentCategory,
# MAGIC   PurchasingDocTypeDescShort,
# MAGIC   LandingFileTimeStamp,
# MAGIC   UpdatedOn,
# MAGIC   DataSource
# MAGIC  )
# MAGIC VALUES
# MAGIC (
# MAGIC T.Client ,
# MAGIC T.LanguageKey ,
# MAGIC T.PurchasingDocumentType ,
# MAGIC T.PurchasingDocumentCategory ,
# MAGIC T.PurchasingDocTypeDescShort ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC T.DataSource
# MAGIC )

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T161T'".format(ts))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Purchase_Document_Type where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchase_Document_Type')")

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
  .option("dbtable", "Purchase_Document_Type") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_Purchase_Document_Type()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchase_Document_Type'".format(ts))

# COMMAND ----------


