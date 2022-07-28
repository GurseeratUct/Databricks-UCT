# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Material_Desc'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_makt = [
"MATNR",
"SPRAS",
"MAKTX",
"MAKTG"
]

# COMMAND ----------

df = spark.sql("select * from S42.MAKT where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'MAKT' and TargetLayerTableName = 'Material_Desc' and DatabaseName = 'S42')")
df_makt = df.select(col_names_makt)
df_makt.createOrReplaceTempView("tmp_makt")
df_ts_makt = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_makt = df_ts_makt["max(UpdatedOn)"]
print(ts_makt)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_material_desc as 
# MAGIC select
# MAGIC MATNR as MaterialNumber,
# MAGIC SPRAS as LanguageKey,
# MAGIC MAKTX as Description,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_makt 

# COMMAND ----------

df_merge = spark.sql("select * from merge_material_desc")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_material_desc')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Material_Desc as T 
# MAGIC USING stg_merge_material_desc as S 
# MAGIC ON T.MaterialNumber = S.MaterialNumber
# MAGIC and T.LanguageKey = S.LanguageKey
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.MaterialNumber = S.MaterialNumber,
# MAGIC T.LanguageKey = S.LanguageKey,
# MAGIC T.Description = S.Description,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC MaterialNumber,
# MAGIC LanguageKey,
# MAGIC Description,
# MAGIC UpdatedOn,
# MAGIC DataSource)
# MAGIC VALUES
# MAGIC (
# MAGIC MaterialNumber,
# MAGIC LanguageKey,
# MAGIC Description,
# MAGIC now(),
# MAGIC DataSource
# MAGIC )

# COMMAND ----------

if(ts_makt != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'MAKT' and DatabaseName = 'S42'".format(ts_makt))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Material_Desc where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Desc' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_sf = ts_sf["max(UpdatedOn)"]
print(ts_sf)

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
  .option("dbtable", "Material_Desc") \
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
# MAGIC 
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC  
# MAGIC Utils.runQuery(options, """call SP_LOAD_MATERIAL_DESC()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Desc' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


