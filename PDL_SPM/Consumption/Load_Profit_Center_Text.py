# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Profit_Center_Text'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_cepct = [
"MANDT",
"SPRAS",
"PRCTR",
"DATBI",
"KOKRS",
"KTEXT",
"LTEXT",
"MCTXT"
]

# COMMAND ----------

df = spark.sql("select * from S42.cepct where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'CEPCT' and DatabaseName = 'S42')")
df_cepct = df.select(col_names_cepct)
df_cepct.createOrReplaceTempView("tmp_cepct")
df_ts_cepct = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_cepct = df_ts_cepct["max(UpdatedOn)"]
print(ts_cepct)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_profit_center_text as 
# MAGIC select 
# MAGIC S.MANDT as Client,
# MAGIC S.SPRAS as LanguageKey,
# MAGIC S.PRCTR as ProfitCenter,
# MAGIC S.DATBI as ValidToDate,
# MAGIC S.KOKRS as ControllingArea,
# MAGIC S.KTEXT as GeneralName,
# MAGIC S.LTEXT as LongText,
# MAGIC S.MCTXT as Searchtermmatchcode,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_cepct as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_profit_center_text")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_profit_center_text')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Profit_Center_Text as T 
# MAGIC USING stg_merge_profit_center_text as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.ProfitCenter = S.ProfitCenter
# MAGIC and T.ValidToDate = S.ValidToDate
# MAGIC and T.ControllingArea = S.ControllingArea
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.`Client` =  S.`Client`,
# MAGIC T.`LanguageKey` =  S.`LanguageKey`,
# MAGIC T.`ProfitCenter` =  S.`ProfitCenter`,
# MAGIC T.`ValidToDate` =  S.`ValidToDate`,
# MAGIC T.`ControllingArea` =  S.`ControllingArea`,
# MAGIC T.`GeneralName` =  S.`GeneralName`,
# MAGIC T.`LongText` =  S.`LongText`,
# MAGIC T.`Searchtermmatchcode` =  S.`Searchtermmatchcode`,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC `Client`,
# MAGIC `LanguageKey`,
# MAGIC `ProfitCenter`,
# MAGIC `ValidToDate`,
# MAGIC `ControllingArea`,
# MAGIC `GeneralName`,
# MAGIC `LongText`,
# MAGIC `Searchtermmatchcode`,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`Client`,
# MAGIC S.`LanguageKey`,
# MAGIC S.`ProfitCenter`,
# MAGIC S.`ValidToDate`,
# MAGIC S.`ControllingArea`,
# MAGIC S.`GeneralName`,
# MAGIC S.`LongText`,
# MAGIC S.`Searchtermmatchcode`,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_cepct != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'CEPCT' and DatabaseName = 'S42'".format(ts_cepct))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.PROFIT_CENTER_TEXT where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Profit_Center_Text' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "Profit_Center_Text") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_PROFIT_CENTER_TEXT()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Profit_Center_Text' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


