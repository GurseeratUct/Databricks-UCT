# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Currency_Codes'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_tcurc = [
 "MANDT",
"WAERS",
"ISOCD",
"ALTWR",
"GDATU",
"XPRIMARY"
]
    

# COMMAND ----------

df = spark.sql("select * from S42.TCURC where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'TCURC' and DatabaseName = 'S42')")
df_tcurc = df.select(col_names_tcurc)
df_tcurc.createOrReplaceTempView("tmp_tcurc")
df_ts_tcurc = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_tcurc = df_ts_tcurc["max(UpdatedOn)"]
print(ts_tcurc)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_currency_codes as 
# MAGIC select 
# MAGIC S.MANDT as Client,
# MAGIC S.WAERS as CurrencyKey,
# MAGIC S.ISOCD as ISOcurrencycode,
# MAGIC S.ALTWR as Alternativekeyforcurrencies,
# MAGIC S.GDATU as Dateuntilwhichthecurrencyisvalid,
# MAGIC S.XPRIMARY as PrimarySAPCurrencyCodeforISOCode,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_tcurc as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_currency_codes")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_currency_codes')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.currency_codes as T 
# MAGIC USING stg_merge_currency_codes as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.CurrencyKey = S.CurrencyKey
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Client =  S.Client,
# MAGIC T.CurrencyKey =  S.CurrencyKey,
# MAGIC T.ISOcurrencycode =  S.ISOcurrencycode,
# MAGIC T.Alternativekeyforcurrencies =  S.Alternativekeyforcurrencies,
# MAGIC T.Dateuntilwhichthecurrencyisvalid =  S.Dateuntilwhichthecurrencyisvalid,
# MAGIC T.PrimarySAPCurrencyCodeforISOCode =  S.PrimarySAPCurrencyCodeforISOCode,
# MAGIC T.UpdatedOn =  S.UpdatedOn,
# MAGIC T.DataSource =  S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Client,
# MAGIC CurrencyKey,
# MAGIC ISOcurrencycode,
# MAGIC Alternativekeyforcurrencies,
# MAGIC Dateuntilwhichthecurrencyisvalid,
# MAGIC PrimarySAPCurrencyCodeforISOCode,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Client,
# MAGIC S.CurrencyKey,
# MAGIC S.ISOcurrencycode,
# MAGIC S.Alternativekeyforcurrencies,
# MAGIC S.Dateuntilwhichthecurrencyisvalid,
# MAGIC S.PrimarySAPCurrencyCodeforISOCode,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_tcurc != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'TCURC' and DatabaseName = 'S42'".format(ts_tcurc))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.currency_codes where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Currency_Codes' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts = ts_sf["max(UpdatedOn)"]
print(ts)

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
  .option("dbtable", "Currency_Codes") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_CURRENCY_CODES()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Currency_Codes' and DatabaseName = 'FEDW'".format(ts))

# COMMAND ----------


