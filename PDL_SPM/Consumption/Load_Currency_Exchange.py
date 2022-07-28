# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Currency_Exchange'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_currency_exchange = [ 
    "ROWID",
    "CURRENCY_ID",
    "EFFECTIVE_DATE",
    "SELL_RATE",
    "BUY_RATE"
]

# COMMAND ----------

df = spark.sql("select * from VE70.CURRENCY_EXCHANGE where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'CURRENCY_EXCHANGE' and DatabaseName = 'VE70')")
df_currency_exchange_ve70 = df.select(col_names_currency_exchange)
df_currency_exchange_ve70.createOrReplaceTempView("currency_exchange_ve70")
df_ts_currency_exchange_ve70 = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_currency_exchange_ve70 = df_ts_currency_exchange_ve70["max(UpdatedOn)"]
print(ts_currency_exchange_ve70)

# COMMAND ----------

df = spark.sql("select * from VE72.CURRENCY_EXCHANGE where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'CURRENCY_EXCHANGE' and DatabaseName = 'VE72')")
df_currency_exchange_ve72 = df.select(col_names_currency_exchange)
df_currency_exchange_ve72.createOrReplaceTempView("currency_exchange_ve72")
df_ts_currency_exchange_ve72 = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_currency_exchange_ve72 = df_ts_currency_exchange_ve72["max(UpdatedOn)"]
print(ts_currency_exchange_ve72)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view currency_exchange_tmp as 
# MAGIC select *,'VE70' as DataSource from currency_exchange_ve70 
# MAGIC union
# MAGIC select *,'VE72' as DataSource from currency_exchange_ve72

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_currency_exchange as 
# MAGIC select
# MAGIC V.ROWID as ROWID,
# MAGIC V.CURRENCY_ID as CurrencyID,
# MAGIC V.EFFECTIVE_DATE as EffectiveDate,
# MAGIC V.SELL_RATE as SellRate,
# MAGIC V.BUY_RATE as BuyRate,
# MAGIC now() as UpdatedOn,
# MAGIC V.DataSource as DataSource
# MAGIC from currency_exchange_tmp as V

# COMMAND ----------

df_merge = spark.sql("select * from merge_currency_exchange")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_currency_exchange')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.CURRENCY_EXCHANGE as T 
# MAGIC USING stg_merge_currency_exchange as S 
# MAGIC ON T.CurrencyID = S.CurrencyID
# MAGIC and T.EffectiveDate = S.EffectiveDate
# MAGIC and T.DataSource = S.DataSource
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.ROWID = S.ROWID,
# MAGIC T.CurrencyID = S.CurrencyID,
# MAGIC T.EffectiveDate = S.EffectiveDate,
# MAGIC T.SellRate = S.SellRate,
# MAGIC T.BuyRate = S.BuyRate,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC ROWID,
# MAGIC CurrencyID,
# MAGIC EffectiveDate,
# MAGIC SellRate,
# MAGIC BuyRate,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.ROWID,
# MAGIC S.CurrencyID,
# MAGIC S.EffectiveDate,
# MAGIC S.SellRate,
# MAGIC S.BuyRate,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_currency_exchange_ve70 != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'CURRENCY_EXCHANGE' and DatabaseName = 'VE70'".format(ts_currency_exchange_ve70))

if(ts_currency_exchange_ve72 != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'CURRENCY_EXCHANGE' and DatabaseName = 'VE72'".format(ts_currency_exchange_ve72))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.CURRENCY_EXCHANGE where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'CURRENCY_EXCHANGE' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "CURRENCY_EXCHANGE") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_CURRENCY_EXCHANGE()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Supplier' and DatabaseName = 'FEDW'".format(ts_sf))
