# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Conversion_Factor'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_tcurf = [
    "MANDT",
"KURST",
"FCURR",
"TCURR",
"GDATU",
"FFACT",
"TFACT",
"ABWCT",
"ABWGA"
]
    

# COMMAND ----------

df = spark.sql("select * from S42.TCURF where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'TCURF' and DatabaseName = 'S42')")
df_tcurf = df.select(col_names_tcurf)
df_tcurf.createOrReplaceTempView("tmp_tcurf")
df_ts_tcurf = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_tcurf = df_ts_tcurf["max(UpdatedOn)"]
print(ts_tcurf)

# COMMAND ----------

if(ts_tcurf == None):
    dbutils.notebook.exit("No Delta Load")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_conversion_factor as 
# MAGIC select 
# MAGIC S.MANDT as Client,
# MAGIC S.KURST as ExchangeRateType,
# MAGIC S.FCURR as FromCurrency,
# MAGIC S.TCURR as Tocurrency,
# MAGIC S.GDATU as DateFromWhichTheEntryIsValid,
# MAGIC S.FFACT as Ratioforthefromcurrencyunits,
# MAGIC S.TFACT as Ratioforthetocurrencyunits,
# MAGIC S.ABWCT as AltExchangeRateType,
# MAGIC S.ABWGA as AltExchangeRateTypeIsValid,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_tcurf as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_conversion_factor")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_conversion_factor')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.conversion_factor as T 
# MAGIC USING stg_merge_conversion_factor as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.ExchangeRateType = S.ExchangeRateType
# MAGIC and T.FromCurrency = S.FromCurrency
# MAGIC and T.Tocurrency = S.Tocurrency
# MAGIC and T.DateFromWhichTheEntryIsValid = S.DateFromWhichTheEntryIsValid
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Client =  S.Client,
# MAGIC T.ExchangeRateType =  S.ExchangeRateType,
# MAGIC T.FromCurrency =  S.FromCurrency,
# MAGIC T.Tocurrency =  S.Tocurrency,
# MAGIC T.DateFromWhichTheEntryIsValid =  S.DateFromWhichTheEntryIsValid,
# MAGIC T.Ratioforthefromcurrencyunits =  S.Ratioforthefromcurrencyunits,
# MAGIC T.Ratioforthetocurrencyunits =  S.Ratioforthetocurrencyunits,
# MAGIC T.AltExchangeRateType =  S.AltExchangeRateType,
# MAGIC T.AltExchangeRateTypeIsValid =  S.AltExchangeRateTypeIsValid,
# MAGIC T.UpdatedOn =  S.UpdatedOn,
# MAGIC T.DataSource =  S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Client,
# MAGIC ExchangeRateType,
# MAGIC FromCurrency,
# MAGIC Tocurrency,
# MAGIC DateFromWhichTheEntryIsValid,
# MAGIC Ratioforthefromcurrencyunits,
# MAGIC Ratioforthetocurrencyunits,
# MAGIC AltExchangeRateType,
# MAGIC AltExchangeRateTypeIsValid,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Client,
# MAGIC S.ExchangeRateType,
# MAGIC S.FromCurrency,
# MAGIC S.Tocurrency,
# MAGIC S.DateFromWhichTheEntryIsValid,
# MAGIC S.Ratioforthefromcurrencyunits,
# MAGIC S.Ratioforthetocurrencyunits,
# MAGIC S.AltExchangeRateType,
# MAGIC S.AltExchangeRateTypeIsValid,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_tcurf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'TCURF' and DatabaseName = 'S42'".format(ts_tcurf))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.conversion_factor where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Conversion_Factor' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "Conversion_Factor") \
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
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC  
# MAGIC Utils.runQuery(options, """call SP_LOAD_CONVERSION_FACTOR()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Conversion_Factor' and DatabaseName = 'FEDW'".format(ts))

# COMMAND ----------


