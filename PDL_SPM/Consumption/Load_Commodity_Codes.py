# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'CommodityCode'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' 
database_name = 'FEDW'

# COMMAND ----------

df_cc = spark.sql("select * from flat_file.commoditycode where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'CommodityCode' and DatabaseName = 'FLAT_FILE')")
df_cc.createOrReplaceTempView("tmp_cc")
df_ts_cc = df_cc.agg({"UpdatedOn": "max"}).collect()[0]
ts_cc = df_ts_cc["max(UpdatedOn)"]
print(ts_cc)

# COMMAND ----------

df_merge = spark.sql("select CommodityCodeTier1,CommodityCodeTier2,SubCommodityCode,CommodityCodeSAP,now() as UpdatedOn,DataSource from tmp_cc")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

if(ts_cc != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'CommodityCode' and DatabaseName = 'FLAT_FILE'".format(ts_cc))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.CommodityCode where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'CommodityCode' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_commodity_code = ts_sf["max(UpdatedOn)"]
print(ts_commodity_code)

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
  .option("dbtable", "commoditycode") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_COMMODITYCODE()""")

# COMMAND ----------

if(ts_commodity_code != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'CommodityCode' and DatabaseName = 'FEDW'".format(ts_commodity_code))

# COMMAND ----------


