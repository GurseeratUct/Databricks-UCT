# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Storage_Location'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_t001l = [
"MANDT",
"WERKS",
"LGORT",
"LGOBE",
"SPART",
"XLONG",
"XBUFX",
"DISKZ",
"XBLGO",
"XRESS",
"XHUPF",
"PARLG",
"VKORG",
"VTWEG",
"VSTEL",
"LIFNR",
"KUNNR",
"MESBS",
"MESST",
"OIH_LICNO",
"OIG_ITRFL",
"OIB_TNKASSIGN"
]

# COMMAND ----------

df = spark.sql("select * from S42.T001L where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T001L' and DatabaseName = 'S42')")
df_t001l = df.select(col_names_t001l)
df_t001l.createOrReplaceTempView("tmp_t001l")
df_ts_t001l = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_t001l = df_ts_t001l["max(UpdatedOn)"]
print(ts_t001l)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_storage_location as 
# MAGIC select
# MAGIC S.MANDT as Client,
# MAGIC S.WERKS as Plant,
# MAGIC S.LGORT as Storagelocation,
# MAGIC S.LGOBE as DescriptionofStorageLocation,
# MAGIC S.SPART as Division,
# MAGIC S.XLONG as Negstocksallowedinstoragelocation,
# MAGIC S.XBUFX as Freezingbookinventorybal,
# MAGIC S.DISKZ as StoragelocationMRPindicator,
# MAGIC S.XBLGO as Storagelocationauth,
# MAGIC S.XRESS as StorageLocationisAssign,
# MAGIC S.XHUPF as Handlingunitrequirement,
# MAGIC S.PARLG as Partnerstoragelocation,
# MAGIC S.VKORG as SalesOrganization,
# MAGIC S.VTWEG as DistributionChannel,
# MAGIC S.VSTEL as ShippingPointOrReceivingPoint,
# MAGIC S.LIFNR as Vendorsaccountnumber,
# MAGIC S.KUNNR as Accountnumberofcustomer,
# MAGIC S.MESBS as BusinessSystemofMES,
# MAGIC S.MESST as Typeofinventorymanagement,
# MAGIC S.OIH_LICNO as Licensenumberforuntaxedstock,
# MAGIC S.OIG_ITRFL as TDintransitflag,
# MAGIC S.OIB_TNKASSIGN as SiloManagament,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_t001l as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_storage_location")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_merge.write.format(write_format).mode("overwrite").save(write_path)
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_storage_location')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Storage_Location as T 
# MAGIC USING stg_merge_storage_location as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.Plant = S.Plant
# MAGIC and T.Storagelocation = S.Storagelocation
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Client =  S.Client,
# MAGIC T.Plant =  S.Plant,
# MAGIC T.Storagelocation =  S.Storagelocation,
# MAGIC T.DescriptionofStorageLocation =  S.DescriptionofStorageLocation,
# MAGIC T.Division =  S.Division,
# MAGIC T.Negstocksallowedinstoragelocation =  S.Negstocksallowedinstoragelocation,
# MAGIC T.Freezingbookinventorybal =  S.Freezingbookinventorybal,
# MAGIC T.StoragelocationMRPindicator =  S.StoragelocationMRPindicator,
# MAGIC T.Storagelocationauth =  S.Storagelocationauth,
# MAGIC T.StorageLocationisAssign =  S.StorageLocationisAssign,
# MAGIC T.Handlingunitrequirement =  S.Handlingunitrequirement,
# MAGIC T.Partnerstoragelocation =  S.Partnerstoragelocation,
# MAGIC T.SalesOrganization =  S.SalesOrganization,
# MAGIC T.DistributionChannel =  S.DistributionChannel,
# MAGIC T.ShippingPointOrReceivingPoint =  S.ShippingPointOrReceivingPoint,
# MAGIC T.Vendorsaccountnumber =  S.Vendorsaccountnumber,
# MAGIC T.Accountnumberofcustomer =  S.Accountnumberofcustomer,
# MAGIC T.BusinessSystemofMES =  S.BusinessSystemofMES,
# MAGIC T.Typeofinventorymanagement =  S.Typeofinventorymanagement,
# MAGIC T.Licensenumberforuntaxedstock =  S.Licensenumberforuntaxedstock,
# MAGIC T.TDintransitflag =  S.TDintransitflag,
# MAGIC T.SiloManagament =  S.SiloManagament,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Client,
# MAGIC Plant,
# MAGIC Storagelocation,
# MAGIC DescriptionofStorageLocation,
# MAGIC Division,
# MAGIC Negstocksallowedinstoragelocation,
# MAGIC Freezingbookinventorybal,
# MAGIC StoragelocationMRPindicator,
# MAGIC Storagelocationauth,
# MAGIC StorageLocationisAssign,
# MAGIC Handlingunitrequirement,
# MAGIC Partnerstoragelocation,
# MAGIC SalesOrganization,
# MAGIC DistributionChannel,
# MAGIC ShippingPointOrReceivingPoint,
# MAGIC Vendorsaccountnumber,
# MAGIC Accountnumberofcustomer,
# MAGIC BusinessSystemofMES,
# MAGIC Typeofinventorymanagement,
# MAGIC Licensenumberforuntaxedstock,
# MAGIC TDintransitflag,
# MAGIC SiloManagament,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Client,
# MAGIC S.Plant,
# MAGIC S.Storagelocation,
# MAGIC S.DescriptionofStorageLocation,
# MAGIC S.Division,
# MAGIC S.Negstocksallowedinstoragelocation,
# MAGIC S.Freezingbookinventorybal,
# MAGIC S.StoragelocationMRPindicator,
# MAGIC S.Storagelocationauth,
# MAGIC S.StorageLocationisAssign,
# MAGIC S.Handlingunitrequirement,
# MAGIC S.Partnerstoragelocation,
# MAGIC S.SalesOrganization,
# MAGIC S.DistributionChannel,
# MAGIC S.ShippingPointOrReceivingPoint,
# MAGIC S.Vendorsaccountnumber,
# MAGIC S.Accountnumberofcustomer,
# MAGIC S.BusinessSystemofMES,
# MAGIC S.Typeofinventorymanagement,
# MAGIC S.Licensenumberforuntaxedstock,
# MAGIC S.TDintransitflag,
# MAGIC S.SiloManagament,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

if(ts_t001l != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T001L' and DatabaseName = 'S42'".format(ts_t001l))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Storage_Location where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Storage_Location' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "Storage_Location") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_STORAGE_LOCATION()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Storage_Location' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


