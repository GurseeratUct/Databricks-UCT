# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Material_CPN_MPN'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_ztc2p_mat_map = [
"MANDT",
"MATNR",
"CPN",
"MANUFACTURER",
"MPN",
"CUSTOMER",
"AGILE_REV",
"REVLV",
"CE_FLAG",
"CP_FLAG",
"SL_FLAG",
"PREF_FLAG",
"ERDAT",
"UMC",
"PART_TYPE",
"INT_REV",
"SUB_FLAG",
"RELEASE_TYPE"
]

# COMMAND ----------

df = spark.sql("select * from S42.ZTC2P_MAT_MAP where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'ZTC2P_MAT_MAP' and DatabaseName = 'S42')")
df_ztc2p_mat_map = df.select(col_names_ztc2p_mat_map)
df_ztc2p_mat_map.createOrReplaceTempView("tmp_ztc2p_mat_map")
df_ts_ztc2p_mat_map = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ztc2p_mat_map = df_ts_ztc2p_mat_map["max(UpdatedOn)"]
print(ts_ztc2p_mat_map)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_ztc2p_mat_map as 
# MAGIC select 
# MAGIC S.MANDT as Client,
# MAGIC S.MATNR as MaterialNumber,
# MAGIC S.CPN as CustomerPartNumber,
# MAGIC S.MANUFACTURER as ManufacturerName,
# MAGIC S.MPN as ManufacturingPartNumber,
# MAGIC S.CUSTOMER as CustomerNamefromAgile,
# MAGIC S.AGILE_REV as AgileRevision,
# MAGIC S.REVLV as RevisionLevel,
# MAGIC S.CE_FLAG as CopyExactFlag,
# MAGIC S.CP_FLAG as CriticalpartFlag,
# MAGIC S.SL_FLAG as SupplierLockedFlag,
# MAGIC S.PREF_FLAG as PreferredFlagforMPN,
# MAGIC S.ERDAT as Dateonwhichtherecordwascreated,
# MAGIC S.UMC as MaterialflaggedasUMC,
# MAGIC S.PART_TYPE as PartType,
# MAGIC S.INT_REV as InternalRevisionIndicator,
# MAGIC S.SUB_FLAG as SubordinateFlag,
# MAGIC S.RELEASE_TYPE as AgileReleaseType,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_ztc2p_mat_map as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_ztc2p_mat_map")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_Material_CPN_MPN')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Material_CPN_MPN as T 
# MAGIC USING stg_merge_Material_CPN_MPN as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.MaterialNumber = S.MaterialNumber
# MAGIC and T.CustomerPartNumber = S.CustomerPartNumber
# MAGIC and T.ManufacturerName = S.ManufacturerName
# MAGIC and T.ManufacturingPartNumber = S.ManufacturingPartNumber
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Client =  S.Client,
# MAGIC T.MaterialNumber =  S.MaterialNumber,
# MAGIC T.CustomerPartNumber =  S.CustomerPartNumber,
# MAGIC T.ManufacturerName =  S.ManufacturerName,
# MAGIC T.ManufacturingPartNumber =  S.ManufacturingPartNumber,
# MAGIC T.CustomerNamefromAgile =  S.CustomerNamefromAgile,
# MAGIC T.AgileRevision =  S.AgileRevision,
# MAGIC T.RevisionLevel =  S.RevisionLevel,
# MAGIC T.CopyExactFlag =  S.CopyExactFlag,
# MAGIC T.CriticalpartFlag =  S.CriticalpartFlag,
# MAGIC T.SupplierLockedFlag =  S.SupplierLockedFlag,
# MAGIC T.PreferredFlagforMPN =  S.PreferredFlagforMPN,
# MAGIC T.Dateonwhichtherecordwascreated =  S.Dateonwhichtherecordwascreated,
# MAGIC T.MaterialflaggedasUMC =  S.MaterialflaggedasUMC,
# MAGIC T.PartType =  S.PartType,
# MAGIC T.InternalRevisionIndicator =  S.InternalRevisionIndicator,
# MAGIC T.SubordinateFlag =  S.SubordinateFlag,
# MAGIC T.AgileReleaseType =  S.AgileReleaseType,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Client,
# MAGIC MaterialNumber,
# MAGIC CustomerPartNumber,
# MAGIC ManufacturerName,
# MAGIC ManufacturingPartNumber,
# MAGIC CustomerNamefromAgile,
# MAGIC AgileRevision,
# MAGIC RevisionLevel,
# MAGIC CopyExactFlag,
# MAGIC CriticalpartFlag,
# MAGIC SupplierLockedFlag,
# MAGIC PreferredFlagforMPN,
# MAGIC Dateonwhichtherecordwascreated,
# MAGIC MaterialflaggedasUMC,
# MAGIC PartType,
# MAGIC InternalRevisionIndicator,
# MAGIC SubordinateFlag,
# MAGIC AgileReleaseType,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Client,
# MAGIC S.MaterialNumber,
# MAGIC S.CustomerPartNumber,
# MAGIC S.ManufacturerName,
# MAGIC S.ManufacturingPartNumber,
# MAGIC S.CustomerNamefromAgile,
# MAGIC S.AgileRevision,
# MAGIC S.RevisionLevel,
# MAGIC S.CopyExactFlag,
# MAGIC S.CriticalpartFlag,
# MAGIC S.SupplierLockedFlag,
# MAGIC S.PreferredFlagforMPN,
# MAGIC S.Dateonwhichtherecordwascreated,
# MAGIC S.MaterialflaggedasUMC,
# MAGIC S.PartType,
# MAGIC S.InternalRevisionIndicator,
# MAGIC S.SubordinateFlag,
# MAGIC S.AgileReleaseType,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_ztc2p_mat_map != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'ZTC2P_MAT_MAP' and DatabaseName = 'S42'".format(ts_ztc2p_mat_map))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Material_CPN_MPN where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_CPN_MPN' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "Material_CPN_MPN") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_MATERIAL_CPN_MPN()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_CPN_MPN' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


