# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Material_Revision_Hist'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_ztc2p_mat_map_h = [
"MANDT",
"MATNR",
"REVLV",
"AGILE_REV",
"CE_FLAG",
"CP_FLAG",
"SL_FLAG",
"ERDAT",
"INT_REV"
]

# COMMAND ----------

df = spark.sql("select * from S42.ZTC2P_MAT_MAP_H where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'ZTC2P_MAT_MAP_H' and DatabaseName = 'S42')")
df_ztc2p_mat_map_h = df.select(col_names_ztc2p_mat_map_h)
df_ztc2p_mat_map_h.createOrReplaceTempView("tmp_ztc2p_mat_map_h")
df_ts_ztc2p_mat_map_h = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ztc2p_mat_map_h = df_ts_ztc2p_mat_map_h["max(UpdatedOn)"]
print(ts_ztc2p_mat_map_h)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_ztc2p_mat_map_h as 
# MAGIC select 
# MAGIC S.MANDT as Client,
# MAGIC S.MATNR as MaterialNumber,
# MAGIC S.REVLV as RevisionLevel,
# MAGIC S.AGILE_REV as AgileRevision,
# MAGIC S.CE_FLAG as CopyExactFlag,
# MAGIC S.CP_FLAG as CriticalpartFlag,
# MAGIC S.SL_FLAG as SupplierLockedFlag,
# MAGIC S.ERDAT as Daterecordcreated,
# MAGIC S.INT_REV as InternalRevisionInd,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_ztc2p_mat_map_h as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_ztc2p_mat_map_h")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")    

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_Material_Revision_Hist')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Material_Revision_Hist as T 
# MAGIC USING stg_merge_Material_Revision_Hist as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.MaterialNumber = S.MaterialNumber
# MAGIC and T.RevisionLevel = S.RevisionLevel
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Client =  S.Client,
# MAGIC T.MaterialNumber =  S.MaterialNumber,
# MAGIC T.RevisionLevel =  S.RevisionLevel,
# MAGIC T.AgileRevision =  S.AgileRevision,
# MAGIC T.CopyExactFlag =  S.CopyExactFlag,
# MAGIC T.CriticalpartFlag =  S.CriticalpartFlag,
# MAGIC T.SupplierLockedFlag =  S.SupplierLockedFlag,
# MAGIC T.Daterecordcreated =  S.Daterecordcreated,
# MAGIC T.InternalRevisionInd =  S.InternalRevisionInd,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Client,
# MAGIC MaterialNumber,
# MAGIC RevisionLevel,
# MAGIC AgileRevision,
# MAGIC CopyExactFlag,
# MAGIC CriticalpartFlag,
# MAGIC SupplierLockedFlag,
# MAGIC Daterecordcreated,
# MAGIC InternalRevisionInd,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Client,
# MAGIC S.MaterialNumber,
# MAGIC S.RevisionLevel,
# MAGIC S.AgileRevision,
# MAGIC S.CopyExactFlag,
# MAGIC S.CriticalpartFlag,
# MAGIC S.SupplierLockedFlag,
# MAGIC S.Daterecordcreated,
# MAGIC S.InternalRevisionInd,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_ztc2p_mat_map_h != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'ZTC2P_MAT_MAP_H' and DatabaseName = 'S42'".format(ts_ztc2p_mat_map_h))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Material_Revision_Hist where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Revision_Hist' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "Material_Revision_Hist") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_MATERIAL_REVISION_HIST()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Revision_Hist' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


