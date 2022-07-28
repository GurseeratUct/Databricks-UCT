# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Material_Type'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_t134 = [
"MANDT",
"MTART",
"MTREF",
"MBREF",
"FLREF",
"NUMKI",
"NUMKE",
"ENVOP",
"BSEXT",
"BSINT",
"PSTAT",
"KKREF",
"VPRSV",
"KZVPR",
"VMTPO",
"EKALR",
"KZGRP",
"KZKFG",
"BEGRU",
"KZPRC",
"KZPIP",
"PRDRU",
"ARANZ",
"WMAKG",
"IZUST",
"ARDEL",
"KZMPN",
"MSTAE",
"CCHIS",
"CTYPE",
"CLASS",
"CHNEU",
"PROD_TYPE_CODE",
"CONCTD_MATNR",
"VTYPE",
"VNUMKI",
"VNUMKE",
"KZFFF",
"KZRAC"
]

# COMMAND ----------

df = spark.sql("select * from S42.T134 where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T134' and DatabaseName = 'S42')")
df_t134 = df.select(col_names_t134)
df_t134.createOrReplaceTempView("tmp_t134")
df_ts_t134 = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_t134 = df_ts_t134["max(UpdatedOn)"]
print(ts_t134)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_material_type as 
# MAGIC select
# MAGIC S.MANDT as Client,
# MAGIC S.MTART as Materialtype,
# MAGIC S.MTREF as Referencematerialtype,
# MAGIC S.MBREF as Screenref,
# MAGIC S.FLREF as Fieldrefmaterialmaster,
# MAGIC S.NUMKI as Numberrange1,
# MAGIC S.NUMKE as Numberrange2,
# MAGIC S.ENVOP as ExternalNumAssignWithoutValidation,
# MAGIC S.BSEXT as ExternalPurchaseOrdersAllowed,
# MAGIC S.BSINT as Internalpurchaseordersallowed,
# MAGIC S.PSTAT as Maintenancestatus,
# MAGIC S.KKREF as Accountcategoryreference,
# MAGIC S.VPRSV as Pricecontrolindicator,
# MAGIC S.KZVPR as PriceControlMandatory,
# MAGIC S.VMTPO as Defvalitemcategorygroup,
# MAGIC S.EKALR as MaterialIsCostedwithQuantityStructure,
# MAGIC S.KZGRP as Groupingindicator,
# MAGIC S.KZKFG as ConfigurableMaterial,
# MAGIC S.BEGRU as Authorizationgroupinthematerialmaster,
# MAGIC S.KZPRC as MaterialMasterRecordforaProcess,
# MAGIC S.KZPIP as PipelineHandlingMandatory,
# MAGIC S.PRDRU as Displaypriceoncash,
# MAGIC S.ARANZ as Displaymaterialoncashregisterdisplay,
# MAGIC S.WMAKG as MaterialTypeID,
# MAGIC S.IZUST as Initialstatusofanewbatch,
# MAGIC S.ARDEL as Timeindaysuntilamaterialisdeleted,
# MAGIC S.KZMPN as ManufacturerPart,
# MAGIC S.MSTAE as CrossPlantMaterialStatus,
# MAGIC S.CCHIS as Controlofhistory,
# MAGIC S.CTYPE as ClassType,
# MAGIC S.CLASS as Classnumber,
# MAGIC S.CHNEU as BatchCreationControl,
# MAGIC S.PROD_TYPE_CODE as ProductTypeGroup,
# MAGIC S.CONCTD_MATNR as ConcatenatedMaterialNo,
# MAGIC S.VTYPE as VersionCategory,
# MAGIC S.VNUMKI as Numberrange3,
# MAGIC S.VNUMKE as Numberrange4,
# MAGIC S.KZFFF as IndicatorFFFclass,
# MAGIC S.KZRAC as ReturnablePackagingLogisticsismandatory,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_t134 as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_material_type")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_material_type')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Material_Type as T 
# MAGIC USING stg_merge_material_type as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.Materialtype = S.Materialtype
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Client =  S.Client,
# MAGIC T.Materialtype =  S.Materialtype,
# MAGIC T.Referencematerialtype =  S.Referencematerialtype,
# MAGIC T.Screenref =  S.Screenref,
# MAGIC T.Fieldrefmaterialmaster =  S.Fieldrefmaterialmaster,
# MAGIC T.Numberrange1 =  S.Numberrange1,
# MAGIC T.Numberrange2 =  S.Numberrange2,
# MAGIC T.ExternalNumAssignWithoutValidation =  S.ExternalNumAssignWithoutValidation,
# MAGIC T.ExternalPurchaseOrdersAllowed =  S.ExternalPurchaseOrdersAllowed,
# MAGIC T.Internalpurchaseordersallowed =  S.Internalpurchaseordersallowed,
# MAGIC T.Maintenancestatus =  S.Maintenancestatus,
# MAGIC T.Accountcategoryreference =  S.Accountcategoryreference,
# MAGIC T.Pricecontrolindicator =  S.Pricecontrolindicator,
# MAGIC T.PriceControlMandatory =  S.PriceControlMandatory,
# MAGIC T.Defvalitemcategorygroup =  S.Defvalitemcategorygroup,
# MAGIC T.MaterialIsCostedwithQuantityStructure =  S.MaterialIsCostedwithQuantityStructure,
# MAGIC T.Groupingindicator =  S.Groupingindicator,
# MAGIC T.ConfigurableMaterial =  S.ConfigurableMaterial,
# MAGIC T.Authorizationgroupinthematerialmaster =  S.Authorizationgroupinthematerialmaster,
# MAGIC T.MaterialMasterRecordforaProcess =  S.MaterialMasterRecordforaProcess,
# MAGIC T.PipelineHandlingMandatory =  S.PipelineHandlingMandatory,
# MAGIC T.Displaypriceoncash =  S.Displaypriceoncash,
# MAGIC T.Displaymaterialoncashregisterdisplay =  S.Displaymaterialoncashregisterdisplay,
# MAGIC T.MaterialTypeID =  S.MaterialTypeID,
# MAGIC T.Initialstatusofanewbatch =  S.Initialstatusofanewbatch,
# MAGIC T.Timeindaysuntilamaterialisdeleted =  S.Timeindaysuntilamaterialisdeleted,
# MAGIC T.ManufacturerPart =  S.ManufacturerPart,
# MAGIC T.CrossPlantMaterialStatus =  S.CrossPlantMaterialStatus,
# MAGIC T.Controlofhistory =  S.Controlofhistory,
# MAGIC T.ClassType =  S.ClassType,
# MAGIC T.Classnumber =  S.Classnumber,
# MAGIC T.BatchCreationControl =  S.BatchCreationControl,
# MAGIC T.ProductTypeGroup =  S.ProductTypeGroup,
# MAGIC T.ConcatenatedMaterialNo =  S.ConcatenatedMaterialNo,
# MAGIC T.VersionCategory =  S.VersionCategory,
# MAGIC T.Numberrange3 =  S.Numberrange3,
# MAGIC T.Numberrange4 =  S.Numberrange4,
# MAGIC T.IndicatorFFFclass =  S.IndicatorFFFclass,
# MAGIC T.ReturnablePackagingLogisticsismandatory =  S.ReturnablePackagingLogisticsismandatory,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Client,
# MAGIC Materialtype,
# MAGIC Referencematerialtype,
# MAGIC Screenref,
# MAGIC Fieldrefmaterialmaster,
# MAGIC Numberrange1,
# MAGIC Numberrange2,
# MAGIC ExternalNumAssignWithoutValidation,
# MAGIC ExternalPurchaseOrdersAllowed,
# MAGIC Internalpurchaseordersallowed,
# MAGIC Maintenancestatus,
# MAGIC Accountcategoryreference,
# MAGIC Pricecontrolindicator,
# MAGIC PriceControlMandatory,
# MAGIC Defvalitemcategorygroup,
# MAGIC MaterialIsCostedwithQuantityStructure,
# MAGIC Groupingindicator,
# MAGIC ConfigurableMaterial,
# MAGIC Authorizationgroupinthematerialmaster,
# MAGIC MaterialMasterRecordforaProcess,
# MAGIC PipelineHandlingMandatory,
# MAGIC Displaypriceoncash,
# MAGIC Displaymaterialoncashregisterdisplay,
# MAGIC MaterialTypeID,
# MAGIC Initialstatusofanewbatch,
# MAGIC Timeindaysuntilamaterialisdeleted,
# MAGIC ManufacturerPart,
# MAGIC CrossPlantMaterialStatus,
# MAGIC Controlofhistory,
# MAGIC ClassType,
# MAGIC Classnumber,
# MAGIC BatchCreationControl,
# MAGIC ProductTypeGroup,
# MAGIC ConcatenatedMaterialNo,
# MAGIC VersionCategory,
# MAGIC Numberrange3,
# MAGIC Numberrange4,
# MAGIC IndicatorFFFclass,
# MAGIC ReturnablePackagingLogisticsismandatory,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Client,
# MAGIC S.Materialtype,
# MAGIC S.Referencematerialtype,
# MAGIC S.Screenref,
# MAGIC S.Fieldrefmaterialmaster,
# MAGIC S.Numberrange1,
# MAGIC S.Numberrange2,
# MAGIC S.ExternalNumAssignWithoutValidation,
# MAGIC S.ExternalPurchaseOrdersAllowed,
# MAGIC S.Internalpurchaseordersallowed,
# MAGIC S.Maintenancestatus,
# MAGIC S.Accountcategoryreference,
# MAGIC S.Pricecontrolindicator,
# MAGIC S.PriceControlMandatory,
# MAGIC S.Defvalitemcategorygroup,
# MAGIC S.MaterialIsCostedwithQuantityStructure,
# MAGIC S.Groupingindicator,
# MAGIC S.ConfigurableMaterial,
# MAGIC S.Authorizationgroupinthematerialmaster,
# MAGIC S.MaterialMasterRecordforaProcess,
# MAGIC S.PipelineHandlingMandatory,
# MAGIC S.Displaypriceoncash,
# MAGIC S.Displaymaterialoncashregisterdisplay,
# MAGIC S.MaterialTypeID,
# MAGIC S.Initialstatusofanewbatch,
# MAGIC S.Timeindaysuntilamaterialisdeleted,
# MAGIC S.ManufacturerPart,
# MAGIC S.CrossPlantMaterialStatus,
# MAGIC S.Controlofhistory,
# MAGIC S.ClassType,
# MAGIC S.Classnumber,
# MAGIC S.BatchCreationControl,
# MAGIC S.ProductTypeGroup,
# MAGIC S.ConcatenatedMaterialNo,
# MAGIC S.VersionCategory,
# MAGIC S.Numberrange3,
# MAGIC S.Numberrange4,
# MAGIC S.IndicatorFFFclass,
# MAGIC S.ReturnablePackagingLogisticsismandatory,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

if(ts_t134 != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T134' and DatabaseName = 'S42'".format(ts_t134))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Material_Type where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Type' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "Material_Type") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_MATERIAL_TYPE()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Type' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


