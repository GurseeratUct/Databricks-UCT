# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Purchase_Source_List'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_eord = [
    "MATNR",
"WERKS",
"ZEORD",
"ERDAT",
"ERNAM",
"VDATU",
"BDATU",
"LIFNR",
"FLIFN",
"EBELN",
"EBELP",
"EKORG",
"VRTYP",
"EORTP",
"AUTET",
"MEINS",
"LASTCHANGEDATETIME"]

# COMMAND ----------

col_names_part = [
    "ID",
"PREF_VENDOR_ID"
]

# COMMAND ----------

plant_code_singapore = spark.sql("select variable_value from Config.config_constant where variable_name = 'plant_code_singapore'")
plant_code_singapore = plant_code_singapore.first()[0]
print(plant_code_singapore)

plant_code_shangai = spark.sql("select variable_value from Config.config_constant where variable_name = 'plant_code_shangai'")
plant_code_shangai = plant_code_shangai.first()[0]
print(plant_code_shangai)

company_code_shangai = spark.sql("select variable_value from Config.config_constant where variable_name = 'company_code_shangai'")
company_code_shangai = company_code_shangai.first()[0]
print(company_code_shangai)

company_code_singapore = spark.sql("select variable_value from Config.config_constant where variable_name = 'company_code_singapore'")
company_code_singapore = company_code_singapore.first()[0]
print(company_code_singapore)




# COMMAND ----------

df = spark.sql("select * from VE70.PART where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PART' and TargetLayerTableName ='Purchase_Source_List' and DatabaseName = 'VE70')")
df_ve70_part = df.select(col_names_part).withColumn('Plant',lit(plant_code_shangai)).withColumn('IndicatorFixedSupplier',lit('X')).withColumn('PurchOrg',lit('0070'))
df_ve70_part.createOrReplaceTempView("ve70_part_tmp")
df_ts_ve70_part = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_part = df_ts_ve70_part["max(UpdatedOn)"]
print(ts_ve70_part)

# COMMAND ----------

df = spark.sql("select * from VE72.PART where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PART' and TargetLayerTableName ='Purchase_Source_List' and DatabaseName = 'VE72')")
df_ve72_part = df.select(col_names_part).withColumn('Plant',lit(plant_code_shangai)).withColumn('IndicatorFixedSupplier',lit('X')).withColumn('PurchOrg',lit('0072'))
df_ve72_part.createOrReplaceTempView("ve72_part_tmp")
df_ts_ve72_part = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_part = df_ts_ve72_part["max(UpdatedOn)"]
print(ts_ve72_part)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view tmp_part
# MAGIC as
# MAGIC select *, 'VE70' as DataSource from ve70_part_tmp
# MAGIC union 
# MAGIC select *, 'VE72' as DataSource  from ve72_part_tmp

# COMMAND ----------

df = spark.sql("select * from S42.EORD where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EORD' and TargetLayerTableName ='Purchase_Source_List' and DatabaseName = 'S42')")
df_eord = df.select(col_names_eord)
df_eord.createOrReplaceTempView("eord_tmp")
df_ts_eord = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_eord = df_ts_eord["max(UpdatedOn)"]
print(ts_eord)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view Purchase_Source_List as
# MAGIC select
# MAGIC S.MATNR as MaterialNumber,
# MAGIC S.WERKS as Plant,
# MAGIC S.ZEORD as NoOfSrcListRec,
# MAGIC S.ERDAT as DateOnWhichRecWasCreated,
# MAGIC S.ERNAM as NameOfPerswhoCreatedObj,
# MAGIC S.VDATU as SrcListRecValidFrom,
# MAGIC S.BDATU as SrcListRecValidTo,
# MAGIC S.LIFNR as VendorsAccNo,
# MAGIC S.FLIFN as IndicatorFixedSupplier,
# MAGIC S.EBELN as AgreementNo,
# MAGIC S.EBELP as AgreementItem,
# MAGIC S.EKORG as PurchOrg,
# MAGIC S.VRTYP as PurchDocCateg,
# MAGIC S.EORTP as CategOfSrcListRec,
# MAGIC S.AUTET as SrcListUsageInMaterialsPlanning,
# MAGIC S.MEINS as PurchOrdUoM,
# MAGIC S.LASTCHANGEDATETIME as UTCTimeStampInLongForm,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC FROM eord_tmp as S
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC select
# MAGIC V.ID as MaterialNumber,
# MAGIC V.Plant as Plant,
# MAGIC ' ' as NoOfSrcListRec,
# MAGIC '' as DateOnWhichRecWasCreated,
# MAGIC '' as NameOfPerswhoCreatedObj,
# MAGIC '' as SrcListRecValidFrom,
# MAGIC '' as SrcListRecValidTo,
# MAGIC V.PREF_VENDOR_ID as VendorsAccNo,
# MAGIC V.IndicatorFixedSupplier as IndicatorFixedSupplier,
# MAGIC '' as AgreementNo,
# MAGIC '' as AgreementItem,
# MAGIC V.PurchOrg as PurchOrg,
# MAGIC '' as PurchDocCateg,
# MAGIC '' as CategOfSrcListRec,
# MAGIC '' as SrcListUsageInMaterialsPlanning,
# MAGIC '' as PurchOrdUoM,
# MAGIC '' as UTCTimeStampInLongForm ,
# MAGIC now() as UpdatedOn,
# MAGIC V.DataSource as DataSource
# MAGIC from tmp_part as V

# COMMAND ----------

df_purchase_source_list = spark.sql("select * from Purchase_Source_List")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_purchase_source_list.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_purchase_source_list.createOrReplaceTempView('tmp_purchase_source_list')

# COMMAND ----------

# MAGIC %sql 
# MAGIC Merge into fedw.purchase_source_list as S
# MAGIC Using tmp_purchase_source_list as T
# MAGIC on S.MaterialNumber = T.MaterialNumber and S.Plant = T.Plant and S.DataSource = T.DataSource
# MAGIC and S.NoOfSrcListRec = T.NoOfSrcListRec 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC S.MaterialNumber = T.MaterialNumber,
# MAGIC S.Plant = T.Plant,
# MAGIC S.NoOfSrcListRec = T.NoOfSrcListRec,
# MAGIC S.DateOnWhichRecWasCreated = T.DateOnWhichRecWasCreated,
# MAGIC S.NameOfPerswhoCreatedObj = T.NameOfPerswhoCreatedObj,
# MAGIC S.SrcListRecValidFrom = T.SrcListRecValidFrom,
# MAGIC S.SrcListRecValidTo = T.SrcListRecValidTo,
# MAGIC S.VendorsAccNo = T.VendorsAccNo,
# MAGIC S.IndicatorFixedSupplier = T.IndicatorFixedSupplier,
# MAGIC S.AgreementNo = T.AgreementNo,
# MAGIC S.AgreementItem = T.AgreementItem,
# MAGIC S.PurchOrg = T.PurchOrg,
# MAGIC S.PurchDocCateg = T.PurchDocCateg,
# MAGIC S.CategOfSrcListRec = T.CategOfSrcListRec,
# MAGIC S.SrcListUsageInMaterialsPlanning = T.SrcListUsageInMaterialsPlanning,
# MAGIC S.PurchOrdUoM = T.PurchOrdUoM,
# MAGIC S.UTCTimeStampInLongForm = T.UTCTimeStampInLongForm,
# MAGIC S.UpdatedOn = now(),
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT
# MAGIC (
# MAGIC MaterialNumber,
# MAGIC Plant,
# MAGIC NoOfSrcListRec,
# MAGIC DateOnWhichRecWasCreated,
# MAGIC NameOfPerswhoCreatedObj,
# MAGIC SrcListRecValidFrom,
# MAGIC SrcListRecValidTo,
# MAGIC VendorsAccNo,
# MAGIC IndicatorFixedSupplier,
# MAGIC AgreementNo,
# MAGIC AgreementItem,
# MAGIC PurchOrg,
# MAGIC PurchDocCateg,
# MAGIC CategOfSrcListRec,
# MAGIC SrcListUsageInMaterialsPlanning,
# MAGIC PurchOrdUoM,
# MAGIC UTCTimeStampInLongForm,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC Values
# MAGIC (
# MAGIC T.MaterialNumber,
# MAGIC T.Plant,
# MAGIC T.NoOfSrcListRec,
# MAGIC T.DateOnWhichRecWasCreated,
# MAGIC T.NameOfPerswhoCreatedObj,
# MAGIC T.SrcListRecValidFrom,
# MAGIC T.SrcListRecValidTo,
# MAGIC T.VendorsAccNo,
# MAGIC T.IndicatorFixedSupplier,
# MAGIC T.AgreementNo,
# MAGIC T.AgreementItem,
# MAGIC T.PurchOrg,
# MAGIC T.PurchDocCateg,
# MAGIC T.CategOfSrcListRec,
# MAGIC T.SrcListUsageInMaterialsPlanning,
# MAGIC T.PurchOrdUoM,
# MAGIC T.UTCTimeStampInLongForm,
# MAGIC now(),
# MAGIC T.DataSource
# MAGIC )

# COMMAND ----------

if(ts_eord != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EORD' and DatabaseName = 'S42'".format(ts_eord))

if(ts_ve70_part != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PART' and DatabaseName = 'VE70' and TargetLayerTableName ='Purchase_Source_List'".format(ts_ve70_part))
    
if(ts_ve72_part != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PART' and DatabaseName = 'VE72' and TargetLayerTableName ='Purchase_Source_List'".format(ts_ve72_part))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.purchase_source_list where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchase_Source_List' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_purchase_source_list = ts_sf["max(UpdatedOn)"]
print(ts_purchase_source_list)

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
  .option("dbtable", "purchase_source_list") \
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
# MAGIC Utils.runQuery(options, """call SP_Load_Purchase_Source_List()""")

# COMMAND ----------

if(ts_purchase_source_list != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchase_Source_List' and DatabaseName = 'FEDW'".format(ts_purchase_source_list))

# COMMAND ----------


