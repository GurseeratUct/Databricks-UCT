# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Purchase_Info_Record'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Fact/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_eine = [
    "INFNR",
"EKORG",
"ESOKZ",
"WERKS",
"ERDAT",
"ERNAM",
"EKGRP",
"WAERS",
"MINBM",
"NORBM",
"APLFZ",
"UEBTO",
"UNTTO",
"ANFPS",
"AMOBM",
"AMOBW",
"AMOAM",
"AMOAW",
"EBELP",
"NETPR",
"PEINH",
"BPRME",
"PRDAT",
"BPUMZ",
"BPUMN",
"MTXNO",
"WEBRE",
"EFFPR",
"MWSKZ",
"BSTAE",
"MHDRZ",
"BSTMA",
"FSH_RLT",
"FSH_MLT",
"FSH_PLT",
"FSH_TLT",
"MRPIND",
"STAGING_TIME"
]

# COMMAND ----------

col_names_eina = ["INFNR","MATNR","LIFNR"]

# COMMAND ----------

col_names_vendor_quote = [
"QUOTE_DATE",
"DEF_UNIT_PRICE_CURR",
"DEFAULT_UNIT_PRICE",
"PURCHASE_UM",
"Vendor_Part_ID",
"Vendor_ID"
]

# COMMAND ----------

df = spark.sql("select * from S42.EINE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EINE' and DatabaseName = 'S42')")
df_eine = df.select(col_names_eine)
df_eine.createOrReplaceTempView("eine_tmp")
df_ts_eine = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_eine = df_ts_eine["max(UpdatedOn)"]
print(ts_eine)

# COMMAND ----------

df = spark.sql("select * from S42.EINA where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EINA' and DatabaseName = 'S42' and TargetLayerTableName = 'Purchase_Info_Record')")
df_eina = df.select(col_names_eina)
df_eina.createOrReplaceTempView("eina_tmp")
df_ts_eina = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_eina = df_ts_eina["max(UpdatedOn)"]
print(ts_eina)

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

df = spark.sql("select * from VE70.VENDOR_QUOTE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'VENDOR_QUOTE' and DatabaseName = 'VE70')")
df_ve70_vendor_quote = df.select(col_names_vendor_quote).withColumn('Plant',lit(plant_code_shangai)).withColumn('DataSource',lit('VE70')).withColumn('PurchasingOrganization',lit('0070'))
df_ve70_vendor_quote.createOrReplaceTempView("ve70_vendor_quote_tmp")
df_ts_ve70_vendor_quote = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_vendor_quote = df_ts_ve70_vendor_quote["max(UpdatedOn)"]
print(ts_ve70_vendor_quote)

# COMMAND ----------

df = spark.sql("select * from VE72.VENDOR_QUOTE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'VENDOR_QUOTE' and DatabaseName = 'VE72')")
df_ve72_vendor_quote = df.select(col_names_vendor_quote).withColumn('Plant',lit(plant_code_shangai)).withColumn('DataSource',lit('VE72')).withColumn('PurchasingOrganization',lit('0072'))
df_ve72_vendor_quote.createOrReplaceTempView("ve72_vendor_quote_tmp")
df_ts_ve72_vendor_quote = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_vendor_quote = df_ts_ve72_vendor_quote["max(UpdatedOn)"]
print(ts_ve72_vendor_quote)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vendor_quote_tmp
# MAGIC as
# MAGIC select * from (
# MAGIC select * from ve70_vendor_quote_tmp
# MAGIC union all
# MAGIC select * from ve72_vendor_quote_tmp
# MAGIC )A

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view eine_eina_tmp
# MAGIC as
# MAGIC select
# MAGIC ei.INFNR,
# MAGIC ei.EKORG,
# MAGIC ei.ESOKZ,
# MAGIC ei.WERKS,
# MAGIC ei.ERDAT,
# MAGIC ei.ERNAM,
# MAGIC ei.EKGRP,
# MAGIC ei.WAERS,
# MAGIC ei.MINBM,
# MAGIC ei.NORBM,
# MAGIC ei.APLFZ,
# MAGIC ei.UEBTO,
# MAGIC ei.UNTTO,
# MAGIC ei.ANFPS,
# MAGIC ei.AMOBM,
# MAGIC ei.AMOBW,
# MAGIC ei.AMOAM,
# MAGIC ei.AMOAW,
# MAGIC ei.EBELP,
# MAGIC ei.NETPR,
# MAGIC ei.PEINH,
# MAGIC ei.BPRME,
# MAGIC ei.PRDAT,
# MAGIC ei.BPUMZ,
# MAGIC ei.BPUMN,
# MAGIC ei.MTXNO,
# MAGIC ei.WEBRE,
# MAGIC ei.EFFPR,
# MAGIC ei.MWSKZ,
# MAGIC ei.BSTAE,
# MAGIC ei.MHDRZ,
# MAGIC ei.BSTMA,
# MAGIC ei.FSH_RLT,
# MAGIC ei.FSH_MLT,
# MAGIC ei.FSH_PLT,
# MAGIC ei.FSH_TLT,
# MAGIC ei.MRPIND,
# MAGIC ei.STAGING_TIME,
# MAGIC ea.MATNR,
# MAGIC ea.LIFNR
# MAGIC from eine_tmp as ei
# MAGIC left join 
# MAGIC eina_tmp as ea
# MAGIC on 
# MAGIC ei.INFNR = ea.INFNR

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view purchase_info_record
# MAGIC as
# MAGIC select
# MAGIC S.INFNR as PurchInfoRecord,
# MAGIC S.EKORG as PurchasingOrganization,
# MAGIC S.ESOKZ as PurchInfoRecordCategory,
# MAGIC S.WERKS as Plant,
# MAGIC S.ERDAT as RecCreateDate,
# MAGIC S.ERNAM as CreatedBy,
# MAGIC S.EKGRP as PurchGroup,
# MAGIC S.WAERS as CurrKey,
# MAGIC S.MINBM as MinPurchOrdQuantity,
# MAGIC S.NORBM as StdPurchOrdQuantity,
# MAGIC S.APLFZ as PlannedDelTimeDays,
# MAGIC S.UEBTO as OverdeliveryTolerance,
# MAGIC S.UNTTO as UnderdeliveryTolerance,
# MAGIC S.ANFPS as ItemNoRFQ,
# MAGIC S.AMOBM as AmortizedPlannedQuantity,
# MAGIC S.AMOBW as AmortizedPlannedValue,
# MAGIC S.AMOAM as AmortizedActualQuantity,
# MAGIC S.AMOAW as AmortizedActualValue,
# MAGIC S.EBELP as ItemNoOfPurchDoc,
# MAGIC S.NETPR as PurchDocNetPrice,
# MAGIC S.PEINH as UnitPrice,
# MAGIC S.BPRME as OrderPriceUnit,
# MAGIC S.PRDAT as DateOfPriceDetermination,
# MAGIC S.BPUMZ as NumeConvOrderPriceUnitToOrdUnit,
# MAGIC S.BPUMN as DenoConvOrderPriceUnitToOrdUnit,
# MAGIC S.MTXNO as MaterialMastRecPOTextNotRelevant,
# MAGIC S.WEBRE as IndicatorGRBasedInvoiceVerification,
# MAGIC S.EFFPR as EffectivePricePurchInfoRec,
# MAGIC S.MWSKZ as TaxSalPurchCode,
# MAGIC S.BSTAE as ConfirmationControlKey,
# MAGIC S.MHDRZ as MiniRemainingShelfLife,
# MAGIC S.BSTMA as MaxPurchOrdQuantity,
# MAGIC S.FSH_RLT as ReplenishmentLeadTimeRawMaterial,
# MAGIC S.FSH_MLT as ManfacturingLeadTime,
# MAGIC S.FSH_PLT as PackingLeadtime,
# MAGIC S.FSH_TLT as TransportationLeadTime,
# MAGIC S.MRPIND as MaxRetPrice,
# MAGIC S.STAGING_TIME as StagingTime,
# MAGIC S.MATNR as MaterialNumber,
# MAGIC S.LIFNR as VendorAccountNo,
# MAGIC 'SAP' as DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from eine_eina_tmp as S
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC '' as PurchInfoRecord,
# MAGIC '' as PurchasingOrganization,
# MAGIC '' as PurchInfoRecordCategory,
# MAGIC '' as Plant,
# MAGIC V.QUOTE_DATE as RecCreateDate,
# MAGIC '' as CreatedBy,
# MAGIC '' as PurchGroup,
# MAGIC V.DEF_UNIT_PRICE_CURR as CurrKey,
# MAGIC '' as MinPurchOrdQuantity,
# MAGIC '' as StdPurchOrdQuantity,
# MAGIC '' as PlannedDelTimeDays,
# MAGIC '' as OverdeliveryTolerance,
# MAGIC '' as UnderdeliveryTolerance,
# MAGIC '' as ItemNoRFQ,
# MAGIC '' as AmortizedPlannedQuantity,
# MAGIC '' as AmortizedPlannedValue,
# MAGIC '' as AmortizedActualQuantity,
# MAGIC '' as AmortizedActualValue,
# MAGIC '' as ItemNoOfPurchDoc,
# MAGIC V.DEFAULT_UNIT_PRICE as PurchDocNetPrice,
# MAGIC 1 as UnitPrice,
# MAGIC V.PURCHASE_UM as OrderPriceUnit,
# MAGIC '' as DateOfPriceDetermination,
# MAGIC '' as NumeConvOrderPriceUnitToOrdUnit,
# MAGIC '' as DenoConvOrderPriceUnitToOrdUnit,
# MAGIC '' as MaterialMastRecPOTextNotRelevant,
# MAGIC '' as IndicatorGRBasedInvoiceVerification,
# MAGIC '' as EffectivePricePurchInfoRec,
# MAGIC '' as TaxSalPurchCode,
# MAGIC '' as ConfirmationControlKey,
# MAGIC '' as MiniRemainingShelfLife,
# MAGIC '' as MaxPurchOrdQuantity,
# MAGIC '' as ReplenishmentLeadTimeRawMaterial,
# MAGIC '' as ManfacturingLeadTime,
# MAGIC '' as PackingLeadtime,
# MAGIC '' as TransportationLeadTime,
# MAGIC '' as MaxRetPrice,
# MAGIC '' as StagingTime,
# MAGIC V.Vendor_Part_ID as MaterialNumber,
# MAGIC V.Vendor_ID as VendorAccountNo,
# MAGIC V.DataSource as DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from 
# MAGIC vendor_quote_tmp as V

# COMMAND ----------

df_purchase_info_record = spark.sql("select * from purchase_info_record")
df_purchase_info_record.count()

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_purchase_info_record.write.format(write_format).mode("overwrite").partitionBy("DataSource").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_purchase_info_record.createOrReplaceTempView('tmp_purchase_info_record')

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into fedw.purchase_info_record as T
# MAGIC Using (select * from (select row_number() OVER (PARTITION BY PurchInfoRecord,PurchasingOrganization,PurchInfoRecordCategory,Plant,Datasource ORDER BY PurchInfoRecord DESC) as rn,* from tmp_purchase_info_record)A where A.rn = 1 ) as S 
# MAGIC on T.PurchInfoRecord = S.PurchInfoRecord 
# MAGIC and T.PurchasingOrganization = S.PurchasingOrganization 
# MAGIC and T.PurchInfoRecordCategory = S.PurchInfoRecordCategory
# MAGIC and T.Plant = S.Plant
# MAGIC and 'SAP' = S.DataSource
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`PurchInfoRecord` =  S.`PurchInfoRecord`,
# MAGIC T.`PurchasingOrganization` =  S.`PurchasingOrganization`,
# MAGIC T.`PurchInfoRecordCategory` =  S.`PurchInfoRecordCategory`,
# MAGIC T.`Plant` =  S.`Plant`,
# MAGIC T.`RecCreateDate` =  S.`RecCreateDate`,
# MAGIC T.`CreatedBy` =  S.`CreatedBy`,
# MAGIC T.`PurchGroup` =  S.`PurchGroup`,
# MAGIC T.`CurrKey` =  S.`CurrKey`,
# MAGIC T.`MinPurchOrdQuantity` =  S.`MinPurchOrdQuantity`,
# MAGIC T.`StdPurchOrdQuantity` =  S.`StdPurchOrdQuantity`,
# MAGIC T.`PlannedDelTimeDays` =  S.`PlannedDelTimeDays`,
# MAGIC T.`OverdeliveryTolerance` =  S.`OverdeliveryTolerance`,
# MAGIC T.`UnderdeliveryTolerance` =  S.`UnderdeliveryTolerance`,
# MAGIC T.`ItemNoRFQ` =  S.`ItemNoRFQ`,
# MAGIC T.`AmortizedPlannedQuantity` =  S.`AmortizedPlannedQuantity`,
# MAGIC T.`AmortizedPlannedValue` =  S.`AmortizedPlannedValue`,
# MAGIC T.`AmortizedActualQuantity` =  S.`AmortizedActualQuantity`,
# MAGIC T.`AmortizedActualValue` =  S.`AmortizedActualValue`,
# MAGIC T.`ItemNoOfPurchDoc` =  S.`ItemNoOfPurchDoc`,
# MAGIC T.`PurchDocNetPrice` =  S.`PurchDocNetPrice`,
# MAGIC T.`UnitPrice` =  S.`UnitPrice`,
# MAGIC T.`OrderPriceUnit` =  S.`OrderPriceUnit`,
# MAGIC T.`DateOfPriceDetermination` =  S.`DateOfPriceDetermination`,
# MAGIC T.`NumeConvOrderPriceUnitToOrdUnit` =  S.`NumeConvOrderPriceUnitToOrdUnit`,
# MAGIC T.`DenoConvOrderPriceUnitToOrdUnit` =  S.`DenoConvOrderPriceUnitToOrdUnit`,
# MAGIC T.`MaterialMastRecPOTextNotRelevant` =  S.`MaterialMastRecPOTextNotRelevant`,
# MAGIC T.`IndicatorGRBasedInvoiceVerification` =  S.`IndicatorGRBasedInvoiceVerification`,
# MAGIC T.`EffectivePricePurchInfoRec` =  S.`EffectivePricePurchInfoRec`,
# MAGIC T.`TaxSalPurchCode` =  S.`TaxSalPurchCode`,
# MAGIC T.`ConfirmationControlKey` =  S.`ConfirmationControlKey`,
# MAGIC T.`MiniRemainingShelfLife` =  S.`MiniRemainingShelfLife`,
# MAGIC T.`MaxPurchOrdQuantity` =  S.`MaxPurchOrdQuantity`,
# MAGIC T.`ReplenishmentLeadTimeRawMaterial` =  S.`ReplenishmentLeadTimeRawMaterial`,
# MAGIC T.`ManfacturingLeadTime` =  S.`ManfacturingLeadTime`,
# MAGIC T.`PackingLeadtime` =  S.`PackingLeadtime`,
# MAGIC T.`TransportationLeadTime` =  S.`TransportationLeadTime`,
# MAGIC T.`MaxRetPrice` =  S.`MaxRetPrice`,
# MAGIC T.`StagingTime` =  S.`StagingTime`,
# MAGIC T.`MaterialNumber` =  S.`MaterialNumber`,
# MAGIC T.`VendorAccountNo` =  S.`VendorAccountNo`,
# MAGIC T.`DataSource` =  S.`DataSource`,
# MAGIC T.`UpdatedOn` =  S.`UpdatedOn`
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT
# MAGIC (
# MAGIC `PurchInfoRecord`,
# MAGIC `PurchasingOrganization`,
# MAGIC `PurchInfoRecordCategory`,
# MAGIC `Plant`,
# MAGIC `RecCreateDate`,
# MAGIC `CreatedBy`,
# MAGIC `PurchGroup`,
# MAGIC `CurrKey`,
# MAGIC `MinPurchOrdQuantity`,
# MAGIC `StdPurchOrdQuantity`,
# MAGIC `PlannedDelTimeDays`,
# MAGIC `OverdeliveryTolerance`,
# MAGIC `UnderdeliveryTolerance`,
# MAGIC `ItemNoRFQ`,
# MAGIC `AmortizedPlannedQuantity`,
# MAGIC `AmortizedPlannedValue`,
# MAGIC `AmortizedActualQuantity`,
# MAGIC `AmortizedActualValue`,
# MAGIC `ItemNoOfPurchDoc`,
# MAGIC `PurchDocNetPrice`,
# MAGIC `UnitPrice`,
# MAGIC `OrderPriceUnit`,
# MAGIC `DateOfPriceDetermination`,
# MAGIC `NumeConvOrderPriceUnitToOrdUnit`,
# MAGIC `DenoConvOrderPriceUnitToOrdUnit`,
# MAGIC `MaterialMastRecPOTextNotRelevant`,
# MAGIC `IndicatorGRBasedInvoiceVerification`,
# MAGIC `EffectivePricePurchInfoRec`,
# MAGIC `TaxSalPurchCode`,
# MAGIC `ConfirmationControlKey`,
# MAGIC `MiniRemainingShelfLife`,
# MAGIC `MaxPurchOrdQuantity`,
# MAGIC `ReplenishmentLeadTimeRawMaterial`,
# MAGIC `ManfacturingLeadTime`,
# MAGIC `PackingLeadtime`,
# MAGIC `TransportationLeadTime`,
# MAGIC `MaxRetPrice`,
# MAGIC `StagingTime`,
# MAGIC `MaterialNumber`,
# MAGIC `VendorAccountNo`,
# MAGIC `DataSource`,
# MAGIC `UpdatedOn`
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`PurchInfoRecord`,
# MAGIC S.`PurchasingOrganization`,
# MAGIC S.`PurchInfoRecordCategory`,
# MAGIC S.`Plant`,
# MAGIC S.`RecCreateDate`,
# MAGIC S.`CreatedBy`,
# MAGIC S.`PurchGroup`,
# MAGIC S.`CurrKey`,
# MAGIC S.`MinPurchOrdQuantity`,
# MAGIC S.`StdPurchOrdQuantity`,
# MAGIC S.`PlannedDelTimeDays`,
# MAGIC S.`OverdeliveryTolerance`,
# MAGIC S.`UnderdeliveryTolerance`,
# MAGIC S.`ItemNoRFQ`,
# MAGIC S.`AmortizedPlannedQuantity`,
# MAGIC S.`AmortizedPlannedValue`,
# MAGIC S.`AmortizedActualQuantity`,
# MAGIC S.`AmortizedActualValue`,
# MAGIC S.`ItemNoOfPurchDoc`,
# MAGIC S.`PurchDocNetPrice`,
# MAGIC S.`UnitPrice`,
# MAGIC S.`OrderPriceUnit`,
# MAGIC S.`DateOfPriceDetermination`,
# MAGIC S.`NumeConvOrderPriceUnitToOrdUnit`,
# MAGIC S.`DenoConvOrderPriceUnitToOrdUnit`,
# MAGIC S.`MaterialMastRecPOTextNotRelevant`,
# MAGIC S.`IndicatorGRBasedInvoiceVerification`,
# MAGIC S.`EffectivePricePurchInfoRec`,
# MAGIC S.`TaxSalPurchCode`,
# MAGIC S.`ConfirmationControlKey`,
# MAGIC S.`MiniRemainingShelfLife`,
# MAGIC S.`MaxPurchOrdQuantity`,
# MAGIC S.`ReplenishmentLeadTimeRawMaterial`,
# MAGIC S.`ManfacturingLeadTime`,
# MAGIC S.`PackingLeadtime`,
# MAGIC S.`TransportationLeadTime`,
# MAGIC S.`MaxRetPrice`,
# MAGIC S.`StagingTime`,
# MAGIC S.`MaterialNumber`,
# MAGIC S.`VendorAccountNo`,
# MAGIC S.`DataSource`,
# MAGIC now()
# MAGIC )

# COMMAND ----------

if(ts_eine != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EINE' and DatabaseName = 'S42'".format(ts_eine))
    
if(ts_eina != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EINA' and TargetLayerTableName = 'Purchase_Info_Record' and DatabaseName = 'S42'".format(ts_eina))

if(ts_ve70_vendor_quote != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'VENDOR_QUOTE' and DatabaseName = 'VE70'".format(ts_ve70_vendor_quote))
    
if(ts_ve72_vendor_quote != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'VENDOR_QUOTE' and DatabaseName = 'VE72'".format(ts_ve72_vendor_quote))


# COMMAND ----------

df_sf = spark.sql("select * from FEDW.purchase_info_record where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchase_Info_Record' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_purchase_info_record = ts_sf["max(UpdatedOn)"]
print(ts_purchase_info_record)

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
  .option("dbtable", "Purchase_Info_Record") \
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
# MAGIC Utils.runQuery(options, """call sp_load_purchase_info_record()""")

# COMMAND ----------

if(ts_purchase_info_record != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchase_Info_Record' and DatabaseName = 'FEDW'".format(ts_purchase_info_record))

# COMMAND ----------


