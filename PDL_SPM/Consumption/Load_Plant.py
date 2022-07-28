# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Plant'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_t001w = [
    "WERKS",
"NAME1",
"BWKEY",
"KUNNR",
"LIFNR",
"FABKL",
"STRAS",
"PSTLZ",
"ORT01",
"EKORG",
"VKORG",
"BEDPL",
"LAND1",
"REGIO",
"ADRNR",
"TXJCD",
"VTWEG",
"SPART",
"SPRAS",
"AWSLS",
"LET01",
"LET02",
"LET03",
"TXNAM_MA1",
"TXNAM_MA2",
"TXNAM_MA3",
"BETOL",
"PKOSA",
"MISCH",
"MGVUPD",
"VSTEL"
]

# COMMAND ----------

df = spark.sql("select * from S42.T001W where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T001W' and DatabaseName = 'S42')")
df_t001W = df.select(col_names_t001w)
df_t001W.createOrReplaceTempView("tmp_t001W")
df_ts_t001W = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_t001W = df_ts_t001W["max(UpdatedOn)"]
print(ts_t001W)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_plant as 
# MAGIC select 
# MAGIC WERKS as Plant,
# MAGIC NAME1 as Name,
# MAGIC BWKEY as ValuationArea,
# MAGIC KUNNR as CustomerNumber,
# MAGIC LIFNR as SupplierNumber,
# MAGIC FABKL as FactoryCalendarKey,
# MAGIC STRAS as StreetHouseNumber,
# MAGIC PSTLZ as PostalCode,
# MAGIC ORT01 as City,
# MAGIC EKORG as PurchasingOrganization,
# MAGIC VKORG as SalesOrganization,
# MAGIC BEDPL as Activatingreqplanning,
# MAGIC LAND1 as CountryKey,
# MAGIC REGIO as Region,
# MAGIC ADRNR as Address,
# MAGIC TXJCD as TaxJurisdiction,
# MAGIC VTWEG as DistributionChannel,
# MAGIC SPART as Division,
# MAGIC SPRAS as LanguageKey,
# MAGIC AWSLS as VarianceKey,
# MAGIC LET01 as DaysForFirstReminder,
# MAGIC LET02 as DaysForSecondReminder,
# MAGIC LET03 as DaysForThirdReminder,
# MAGIC TXNAM_MA1 as FirstDunningOfVendorDeclarations,
# MAGIC TXNAM_MA2 as SecondDunningOfVendorDeclarations,
# MAGIC TXNAM_MA3 as ThirdDunningOfVendorDeclarations,
# MAGIC BETOL as DaysForPOTolerance,
# MAGIC PKOSA as CostObjectControllingLinkingActive,
# MAGIC MISCH as UpdatingIsActiveForMixedCosting,
# MAGIC MGVUPD as UpdatingIsActiveInActualCosting,
# MAGIC VSTEL as ShippingPointOrReceivingPoint,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_t001W

# COMMAND ----------

df_merge = spark.sql("select * from merge_plant")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")    

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_plant')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Plant as T 
# MAGIC USING stg_merge_plant as S 
# MAGIC ON T.Plant = S.Plant
# MAGIC and T.DataSource = S.DataSource
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Plant = S.Plant,
# MAGIC T.Name = S.Name,
# MAGIC T.ValuationArea = S.ValuationArea,
# MAGIC T.CustomerNumber = S.CustomerNumber,
# MAGIC T.SupplierNumber = S.SupplierNumber,
# MAGIC T.FactoryCalendarKey = S.FactoryCalendarKey,
# MAGIC T.StreetHouseNumber = S.StreetHouseNumber,
# MAGIC T.PostalCode = S.PostalCode,
# MAGIC T.City = S.City,
# MAGIC T.PurchasingOrganization = S.PurchasingOrganization,
# MAGIC T.SalesOrganization = S.SalesOrganization,
# MAGIC T.Activatingreqplanning = S.Activatingreqplanning,
# MAGIC T.CountryKey = S.CountryKey,
# MAGIC T.Region = S.Region,
# MAGIC T.Address = S.Address,
# MAGIC T.TaxJurisdiction = S.TaxJurisdiction,
# MAGIC T.DistributionChannel = S.DistributionChannel,
# MAGIC T.Division = S.Division,
# MAGIC T.LanguageKey = S.LanguageKey,
# MAGIC T.VarianceKey = S.VarianceKey,
# MAGIC T.DaysForFirstReminder = S.DaysForFirstReminder,
# MAGIC T.DaysForSecondReminder = S.DaysForSecondReminder,
# MAGIC T.DaysForThirdReminder = S.DaysForThirdReminder,
# MAGIC T.FirstDunningOfVendorDeclarations = S.FirstDunningOfVendorDeclarations,
# MAGIC T.SecondDunningOfVendorDeclarations = S.SecondDunningOfVendorDeclarations,
# MAGIC T.ThirdDunningOfVendorDeclarations = S.ThirdDunningOfVendorDeclarations,
# MAGIC T.DaysForPOTolerance = S.DaysForPOTolerance,
# MAGIC T.CostObjectControllingLinkingActive = S.CostObjectControllingLinkingActive,
# MAGIC T.UpdatingIsActiveForMixedCosting = S.UpdatingIsActiveForMixedCosting,
# MAGIC T.UpdatingIsActiveInActualCosting = S.UpdatingIsActiveInActualCosting,
# MAGIC T.ShippingPointOrReceivingPoint = S.ShippingPointOrReceivingPoint,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Plant,
# MAGIC Name,
# MAGIC ValuationArea,
# MAGIC CustomerNumber,
# MAGIC SupplierNumber,
# MAGIC FactoryCalendarKey,
# MAGIC StreetHouseNumber,
# MAGIC PostalCode,
# MAGIC City,
# MAGIC PurchasingOrganization,
# MAGIC SalesOrganization,
# MAGIC Activatingreqplanning,
# MAGIC CountryKey,
# MAGIC Region,
# MAGIC Address,
# MAGIC TaxJurisdiction,
# MAGIC DistributionChannel,
# MAGIC Division,
# MAGIC LanguageKey,
# MAGIC VarianceKey,
# MAGIC DaysForFirstReminder,
# MAGIC DaysForSecondReminder,
# MAGIC DaysForThirdReminder,
# MAGIC FirstDunningOfVendorDeclarations,
# MAGIC SecondDunningOfVendorDeclarations,
# MAGIC ThirdDunningOfVendorDeclarations,
# MAGIC DaysForPOTolerance,
# MAGIC CostObjectControllingLinkingActive,
# MAGIC UpdatingIsActiveForMixedCosting,
# MAGIC UpdatingIsActiveInActualCosting,
# MAGIC ShippingPointOrReceivingPoint,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Plant,
# MAGIC S.Name,
# MAGIC S.ValuationArea,
# MAGIC S.CustomerNumber,
# MAGIC S.SupplierNumber,
# MAGIC S.FactoryCalendarKey,
# MAGIC S.StreetHouseNumber,
# MAGIC S.PostalCode,
# MAGIC S.City,
# MAGIC S.PurchasingOrganization,
# MAGIC S.SalesOrganization,
# MAGIC S.Activatingreqplanning,
# MAGIC S.CountryKey,
# MAGIC S.Region,
# MAGIC S.Address,
# MAGIC S.TaxJurisdiction,
# MAGIC S.DistributionChannel,
# MAGIC S.Division,
# MAGIC S.LanguageKey,
# MAGIC S.VarianceKey,
# MAGIC S.DaysForFirstReminder,
# MAGIC S.DaysForSecondReminder,
# MAGIC S.DaysForThirdReminder,
# MAGIC S.FirstDunningOfVendorDeclarations,
# MAGIC S.SecondDunningOfVendorDeclarations,
# MAGIC S.ThirdDunningOfVendorDeclarations,
# MAGIC S.DaysForPOTolerance,
# MAGIC S.CostObjectControllingLinkingActive,
# MAGIC S.UpdatingIsActiveForMixedCosting,
# MAGIC S.UpdatingIsActiveInActualCosting,
# MAGIC S.ShippingPointOrReceivingPoint,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_t001W != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T001W' and DatabaseName = 'S42'".format(ts_t001W))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Plant where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Plant' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_plant = ts_sf["max(UpdatedOn)"]
print(ts_plant)

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
  .option("dbtable", "Plant") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_PLANT()""")

# COMMAND ----------

if(ts_plant != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Plant' and DatabaseName = 'FEDW'".format(ts_plant))

# COMMAND ----------


