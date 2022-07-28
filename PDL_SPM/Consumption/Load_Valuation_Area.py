# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Valuation_Area'
table_name = 'T001K' 
read_format = 'delta'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names = [   'MANDT',
                'BWKEY',
                'BUKRS',
                'BWMOD',
                'XBKNG',
                'MLBWA',
                'MLBWV',
                'XVKBW',
                'ERKLAERKOM',
                'UPROF',
                'WBPRO',
                'MLAST',
                'MLASV',
                'BDIFP',
                'XLBPD',
                'XEWRX',
                'X2FDO',
                'PRSFR',
                'MLCCS',
                'XEFRE',
                'EFREJ',
                '/FMP/PRSFR',
                '/FMP/PRFRGR',
                'ODQ_CHANGEMODE',
                'ODQ_ENTITYCNTR',
                'LandingFileTimeStamp',
                'UpdatedOn',
                'DataSource']

# COMMAND ----------

df = spark.sql("select * from S42.T001K where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T001K')")

# COMMAND ----------

df_ts = df.agg({"UpdatedOn": "max"}).collect()[0]
ts = df_ts["max(UpdatedOn)"]
print(ts)

# COMMAND ----------

df_select_col = df.select(col_names)

# COMMAND ----------

df_rename = df_select_col.withColumnRenamed('MANDT','Client') \
                        .withColumnRenamed('BWKEY','ValuationArea') \
                        .withColumnRenamed('BUKRS','CompanyCode') \
                        .withColumnRenamed('BWMOD','ValuationGroupingCode') \
                        .withColumnRenamed('XBKNG','NegativeStocksInValuationAreaAllowed') \
                        .withColumnRenamed('MLBWA','MaterialLedgerActivatedInValuationArea') \
                        .withColumnRenamed('MLBWV','MaterialLedgerActivatedInValuationAreaCompulsory') \
                        .withColumnRenamed('XVKBW','SalesPriceValuationActive') \
                        .withColumnRenamed('ERKLAERKOM','ExplanationFacilityForMaterialLedger') \
                        .withColumnRenamed('UPROF','RetailRevalutionProfile') \
                        .withColumnRenamed('WBPRO','ProfileForValueBasedInventory') \
                        .withColumnRenamed('MLAST','MaterialPriceDeterminationControl') \
                        .withColumnRenamed('MLASV','PriceDeterminationBindingInValuationArea') \
                        .withColumnRenamed('BDIFP','StockCorrectionTolerance') \
                        .withColumnRenamed('XLBPD','PriceDifferenceInGRForSubcontract') \
                        .withColumnRenamed('XEWRX','PostPurchaseAccountWithReceiptValue') \
                        .withColumnRenamed('X2FDO','TwoFIDocumentsWithPurchaseAccount') \
                        .withColumnRenamed('PRSFR','PriceRelease') \
                        .withColumnRenamed('MLCCS','ActiveActualCostComponentSplit') \
                        .withColumnRenamed('XEFRE','DelCostsToPricediffAcctWhenPurchAcc') \
                        .withColumnRenamed('EFREJ','StartOfValidityPeriodFDelCostsInPriceDiffAcct') \
                        .withColumnRenamed('/FMP/PRSFR','PriceReleaseFlexibleMaterialPrices') \
                        .withColumnRenamed('/FMP/PRFRGR','PriceReleaseGroup')  \
                        .withColumnRenamed('LandingFileTimeStamp','LandingFileTimeStamp') \
                        .withColumnRenamed('UpdatedOn','UpdatedOn') \
                        .withColumn('DataSource',lit('SAP'))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_rename.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_rename.createOrReplaceTempView('stg_valuation_area')

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO fedw.Valuation_Area  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY Client,ValuationArea ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_valuation_area)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.Client = T.Client 
# MAGIC and 
# MAGIC S.ValuationArea = T.ValuationArea
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  S.Client = T.Client,
# MAGIC S.ValuationArea = T.ValuationArea,
# MAGIC S.CompanyCode = T.CompanyCode,
# MAGIC S.ValuationGroupingCode = T.ValuationGroupingCode,
# MAGIC S.NegativeStocksInValuationAreaAllowed = T.NegativeStocksInValuationAreaAllowed,
# MAGIC S.MaterialLedgerActivatedInValuationArea = T.MaterialLedgerActivatedInValuationArea,
# MAGIC S.MaterialLedgerActivatedInValuationAreaCompulsory = T.MaterialLedgerActivatedInValuationAreaCompulsory,
# MAGIC S.SalesPriceValuationActive = T.SalesPriceValuationActive,
# MAGIC S.ExplanationFacilityForMaterialLedger = T.ExplanationFacilityForMaterialLedger,
# MAGIC S.RetailRevalutionProfile = T.RetailRevalutionProfile,
# MAGIC S.ProfileForValueBasedInventory = T.ProfileForValueBasedInventory,
# MAGIC S.MaterialPriceDeterminationControl = T.MaterialPriceDeterminationControl,
# MAGIC S.PriceDeterminationBindingInValuationArea = T.PriceDeterminationBindingInValuationArea,
# MAGIC S.StockCorrectionTolerance = T.StockCorrectionTolerance,
# MAGIC S.PriceDifferenceInGRForSubcontract = T.PriceDifferenceInGRForSubcontract,
# MAGIC S.PostPurchaseAccountWithReceiptValue = T.PostPurchaseAccountWithReceiptValue,
# MAGIC S.TwoFIDocumentsWithPurchaseAccount = T.TwoFIDocumentsWithPurchaseAccount,
# MAGIC S.PriceRelease = T.PriceRelease,
# MAGIC S.ActiveActualCostComponentSplit = T.ActiveActualCostComponentSplit,
# MAGIC S.DelCostsToPricediffAcctWhenPurchAcc = T.DelCostsToPricediffAcctWhenPurchAcc,
# MAGIC S.StartOfValidityPeriodFDelCostsInPriceDiffAcct = T.StartOfValidityPeriodFDelCostsInPriceDiffAcct,
# MAGIC S.PriceReleaseFlexibleMaterialPrices = T.PriceReleaseFlexibleMaterialPrices,
# MAGIC S.PriceReleaseGroup = T.PriceReleaseGroup,
# MAGIC S.ODQ_CHANGEMODE = T.ODQ_CHANGEMODE,
# MAGIC S.ODQ_ENTITYCNTR = T.ODQ_ENTITYCNTR,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC Client,
# MAGIC ValuationArea,
# MAGIC CompanyCode,
# MAGIC ValuationGroupingCode,
# MAGIC NegativeStocksInValuationAreaAllowed,
# MAGIC MaterialLedgerActivatedInValuationArea,
# MAGIC MaterialLedgerActivatedInValuationAreaCompulsory,
# MAGIC SalesPriceValuationActive,
# MAGIC ExplanationFacilityForMaterialLedger,
# MAGIC RetailRevalutionProfile,
# MAGIC ProfileForValueBasedInventory,
# MAGIC MaterialPriceDeterminationControl,
# MAGIC PriceDeterminationBindingInValuationArea,
# MAGIC StockCorrectionTolerance,
# MAGIC PriceDifferenceInGRForSubcontract,
# MAGIC PostPurchaseAccountWithReceiptValue,
# MAGIC TwoFIDocumentsWithPurchaseAccount,
# MAGIC PriceRelease,
# MAGIC ActiveActualCostComponentSplit,
# MAGIC DelCostsToPricediffAcctWhenPurchAcc,
# MAGIC StartOfValidityPeriodFDelCostsInPriceDiffAcct,
# MAGIC PriceReleaseFlexibleMaterialPrices,
# MAGIC PriceReleaseGroup,
# MAGIC ODQ_CHANGEMODE,
# MAGIC ODQ_ENTITYCNTR,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.Client ,
# MAGIC T.ValuationArea ,
# MAGIC T.CompanyCode ,
# MAGIC T.ValuationGroupingCode ,
# MAGIC T.NegativeStocksInValuationAreaAllowed ,
# MAGIC T.MaterialLedgerActivatedInValuationArea ,
# MAGIC T.MaterialLedgerActivatedInValuationAreaCompulsory ,
# MAGIC T.SalesPriceValuationActive ,
# MAGIC T.ExplanationFacilityForMaterialLedger ,
# MAGIC T.RetailRevalutionProfile ,
# MAGIC T.ProfileForValueBasedInventory ,
# MAGIC T.MaterialPriceDeterminationControl ,
# MAGIC T.PriceDeterminationBindingInValuationArea ,
# MAGIC T.StockCorrectionTolerance ,
# MAGIC T.PriceDifferenceInGRForSubcontract ,
# MAGIC T.PostPurchaseAccountWithReceiptValue ,
# MAGIC T.TwoFIDocumentsWithPurchaseAccount ,
# MAGIC T.PriceRelease ,
# MAGIC T.ActiveActualCostComponentSplit ,
# MAGIC T.DelCostsToPricediffAcctWhenPurchAcc ,
# MAGIC T.StartOfValidityPeriodFDelCostsInPriceDiffAcct ,
# MAGIC T.PriceReleaseFlexibleMaterialPrices ,
# MAGIC T.PriceReleaseGroup ,
# MAGIC T.ODQ_CHANGEMODE ,
# MAGIC T.ODQ_ENTITYCNTR ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC T.DataSource
# MAGIC )

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T001K'".format(ts))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Valuation_Area where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Valuation_Area')")

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
  .option("dbtable", "Valuation_Area") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_Valuation_Area()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Valuation_Area'".format(ts))

# COMMAND ----------


