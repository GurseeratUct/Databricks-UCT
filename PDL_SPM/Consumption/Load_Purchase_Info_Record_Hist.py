# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Purchase_Info_Record_Hist'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Fact/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_eina = ["INFNR","MATNR","LIFNR"]

# COMMAND ----------

col_names_eipa = ["INFNR",
"EKORG",
"EBELN",
"EBELP",                  
"ESOKZ",
"WERKS",
"BEDAT",
"PREIS",
"PEINH",
"BPRME",
"BWAER",
"LPEIN"]
    

# COMMAND ----------

col_names_vendor_quote_history = [
    "VENDOR_ID",
    "VENDOR_PART_ID",
    "PART_ID",
    "CHANGE_DATE",
    "NEW_DEFAULT_UNIT_PRICE",
    "DEF_UNIT_PRICE_CURR",
    "QUOTE_DATE",
    "NEW_PURCHASE_UM"
]


# COMMAND ----------

df = spark.sql("select * from S42.EIPA where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EIPA' and DatabaseName = 'S42')")
df_eipa = df.select(col_names_eipa)
df_eipa.createOrReplaceTempView("eipa_tmp")
df_ts_eipa = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_eipa = df_ts_eipa["max(UpdatedOn)"]
print(ts_eipa)

# COMMAND ----------

df = spark.sql("select * from S42.EINA where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EINA' and TargetLayerTableName = 'Purchase_Info_Record_Hist' and DatabaseName = 'S42')")
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

df = spark.sql("select * from VE70.UCT_VENDOR_QUOTE_HISTORY where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_VENDOR_QUOTE_HISTORY' and DatabaseName = 'VE70')")
df_ve70_vendor_quote_hist = df.select(col_names_vendor_quote_history).withColumn('Plant',lit(plant_code_shangai)).withColumn('DataSource',lit('VE70')).withColumn('PurchasingOrganization',lit('0070'))
df_ve70_vendor_quote_hist.createOrReplaceTempView("ve70_vendor_quote_hist_tmp")
df_ts_ve70_vendor_quote_hist = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_vendor_quote_hist = df_ts_ve70_vendor_quote_hist["max(UpdatedOn)"]
print(ts_ve70_vendor_quote_hist)

# COMMAND ----------

df = spark.sql("select * from VE72.UCT_VENDOR_QUOTE_HISTORY where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_VENDOR_QUOTE_HISTORY' and DatabaseName = 'VE72')")
df_ve72_vendor_quote_hist = df.select(col_names_vendor_quote_history).withColumn('Plant',lit(plant_code_shangai)).withColumn('DataSource',lit('VE72')).withColumn('PurchasingOrganization',lit('0072'))
df_ve72_vendor_quote_hist.createOrReplaceTempView("ve72_vendor_quote_hist_tmp")
df_ts_ve72_vendor_quote_hist = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_vendor_quote_hist = df_ts_ve72_vendor_quote_hist["max(UpdatedOn)"]
print(ts_ve72_vendor_quote_hist)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vendor_quote_hist_tmp
# MAGIC as
# MAGIC select * from (
# MAGIC select * from ve70_vendor_quote_hist_tmp
# MAGIC union all
# MAGIC select * from ve72_vendor_quote_hist_tmp
# MAGIC )A

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view vendor_quote_hist_tmp_1 
# MAGIC as
# MAGIC select VENDOR_ID,VENDOR_PART_ID,PART_ID,CHANGE_DATE,NEW_DEFAULT_UNIT_PRICE,DEF_UNIT_PRICE_CURR,QUOTE_DATE,NEW_PURCHASE_UM,Plant,DataSource,PurchasingOrganization from (select ROW_NUMBER() OVER (PARTITION BY VENDOR_ID,VENDOR_PART_ID,PLANT,QUOTE_DATE ORDER BY CHANGE_DATE DESC) as rn,* 
# MAGIC from vendor_quote_hist_tmp)A where rn = 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view vendor_quote_hist_tmp_2 
# MAGIC as
# MAGIC select *,Lag(QUOTE_DATE, 1, '9999-12-31') OVER(partition by VENDOR_ID,PLANT,VENDOR_PART_ID,DATASOURCE ORDER BY QUOTE_DATE DESC)-1 as PriceValidUntil  from vendor_quote_hist_tmp_1 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view eipa_eina_tmp
# MAGIC as
# MAGIC select
# MAGIC ep.INFNR,
# MAGIC ep.EKORG,
# MAGIC ep.EBELN,
# MAGIC ep.EBELP,                  
# MAGIC ep.ESOKZ,
# MAGIC ep.WERKS,
# MAGIC ep.BEDAT,
# MAGIC ep.PREIS,
# MAGIC ep.PEINH,
# MAGIC ep.BPRME,
# MAGIC ep.BWAER,
# MAGIC ep.LPEIN,
# MAGIC ea.MATNR,
# MAGIC ea.LIFNR
# MAGIC from eipa_tmp as ep
# MAGIC left join eina_tmp as ea on ep.INFNR = ea.INFNR

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view eipa_eina_tmp_1
# MAGIC as
# MAGIC select *,Lag(BEDAT, 1, '9999-12-31') OVER(partition by INFNR,EKORG ORDER BY BEDAT DESC)-1 as PriceValidUntil from eipa_eina_tmp 

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view purchase_info_record_Hist
# MAGIC as
# MAGIC select
# MAGIC S.INFNR as PurchInfoRecord,
# MAGIC S.EKORG as PurchasingOrganization,
# MAGIC S.EBELN as PurchDocNumber,
# MAGIC S.EBELP as ItemNoOfPurchDoc,
# MAGIC S.ESOKZ as PurchInfRecCategory,
# MAGIC S.WERKS as Plant,
# MAGIC S.BEDAT as PurchOrdDate,
# MAGIC '' as ChangeDate,
# MAGIC S.PriceValidUntil as PriceValidUntil,
# MAGIC S.PREIS as PurchDocNetPrice,
# MAGIC S.PEINH as UnitPrice,
# MAGIC S.BPRME as OrderPriceUnit,
# MAGIC S.BWAER as CurrKey,
# MAGIC S.LPEIN as PriceUnit,
# MAGIC S.MATNR as VendorPartId,
# MAGIC S.MATNR as MaterialNumber,
# MAGIC S.LIFNR as VendorAccountNo,
# MAGIC 'SAP' as DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from eipa_eina_tmp_1 as S
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC '' as PurchInfoRecord,
# MAGIC '' as PurchasingOrganization,
# MAGIC '' as PurchDocNumber,
# MAGIC '' as ItemNoOfPurchDoc,
# MAGIC '' as PurchInfRecCategory,
# MAGIC V.Plant as Plant,
# MAGIC cast(V.QUOTE_DATE as date) as PurchOrdDate,
# MAGIC cast(V.CHANGE_DATE as timestamp) as ChangeDate,
# MAGIC V.PriceValidUntil as PriceValidUntil,
# MAGIC V.NEW_DEFAULT_UNIT_PRICE as PurchDocNetPrice,
# MAGIC 1 as UnitPrice,
# MAGIC V.NEW_PURCHASE_UM as OrderPriceUnit,
# MAGIC V.DEF_UNIT_PRICE_CURR as CurrKey,
# MAGIC '' as PriceUnit,
# MAGIC V.VENDOR_PART_ID as VendorPartId,
# MAGIC V.PART_ID as MaterialNumber,
# MAGIC V.Vendor_ID as VendorAccountNo,
# MAGIC V.DataSource as DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from 
# MAGIC vendor_quote_hist_tmp_2 as V

# COMMAND ----------

df_purchase_info_record_Hist = spark.sql("select * from purchase_info_record_Hist")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_purchase_info_record_Hist.write.format(write_format).mode("overwrite").partitionBy("DataSource").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_purchase_info_record_Hist.createOrReplaceTempView('tmp_purchase_info_record_hist')

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into fedw.purchase_info_record_hist as T
# MAGIC Using purchase_info_record_hist as S
# MAGIC on T.PurchInfoRecord = S.PurchInfoRecord 
# MAGIC and T.PurchasingOrganization = S.PurchasingOrganization 
# MAGIC and T.PurchDocNumber = S.PurchDocNumber
# MAGIC and T.ItemNoOfPurchDoc = S.ItemNoOfPurchDoc
# MAGIC and 'SAP' = S.DataSource
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`PurchInfoRecord` =  S.`PurchInfoRecord`,
# MAGIC T.`PurchasingOrganization` =  S.`PurchasingOrganization`,
# MAGIC T.`PurchDocNumber` =  S.`PurchDocNumber`,
# MAGIC T.`ItemNoOfPurchDoc` =  S.`ItemNoOfPurchDoc`,
# MAGIC T.`PurchInfRecCategory` =  S.`PurchInfRecCategory`,
# MAGIC T.`Plant` =  S.`Plant`,
# MAGIC T.`PurchOrdDate` =  S.`PurchOrdDate`,
# MAGIC T.`ChangeDate` =  S.`ChangeDate`,
# MAGIC T.`PriceValidUntil` =  S.`PriceValidUntil`,
# MAGIC T.`PurchDocNetPrice` =  S.`PurchDocNetPrice`,
# MAGIC T.`UnitPrice` =  S.`UnitPrice`,
# MAGIC T.`OrderPriceUnit` =  S.`OrderPriceUnit`,
# MAGIC T.`CurrKey` =  S.`CurrKey`,
# MAGIC T.`PriceUnit` =  S.`PriceUnit`,
# MAGIC T.`VendorPartId` =  S.`VendorPartId`,
# MAGIC T.`MaterialNumber` =  S.`MaterialNumber`,
# MAGIC T.`VendorAccountNo` =  S.`VendorAccountNo`,
# MAGIC T.`DataSource` =  S.`DataSource`,
# MAGIC T.`UpdatedOn` =  S.`UpdatedOn`
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT
# MAGIC (
# MAGIC `PurchInfoRecord`,
# MAGIC `PurchasingOrganization`,
# MAGIC `PurchDocNumber`,
# MAGIC `ItemNoOfPurchDoc`,
# MAGIC `PurchInfRecCategory`,
# MAGIC `Plant`,
# MAGIC `PurchOrdDate`,
# MAGIC `ChangeDate`,
# MAGIC `PriceValidUntil`,
# MAGIC `PurchDocNetPrice`,
# MAGIC `UnitPrice`,
# MAGIC `OrderPriceUnit`,
# MAGIC `CurrKey`,
# MAGIC `PriceUnit`,
# MAGIC `VendorPartId`,
# MAGIC `MaterialNumber`,
# MAGIC `VendorAccountNo`,
# MAGIC `DataSource`,
# MAGIC `UpdatedOn`
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`PurchInfoRecord`,
# MAGIC S.`PurchasingOrganization`,
# MAGIC S.`PurchDocNumber`,
# MAGIC S.`ItemNoOfPurchDoc`,
# MAGIC S.`PurchInfRecCategory`,
# MAGIC S.`Plant`,
# MAGIC S.`PurchOrdDate`,
# MAGIC S.`ChangeDate`,
# MAGIC S.`PriceValidUntil`,
# MAGIC S.`PurchDocNetPrice`,
# MAGIC S.`UnitPrice`,
# MAGIC S.`OrderPriceUnit`,
# MAGIC S.`CurrKey`,
# MAGIC S.`PriceUnit`,
# MAGIC S.`VendorPartId`,
# MAGIC S.`MaterialNumber`,
# MAGIC S.`VendorAccountNo`,
# MAGIC S.`DataSource`,
# MAGIC now()
# MAGIC )

# COMMAND ----------

if(ts_eina != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EINA' and TargetLayerTableName = 'Purchase_Info_Record_Hist' and DatabaseName = 'S42'".format(ts_eina))
    
if(ts_eipa != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EIPA' and DatabaseName = 'S42'".format(ts_eipa))
    
if(ts_ve70_vendor_quote_hist != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_VENDOR_QUOTE_HISTORY' and DatabaseName = 'VE70'".format(ts_ve70_vendor_quote_hist))
    
if(ts_ve72_vendor_quote_hist != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_VENDOR_QUOTE_HISTORY' and DatabaseName = 'VE72'".format(ts_ve72_vendor_quote_hist))


# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Purchase_Info_Record_Hist where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchase_Info_Record_Hist' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "Purchase_Info_Record_Hist") \
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
# MAGIC Utils.runQuery(options, """call sp_load_purchase_info_record_hist()""")

# COMMAND ----------

if(ts_purchase_info_record != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchase_Info_Record_Hist' and DatabaseName = 'FEDW'".format(ts_purchase_info_record))

# COMMAND ----------


