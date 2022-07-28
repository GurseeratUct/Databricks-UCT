# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Profit_Center'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_cepc = ["MANDT",
"PRCTR",
"DATBI",
"KOKRS",
"DATAB",
"ERSDA",
"USNAM",
"MERKMAL",
"ABTEI",
"VERAK",
"VERAK_USER",
"WAERS",
"NPRCTR",
"LAND1",
"ANRED",
"NAME1",
"NAME2",
"NAME3",
"NAME4",
"ORT01",
"ORT02",
"STRAS",
"PFACH",
"PSTLZ",
"PSTL2",
"SPRAS",
"TELBX",
"TELF1",
"TELF2",
"TELFX",
"TELTX",
"TELX1",
"DATLT",
"DRNAM",
"KHINR",
"BUKRS",
"VNAME",
"RECID",
"ETYPE",
"TXJCD",
"REGIO",
"KVEWE",
"KAPPL",
"KALSM",
"LOGSYSTEM",
"LOCK_IND",
"PCA_TEMPLATE",
"SEGMENT",
"EEW_CEPC_PS_DUMMY"]

# COMMAND ----------

df = spark.sql("select * from S42.cepc where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'CEPC' and DatabaseName = 'S42')")
df_cepc = df.select(col_names_cepc)
df_cepc.createOrReplaceTempView("tmp_cepc")
df_ts_cepc = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_cepc = df_ts_cepc["max(UpdatedOn)"]
print(ts_cepc)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_profit_center as 
# MAGIC select 
# MAGIC S.MANDT as Client,
# MAGIC S.PRCTR as ProfitCenter,
# MAGIC S.DATBI as ValidToDate,
# MAGIC S.KOKRS as ControllingArea,
# MAGIC S.DATAB as ValidFromDate,
# MAGIC S.ERSDA as EnteredOn,
# MAGIC S.USNAM as Enteredby,
# MAGIC S.MERKMAL as FieldnameofCOPAcharacteristic,
# MAGIC S.ABTEI as Department,
# MAGIC S.VERAK as PersonResponsibleforProfitCenter,
# MAGIC S.VERAK_USER as UserResponsiblefortheProfitCenter,
# MAGIC S.WAERS as CurrencyKey,
# MAGIC S.NPRCTR as Successorprofitcenter,
# MAGIC S.LAND1 as CountryKey,
# MAGIC S.ANRED as Title,
# MAGIC S.NAME1 as Name1,
# MAGIC S.NAME2 as Name2,
# MAGIC S.NAME3 as Name3,
# MAGIC S.NAME4 as Name4,
# MAGIC S.ORT01 as City,
# MAGIC S.ORT02 as District,
# MAGIC S.STRAS as StreetandHouseNumber,
# MAGIC S.PFACH as POBox,
# MAGIC S.PSTLZ as PostalCode,
# MAGIC S.PSTL2 as POBoxPostalCode,
# MAGIC S.SPRAS as LanguageKey,
# MAGIC S.TELBX as Teleboxnumber,
# MAGIC S.TELF1 as Firsttelephonenumber,
# MAGIC S.TELF2 as Secondtelephonenumber,
# MAGIC S.TELFX as FaxNumber,
# MAGIC S.TELTX as Teletexnumber,
# MAGIC S.TELX1 as Telexnumber,
# MAGIC S.DATLT as Datacommunicationlineno,
# MAGIC S.DRNAM as Printernameforprofitcenter,
# MAGIC S.KHINR as Profitcenterarea,
# MAGIC S.BUKRS as CompanyCode,
# MAGIC S.VNAME as Jointventure,
# MAGIC S.RECID as RecoveryIndicator,
# MAGIC S.ETYPE as Equitytype,
# MAGIC S.TXJCD as TaxJurisdiction,
# MAGIC S.REGIO as Region,
# MAGIC S.KVEWE as Usageoftheconditiontable,
# MAGIC S.KAPPL as Application,
# MAGIC S.KALSM as Procedure,
# MAGIC S.LOGSYSTEM as LogicalSystem,
# MAGIC S.LOCK_IND as Lockindicator,
# MAGIC S.PCA_TEMPLATE as TemplateforFormulaPlanninginProfitCenters,
# MAGIC S.SEGMENT as SegmentforSegmentalReporting,
# MAGIC S.EEW_CEPC_PS_DUMMY as Dummyfunctioninlength1,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_cepc as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_profit_center")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_profit_center')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Profit_Center as T 
# MAGIC USING stg_merge_profit_center as S 
# MAGIC ON T.ProfitCenter = S.ProfitCenter
# MAGIC and T.ValidToDate = S.ValidToDate
# MAGIC and T.ControllingArea = S.ControllingArea
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.`Client` =  S.`Client`,
# MAGIC T.`ProfitCenter` =  S.`ProfitCenter`,
# MAGIC T.`ValidToDate` =  S.`ValidToDate`,
# MAGIC T.`ControllingArea` =  S.`ControllingArea`,
# MAGIC T.`ValidFromDate` =  S.`ValidFromDate`,
# MAGIC T.`EnteredOn` =  S.`EnteredOn`,
# MAGIC T.`Enteredby` =  S.`Enteredby`,
# MAGIC T.`FieldnameofCOPAcharacteristic` =  S.`FieldnameofCOPAcharacteristic`,
# MAGIC T.`Department` =  S.`Department`,
# MAGIC T.`PersonResponsibleforProfitCenter` =  S.`PersonResponsibleforProfitCenter`,
# MAGIC T.`UserResponsiblefortheProfitCenter` =  S.`UserResponsiblefortheProfitCenter`,
# MAGIC T.`CurrencyKey` =  S.`CurrencyKey`,
# MAGIC T.`Successorprofitcenter` =  S.`Successorprofitcenter`,
# MAGIC T.`CountryKey` =  S.`CountryKey`,
# MAGIC T.`Title` =  S.`Title`,
# MAGIC T.`Name1` =  S.`Name1`,
# MAGIC T.`Name2` =  S.`Name2`,
# MAGIC T.`Name3` =  S.`Name3`,
# MAGIC T.`Name4` =  S.`Name4`,
# MAGIC T.`City` =  S.`City`,
# MAGIC T.`District` =  S.`District`,
# MAGIC T.`StreetandHouseNumber` =  S.`StreetandHouseNumber`,
# MAGIC T.`POBox` =  S.`POBox`,
# MAGIC T.`PostalCode` =  S.`PostalCode`,
# MAGIC T.`POBoxPostalCode` =  S.`POBoxPostalCode`,
# MAGIC T.`LanguageKey` =  S.`LanguageKey`,
# MAGIC T.`Teleboxnumber` =  S.`Teleboxnumber`,
# MAGIC T.`Firsttelephonenumber` =  S.`Firsttelephonenumber`,
# MAGIC T.`Secondtelephonenumber` =  S.`Secondtelephonenumber`,
# MAGIC T.`FaxNumber` =  S.`FaxNumber`,
# MAGIC T.`Teletexnumber` =  S.`Teletexnumber`,
# MAGIC T.`Telexnumber` =  S.`Telexnumber`,
# MAGIC T.`Datacommunicationlineno` =  S.`Datacommunicationlineno`,
# MAGIC T.`Printernameforprofitcenter` =  S.`Printernameforprofitcenter`,
# MAGIC T.`Profitcenterarea` =  S.`Profitcenterarea`,
# MAGIC T.`CompanyCode` =  S.`CompanyCode`,
# MAGIC T.`Jointventure` =  S.`Jointventure`,
# MAGIC T.`RecoveryIndicator` =  S.`RecoveryIndicator`,
# MAGIC T.`Equitytype` =  S.`Equitytype`,
# MAGIC T.`TaxJurisdiction` =  S.`TaxJurisdiction`,
# MAGIC T.`Region` =  S.`Region`,
# MAGIC T.`Usageoftheconditiontable` =  S.`Usageoftheconditiontable`,
# MAGIC T.`Application` =  S.`Application`,
# MAGIC T.`Procedure` =  S.`Procedure`,
# MAGIC T.`LogicalSystem` =  S.`LogicalSystem`,
# MAGIC T.`Lockindicator` =  S.`Lockindicator`,
# MAGIC T.`TemplateforFormulaPlanninginProfitCenters` =  S.`TemplateforFormulaPlanninginProfitCenters`,
# MAGIC T.`SegmentforSegmentalReporting` =  S.`SegmentforSegmentalReporting`,
# MAGIC T.`Dummyfunctioninlength1` =  S.`Dummyfunctioninlength1`,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC 
# MAGIC `Client`,
# MAGIC `ProfitCenter`,
# MAGIC `ValidToDate`,
# MAGIC `ControllingArea`,
# MAGIC `ValidFromDate`,
# MAGIC `EnteredOn`,
# MAGIC `Enteredby`,
# MAGIC `FieldnameofCOPAcharacteristic`,
# MAGIC `Department`,
# MAGIC `PersonResponsibleforProfitCenter`,
# MAGIC `UserResponsiblefortheProfitCenter`,
# MAGIC `CurrencyKey`,
# MAGIC `Successorprofitcenter`,
# MAGIC `CountryKey`,
# MAGIC `Title`,
# MAGIC `Name1`,
# MAGIC `Name2`,
# MAGIC `Name3`,
# MAGIC `Name4`,
# MAGIC `City`,
# MAGIC `District`,
# MAGIC `StreetandHouseNumber`,
# MAGIC `POBox`,
# MAGIC `PostalCode`,
# MAGIC `POBoxPostalCode`,
# MAGIC `LanguageKey`,
# MAGIC `Teleboxnumber`,
# MAGIC `Firsttelephonenumber`,
# MAGIC `Secondtelephonenumber`,
# MAGIC `FaxNumber`,
# MAGIC `Teletexnumber`,
# MAGIC `Telexnumber`,
# MAGIC `Datacommunicationlineno`,
# MAGIC `Printernameforprofitcenter`,
# MAGIC `Profitcenterarea`,
# MAGIC `CompanyCode`,
# MAGIC `Jointventure`,
# MAGIC `RecoveryIndicator`,
# MAGIC `Equitytype`,
# MAGIC `TaxJurisdiction`,
# MAGIC `Region`,
# MAGIC `Usageoftheconditiontable`,
# MAGIC `Application`,
# MAGIC `Procedure`,
# MAGIC `LogicalSystem`,
# MAGIC `Lockindicator`,
# MAGIC `TemplateforFormulaPlanninginProfitCenters`,
# MAGIC `SegmentforSegmentalReporting`,
# MAGIC `Dummyfunctioninlength1`,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`Client`,
# MAGIC S.`ProfitCenter`,
# MAGIC S.`ValidToDate`,
# MAGIC S.`ControllingArea`,
# MAGIC S.`ValidFromDate`,
# MAGIC S.`EnteredOn`,
# MAGIC S.`Enteredby`,
# MAGIC S.`FieldnameofCOPAcharacteristic`,
# MAGIC S.`Department`,
# MAGIC S.`PersonResponsibleforProfitCenter`,
# MAGIC S.`UserResponsiblefortheProfitCenter`,
# MAGIC S.`CurrencyKey`,
# MAGIC S.`Successorprofitcenter`,
# MAGIC S.`CountryKey`,
# MAGIC S.`Title`,
# MAGIC S.`Name1`,
# MAGIC S.`Name2`,
# MAGIC S.`Name3`,
# MAGIC S.`Name4`,
# MAGIC S.`City`,
# MAGIC S.`District`,
# MAGIC S.`StreetandHouseNumber`,
# MAGIC S.`POBox`,
# MAGIC S.`PostalCode`,
# MAGIC S.`POBoxPostalCode`,
# MAGIC S.`LanguageKey`,
# MAGIC S.`Teleboxnumber`,
# MAGIC S.`Firsttelephonenumber`,
# MAGIC S.`Secondtelephonenumber`,
# MAGIC S.`FaxNumber`,
# MAGIC S.`Teletexnumber`,
# MAGIC S.`Telexnumber`,
# MAGIC S.`Datacommunicationlineno`,
# MAGIC S.`Printernameforprofitcenter`,
# MAGIC S.`Profitcenterarea`,
# MAGIC S.`CompanyCode`,
# MAGIC S.`Jointventure`,
# MAGIC S.`RecoveryIndicator`,
# MAGIC S.`Equitytype`,
# MAGIC S.`TaxJurisdiction`,
# MAGIC S.`Region`,
# MAGIC S.`Usageoftheconditiontable`,
# MAGIC S.`Application`,
# MAGIC S.`Procedure`,
# MAGIC S.`LogicalSystem`,
# MAGIC S.`Lockindicator`,
# MAGIC S.`TemplateforFormulaPlanninginProfitCenters`,
# MAGIC S.`SegmentforSegmentalReporting`,
# MAGIC S.`Dummyfunctioninlength1`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

if(ts_cepc != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'CEPC' and DatabaseName = 'S42'".format(ts_cepc))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.PROFIT_CENTER where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Profit_Center' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "Profit_Center") \
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
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC  
# MAGIC Utils.runQuery(options, """call SP_LOAD_PROFIT_CENTER()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Profit_Center' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


