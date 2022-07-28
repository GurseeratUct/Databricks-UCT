# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Supplier'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_lfa1 = ["LIFNR",
"LAND1",
"NAME1",
"ORT01",
"PSTLZ",
"REGIO",
"SORTL",
"STRAS",
"ADRNR",
"BBBNR",
"BBSNR",
"ERDAT",
"ERNAM",
"KTOKK",
"SPRAS",
"STCEG",
"TAXBS",
"UPTIM",
"RIC",
"LEGALNAT",
"J_SC_CAPITAL",
"CODCAE",
"STAGING_TIME",
"UpdatedOn"
]

# COMMAND ----------

col_names_vendor = ["ID",
"COUNTRY_ID",
"NAME",
"UpdatedOn"
]

# COMMAND ----------

df = spark.sql("select * from S42.LFA1 where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'LFA1' and DatabaseName = 'S42')")
df_lfa1 = df.select(col_names_lfa1)
df_lfa1.createOrReplaceTempView("tmp_lfa1")
df_ts_lfa1 = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_lfa1 = df_ts_lfa1["max(UpdatedOn)"]
print(ts_lfa1)

# COMMAND ----------

df = spark.sql("select * from VE70.VENDOR where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'VENDOR' and DatabaseName = 'VE70')")
df_vendor_ve70 = df.select(col_names_vendor)
df_vendor_ve70.createOrReplaceTempView("vendor_ve70")
df_ts_vendor_ve70 = df_vendor_ve70.agg({"UpdatedOn": "max"}).collect()[0]
ts_vendor_ve70 = df_ts_vendor_ve70["max(UpdatedOn)"]
print(ts_vendor_ve70)

# COMMAND ----------

df = spark.sql("select * from VE72.VENDOR where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'VENDOR' and DatabaseName = 'VE72')")
df_vendor_ve72 = df.select(col_names_vendor)
df_vendor_ve72.createOrReplaceTempView("vendor_ve72")
df_ts_vendor_ve72 = df_vendor_ve72.agg({"UpdatedOn": "max"}).collect()[0]
ts_vendor_ve72 = df_ts_vendor_ve72["max(UpdatedOn)"]
print(ts_vendor_ve72)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vendor_tmp as 
# MAGIC select * from(select ID,COUNTRY_ID,NAME,'VE70' as DataSource from vendor_ve70 
# MAGIC union
# MAGIC select ID,COUNTRY_ID,NAME,'VE72' as DataSource from vendor_ve72
# MAGIC )A where ID NOT IN (select replace(ltrim(replace(LIFNR,'0',' ')),' ','0') from tmp_lfa1) 

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_supplier as 
# MAGIC select 
# MAGIC case when M.`LIFNR` IS NULL then V.ID else M.`LIFNR` end as SupplierAccountNumber,
# MAGIC case when M.`LIFNR` IS NULL then V.COUNTRY_ID else M.`LAND1` end as CountryKey,
# MAGIC case when M.`LIFNR` IS NULL then V.NAME else M.`NAME1` end as Name,
# MAGIC M.`ORT01` as City,
# MAGIC M.`PSTLZ` as PostalCode,
# MAGIC M.`REGIO` as Region,
# MAGIC M.`SORTL` as SortField,
# MAGIC M.`STRAS` as StreetAndHouseNumber,
# MAGIC M.`ADRNR` as Address,
# MAGIC M.`BBBNR` as InternationalLocationNumber1,
# MAGIC M.`BBSNR` as InternationalLocationNumber2,
# MAGIC M.`ERDAT` as RecordCreationDate,
# MAGIC M.`ERNAM` as Person,
# MAGIC M.`KTOKK` as SupplierAccountGroup,
# MAGIC M.`SPRAS` as LanguageKey,
# MAGIC M.`STCEG` as VATRegistrationNumber,
# MAGIC M.`TAXBS` as TaxBaseInPercentage,
# MAGIC M.`UPTIM` as LastChangeConfirmationTime,
# MAGIC M.`RIC` as RICNumber,
# MAGIC M.`LEGALNAT` as LegalNature,
# MAGIC M.`J_SC_CAPITAL` as CapitalAmount,
# MAGIC M.`CODCAE` as CAECode,
# MAGIC M.`STAGING_TIME` as StagingTimeInDays,
# MAGIC now() as UpdatedOn,
# MAGIC case when M.`LIFNR` IS NULL then V.DataSource else 'SAP' end as DataSource
# MAGIC from tmp_lfa1 as M
# MAGIC full join vendor_tmp as V
# MAGIC on V.ID =  replace(ltrim(replace(M.LIFNR,'0',' ')),' ','0')

# COMMAND ----------

df_merge = spark.sql("""
select SupplierAccountNumber,CountryKey,
Name,
City,
PostalCode,
Region,
SortField,
StreetAndHouseNumber,
Address,
InternationalLocationNumber1,
InternationalLocationNumber2,
RecordCreationDate,
Person,
SupplierAccountGroup,
LanguageKey,
VATRegistrationNumber,
TaxBaseInPercentage,
LastChangeConfirmationTime,
RICNumber,
LegalNature,
CapitalAmount,
CAECode,
StagingTimeInDays,
UpdatedOn,
DataSource
 from (select ROW_NUMBER() OVER (PARTITION BY SupplierAccountNumber ORDER BY DataSource ASC) as rn,* from merge_supplier)A where A.rn = 1""")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_merge.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_supplier')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Supplier as T 
# MAGIC USING stg_merge_supplier as S 
# MAGIC ON T.SupplierAccountNumber = S.SupplierAccountNumber
# MAGIC and T.DataSource = S.DataSource
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.SupplierAccountNumber = S.SupplierAccountNumber,
# MAGIC T.CountryKey = S.CountryKey,
# MAGIC T.Name = S.Name,
# MAGIC T.City = S.City,
# MAGIC T.PostalCode = S.PostalCode,
# MAGIC T.Region = S.Region,
# MAGIC T.SortField = S.SortField,
# MAGIC T.StreetAndHouseNumber = S.StreetAndHouseNumber,
# MAGIC T.Address = S.Address,
# MAGIC T.InternationalLocationNumber1 = S.InternationalLocationNumber1,
# MAGIC T.InternationalLocationNumber2 = S.InternationalLocationNumber2,
# MAGIC T.RecordCreationDate = S.RecordCreationDate,
# MAGIC T.Person = S.Person,
# MAGIC T.SupplierAccountGroup = S.SupplierAccountGroup,
# MAGIC T.LanguageKey = S.LanguageKey,
# MAGIC T.VATRegistrationNumber = S.VATRegistrationNumber,
# MAGIC T.TaxBaseInPercentage = S.TaxBaseInPercentage,
# MAGIC T.LastChangeConfirmationTime = S.LastChangeConfirmationTime,
# MAGIC T.RICNumber = S.RICNumber,
# MAGIC T.LegalNature = S.LegalNature,
# MAGIC T.CapitalAmount = S.CapitalAmount,
# MAGIC T.CAECode = S.CAECode,
# MAGIC T.StagingTimeInDays = S.StagingTimeInDays,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC SupplierAccountNumber,
# MAGIC CountryKey,
# MAGIC Name,
# MAGIC City,
# MAGIC PostalCode,
# MAGIC Region,
# MAGIC SortField,
# MAGIC StreetAndHouseNumber,
# MAGIC Address,
# MAGIC InternationalLocationNumber1,
# MAGIC InternationalLocationNumber2,
# MAGIC RecordCreationDate,
# MAGIC Person,
# MAGIC SupplierAccountGroup,
# MAGIC LanguageKey,
# MAGIC VATRegistrationNumber,
# MAGIC TaxBaseInPercentage,
# MAGIC LastChangeConfirmationTime,
# MAGIC RICNumber,
# MAGIC LegalNature,
# MAGIC CapitalAmount,
# MAGIC CAECode,
# MAGIC StagingTimeInDays,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.SupplierAccountNumber,
# MAGIC S.CountryKey,
# MAGIC S.Name,
# MAGIC S.City,
# MAGIC S.PostalCode,
# MAGIC S.Region,
# MAGIC S.SortField,
# MAGIC S.StreetAndHouseNumber,
# MAGIC S.Address,
# MAGIC S.InternationalLocationNumber1,
# MAGIC S.InternationalLocationNumber2,
# MAGIC S.RecordCreationDate,
# MAGIC S.Person,
# MAGIC S.SupplierAccountGroup,
# MAGIC S.LanguageKey,
# MAGIC S.VATRegistrationNumber,
# MAGIC S.TaxBaseInPercentage,
# MAGIC S.LastChangeConfirmationTime,
# MAGIC S.RICNumber,
# MAGIC S.LegalNature,
# MAGIC S.CapitalAmount,
# MAGIC S.CAECode,
# MAGIC S.StagingTimeInDays,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_lfa1 != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'LFA1' and DatabaseName = 'S42'".format(ts_lfa1))

if(ts_vendor_ve70 != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'VENDOR' and DatabaseName = 'VE70'".format(ts_vendor_ve70))
    
if(ts_vendor_ve72 != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'VENDOR' and DatabaseName = 'VE72'".format(ts_vendor_ve72))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Supplier where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Supplier' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_supplier = ts_sf["max(UpdatedOn)"]
print(ts_supplier)

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
  .option("dbtable", "Supplier") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_SUPPLIER()""")

# COMMAND ----------

if(ts_supplier != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Supplier' and DatabaseName = 'FEDW'".format(ts_supplier))

# COMMAND ----------


