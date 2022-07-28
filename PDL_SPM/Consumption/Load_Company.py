# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Company'
table_name = 'T001' 
read_format = 'delta'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
stage_table = 'stg_'+table_name
database_name = 'FEDW'

# COMMAND ----------

col_names = ['BUKRS',
'BUTXT',
'ORT01',
'LAND1',
'WAERS',
'SPRAS',
'KTOPL',
'WAABW',
'PERIV',
'KOKFI',
'KKBER',
'XGJRV',
'XVVWA',
'UMKRS',
'FSTVA',
'OPVAR',
'WFVAR',
'MWSKV',
'MWSKA',
'IMPDA',
'XKKBI',
'FSTVARE',
'OFFSACCT',
'RCOMP',
'ADRNR',
'STCEG',
'FIKRS',
'TXJCD',
'INFMT',
'XCOS',
'UpdatedOn'            
]

# COMMAND ----------

df = spark.sql("select * from S42.T001 where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T001')")

# COMMAND ----------

df_ts = df.agg({"UpdatedOn": "max"}).collect()[0]
ts = df_ts["max(UpdatedOn)"]
print(ts)

# COMMAND ----------

df_select_col = df.select(col_names)

# COMMAND ----------

df_rename = df_select_col.withColumnRenamed('TimeStamp','LandingTimeStamp') \
		.withColumnRenamed('BUKRS','CompanyCode') \
		.withColumnRenamed('BUTXT','CompanyName') \
		.withColumnRenamed('ORT01','City') \
		.withColumnRenamed('LAND1','CountryKey') \
		.withColumnRenamed('WAERS','CurrencyKey') \
		.withColumnRenamed('SPRAS','LanguageKey') \
		.withColumnRenamed('KTOPL','ChartOfAccounts') \
		.withColumnRenamed('WAABW','MaxExchRateDeviationPer') \
		.withColumnRenamed('PERIV','FiscalYearVariant') \
		.withColumnRenamed('KOKFI','AllocationIndicator') \
		.withColumnRenamed('KKBER','CreditControlArea') \
		.withColumnRenamed('XGJRV','IndProposeFiscalYear') \
		.withColumnRenamed('XVVWA','IndFinAssetsMgmtActive') \
		.withColumnRenamed('UMKRS','SalesOrPurchTaxGroup') \
		.withColumnRenamed('FSTVA','FieldStatusVariant') \
		.withColumnRenamed('OPVAR','PostingPeriodVariant') \
		.withColumnRenamed('WFVAR','WorkflowVariant') \
		.withColumnRenamed('MWSKV','InTaxCodeNonTaxTrans') \
		.withColumnRenamed('MWSKA','OutTaxCodeNonTaxTrans') \
		.withColumnRenamed('IMPDA','ForeignTradeImpDataContr') \
		.withColumnRenamed('XKKBI','IndCredContrAreaOverwrit') \
		.withColumnRenamed('FSTVARE','FundsResFieldStatusVar') \
		.withColumnRenamed('OFFSACCT','MethodOffsetACDeter') \
		.withColumnRenamed('RCOMP','Company') \
		.withColumnRenamed('ADRNR','Address') \
		.withColumnRenamed('STCEG','VATRegistrationNumber') \
		.withColumnRenamed('FIKRS','FinancialMamtArea') \
		.withColumnRenamed('TXJCD','TaxJurisdictionCode') \
		.withColumnRenamed('INFMT','InflationMethod') \
		.withColumnRenamed('XCOS','CostOfSalesACStatus') \
        .withColumn('DataSource',lit('SAP'))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_rename.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_rename.createOrReplaceTempView('stg_company')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.Company as T 
# MAGIC USING stg_company as S 
# MAGIC ON T.CompanyCode = S.CompanyCode
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.CompanyCode =  S.CompanyCode,
# MAGIC T.CompanyName =  S.CompanyName,
# MAGIC T.City =  S.City,
# MAGIC T.CountryKey =  S.CountryKey,
# MAGIC T.CurrencyKey =  S.CurrencyKey,
# MAGIC T.LanguageKey =  S.LanguageKey,
# MAGIC T.ChartOfAccounts =  S.ChartOfAccounts,
# MAGIC T.MaxExchRateDeviationPer =  S.MaxExchRateDeviationPer,
# MAGIC T.FiscalYearVariant =  S.FiscalYearVariant,
# MAGIC T.AllocationIndicator =  S.AllocationIndicator,
# MAGIC T.CreditControlArea =  S.CreditControlArea,
# MAGIC T.IndProposeFiscalYear =  S.IndProposeFiscalYear,
# MAGIC T.IndFinAssetsMgmtActive =  S.IndFinAssetsMgmtActive,
# MAGIC T.SalesOrPurchTaxGroup =  S.SalesOrPurchTaxGroup,
# MAGIC T.FieldStatusVariant =  S.FieldStatusVariant,
# MAGIC T.PostingPeriodVariant =  S.PostingPeriodVariant,
# MAGIC T.WorkflowVariant =  S.WorkflowVariant,
# MAGIC T.InTaxCodeNonTaxTrans =  S.InTaxCodeNonTaxTrans,
# MAGIC T.OutTaxCodeNonTaxTrans =  S.OutTaxCodeNonTaxTrans,
# MAGIC T.ForeignTradeImpDataContr =  S.ForeignTradeImpDataContr,
# MAGIC T.IndCredContrAreaOverwrit =  S.IndCredContrAreaOverwrit,
# MAGIC T.FundsResFieldStatusVar =  S.FundsResFieldStatusVar,
# MAGIC T.MethodOffsetACDeter =  S.MethodOffsetACDeter,
# MAGIC T.Company =  S.Company,
# MAGIC T.Address =  S.Address,
# MAGIC T.VATRegistrationNumber =  S.VATRegistrationNumber,
# MAGIC T.FinancialMamtArea =  S.FinancialMamtArea,
# MAGIC T.TaxJurisdictionCode =  S.TaxJurisdictionCode,
# MAGIC T.InflationMethod =  S.InflationMethod,
# MAGIC T.CostOfSalesACStatus =  S.CostOfSalesACStatus,
# MAGIC T.UpdatedOn = now(),
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     CompanyCode,
# MAGIC CompanyName,
# MAGIC City,
# MAGIC CountryKey,
# MAGIC CurrencyKey,
# MAGIC LanguageKey,
# MAGIC ChartOfAccounts,
# MAGIC MaxExchRateDeviationPer,
# MAGIC FiscalYearVariant,
# MAGIC AllocationIndicator,
# MAGIC CreditControlArea,
# MAGIC IndProposeFiscalYear,
# MAGIC IndFinAssetsMgmtActive,
# MAGIC SalesOrPurchTaxGroup,
# MAGIC FieldStatusVariant,
# MAGIC PostingPeriodVariant,
# MAGIC WorkflowVariant,
# MAGIC InTaxCodeNonTaxTrans,
# MAGIC OutTaxCodeNonTaxTrans,
# MAGIC ForeignTradeImpDataContr,
# MAGIC IndCredContrAreaOverwrit,
# MAGIC FundsResFieldStatusVar,
# MAGIC MethodOffsetACDeter,
# MAGIC Company,
# MAGIC Address,
# MAGIC VATRegistrationNumber,
# MAGIC FinancialMamtArea,
# MAGIC TaxJurisdictionCode,
# MAGIC InflationMethod,
# MAGIC CostOfSalesACStatus,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES (
# MAGIC S.CompanyCode,
# MAGIC S.CompanyName,
# MAGIC S.City,
# MAGIC S.CountryKey,
# MAGIC S.CurrencyKey,
# MAGIC S.LanguageKey,
# MAGIC S.ChartOfAccounts,
# MAGIC S.MaxExchRateDeviationPer,
# MAGIC S.FiscalYearVariant,
# MAGIC S.AllocationIndicator,
# MAGIC S.CreditControlArea,
# MAGIC S.IndProposeFiscalYear,
# MAGIC S.IndFinAssetsMgmtActive,
# MAGIC S.SalesOrPurchTaxGroup,
# MAGIC S.FieldStatusVariant,
# MAGIC S.PostingPeriodVariant,
# MAGIC S.WorkflowVariant,
# MAGIC S.InTaxCodeNonTaxTrans,
# MAGIC S.OutTaxCodeNonTaxTrans,
# MAGIC S.ForeignTradeImpDataContr,
# MAGIC S.IndCredContrAreaOverwrit,
# MAGIC S.FundsResFieldStatusVar,
# MAGIC S.MethodOffsetACDeter,
# MAGIC S.Company,
# MAGIC S.Address,
# MAGIC S.VATRegistrationNumber,
# MAGIC S.FinancialMamtArea,
# MAGIC S.TaxJurisdictionCode,
# MAGIC S.InflationMethod,
# MAGIC S.CostOfSalesACStatus,
# MAGIC S.UpdatedOn,
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T001'".format(ts))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Company where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Company')")

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
  .option("dbtable", "Company") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_COMPANY()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Company'".format(ts))

# COMMAND ----------


