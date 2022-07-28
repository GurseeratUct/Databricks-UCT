# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'GL_Account_Master'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_ska1 = [
"MANDT",
"KTOPL",
"SAKNR",
"XBILK",
"SAKAN",
"BILKT",
"ERDAT",
"ERNAM",
"GVTYP",
"KTOKS",
"MUSTR",
"VBUND",
"XLOEV",
"XSPEA",
"XSPEB",
"XSPEP",
"MCOD1",
"FUNC_AREA",
"GLACCOUNT_TYPE",
"GLACCOUNT_SUBTYPE",
"MAIN_SAKNR",
"LAST_CHANGED_TS"
]

# COMMAND ----------

df = spark.sql("select * from S42.ska1 where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'SKA1' and DatabaseName = 'S42')")
df_ska1 = df.select(col_names_ska1)
df_ska1.createOrReplaceTempView("tmp_ska1")
df_ts_ska1 = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ska1 = df_ts_ska1["max(UpdatedOn)"]
print(ts_ska1)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_gl_account_master as 
# MAGIC select
# MAGIC S.MANDT as Client,
# MAGIC S.KTOPL as ChartofAccounts,
# MAGIC S.SAKNR as GLAccountNumber,
# MAGIC S.XBILK as Accountisabalancesheetaccount,
# MAGIC S.SAKAN as GLAccountNumber1,
# MAGIC S.BILKT as GroupAccountNumber,
# MAGIC S.ERDAT as DateonwhichtheRecordWasCreated,
# MAGIC S.ERNAM as NameofPersonCreatedtheObject,
# MAGIC S.GVTYP as PLstatementaccounttype,
# MAGIC S.KTOKS as GLAccountGroup,
# MAGIC S.MUSTR as NumberoftheSampleAccount,
# MAGIC S.VBUND as CompanyIDofTradingPartner,
# MAGIC S.XLOEV as AccountMarkedforDeletion,
# MAGIC S.XSPEA as AccountIsBlockedforCreation,
# MAGIC S.XSPEB as IsAccountBlockedforPosting,
# MAGIC S.XSPEP as AccountBlockedforPlanning,
# MAGIC S.MCOD1 as SearchTermforUsingMatchcode,
# MAGIC S.FUNC_AREA as FunctionalArea,
# MAGIC S.GLACCOUNT_TYPE as TypeofaGeneralLedgerAccount,
# MAGIC S.GLACCOUNT_SUBTYPE as SubtypeofaGLAccount,
# MAGIC S.MAIN_SAKNR as BankReconciliationAccount,
# MAGIC S.LAST_CHANGED_TS as UTCTimeStampinShortForm,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_ska1 as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_gl_account_master")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_gl_account_master')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.GL_Account_Master as T 
# MAGIC USING stg_merge_gl_account_master as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.ChartofAccounts = S.ChartofAccounts
# MAGIC and T.GLAccountNumber = S.GLAccountNumber
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Client =  S.Client,
# MAGIC T.ChartofAccounts =  S.ChartofAccounts,
# MAGIC T.GLAccountNumber =  S.GLAccountNumber,
# MAGIC T.Accountisabalancesheetaccount =  S.Accountisabalancesheetaccount,
# MAGIC T.GLAccountNumber1 =  S.GLAccountNumber1,
# MAGIC T.GroupAccountNumber =  S.GroupAccountNumber,
# MAGIC T.DateonwhichtheRecordWasCreated =  S.DateonwhichtheRecordWasCreated,
# MAGIC T.NameofPersonCreatedtheObject =  S.NameofPersonCreatedtheObject,
# MAGIC T.PLstatementaccounttype =  S.PLstatementaccounttype,
# MAGIC T.GLAccountGroup =  S.GLAccountGroup,
# MAGIC T.NumberoftheSampleAccount =  S.NumberoftheSampleAccount,
# MAGIC T.CompanyIDofTradingPartner =  S.CompanyIDofTradingPartner,
# MAGIC T.AccountMarkedforDeletion =  S.AccountMarkedforDeletion,
# MAGIC T.AccountIsBlockedforCreation =  S.AccountIsBlockedforCreation,
# MAGIC T.IsAccountBlockedforPosting =  S.IsAccountBlockedforPosting,
# MAGIC T.AccountBlockedforPlanning =  S.AccountBlockedforPlanning,
# MAGIC T.SearchTermforUsingMatchcode =  S.SearchTermforUsingMatchcode,
# MAGIC T.FunctionalArea =  S.FunctionalArea,
# MAGIC T.TypeofaGeneralLedgerAccount =  S.TypeofaGeneralLedgerAccount,
# MAGIC T.SubtypeofaGLAccount =  S.SubtypeofaGLAccount,
# MAGIC T.BankReconciliationAccount =  S.BankReconciliationAccount,
# MAGIC T.UTCTimeStampinShortForm =  S.UTCTimeStampinShortForm,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Client,
# MAGIC ChartofAccounts,
# MAGIC GLAccountNumber,
# MAGIC Accountisabalancesheetaccount,
# MAGIC GLAccountNumber1,
# MAGIC GroupAccountNumber,
# MAGIC DateonwhichtheRecordWasCreated,
# MAGIC NameofPersonCreatedtheObject,
# MAGIC PLstatementaccounttype,
# MAGIC GLAccountGroup,
# MAGIC NumberoftheSampleAccount,
# MAGIC CompanyIDofTradingPartner,
# MAGIC AccountMarkedforDeletion,
# MAGIC AccountIsBlockedforCreation,
# MAGIC IsAccountBlockedforPosting,
# MAGIC AccountBlockedforPlanning,
# MAGIC SearchTermforUsingMatchcode,
# MAGIC FunctionalArea,
# MAGIC TypeofaGeneralLedgerAccount,
# MAGIC SubtypeofaGLAccount,
# MAGIC BankReconciliationAccount,
# MAGIC UTCTimeStampinShortForm,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Client,
# MAGIC S.ChartofAccounts,
# MAGIC S.GLAccountNumber,
# MAGIC S.Accountisabalancesheetaccount,
# MAGIC S.GLAccountNumber1,
# MAGIC S.GroupAccountNumber,
# MAGIC S.DateonwhichtheRecordWasCreated,
# MAGIC S.NameofPersonCreatedtheObject,
# MAGIC S.PLstatementaccounttype,
# MAGIC S.GLAccountGroup,
# MAGIC S.NumberoftheSampleAccount,
# MAGIC S.CompanyIDofTradingPartner,
# MAGIC S.AccountMarkedforDeletion,
# MAGIC S.AccountIsBlockedforCreation,
# MAGIC S.IsAccountBlockedforPosting,
# MAGIC S.AccountBlockedforPlanning,
# MAGIC S.SearchTermforUsingMatchcode,
# MAGIC S.FunctionalArea,
# MAGIC S.TypeofaGeneralLedgerAccount,
# MAGIC S.SubtypeofaGLAccount,
# MAGIC S.BankReconciliationAccount,
# MAGIC S.UTCTimeStampinShortForm,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_ska1 != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'SKA1' and DatabaseName = 'S42'".format(ts_ska1))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.GL_Account_Master where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'GL_Account_Master' and DatabaseName = 'FEDW' )")
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
  .option("dbtable", "GL_Account_Master") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_GL_ACCOUNT_MASTER()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'GL_Account_Master' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


