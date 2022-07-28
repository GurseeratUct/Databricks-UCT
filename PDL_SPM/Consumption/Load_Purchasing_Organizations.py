# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Purchasing_Organizations'
table_name = 'T024E' 
read_format = 'delta'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
stage_table = 'stg_'+table_name
database_name = 'FEDW'

# COMMAND ----------

col_names = [  'MANDT',
                'EKORG',
                'EKOTX',
                'BUKRS',
                'TXADR',
                'TXKOP',
                'TXFUS',
                'TXGRU',
                'KALSE',
                'MKALS',
                'BPEFF',
                'BUKRS_NTR',
                'LandingFileTimeStamp',
                'UpdatedOn',
                'DataSource']

# COMMAND ----------

df = spark.sql("select * from S42.T024E where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T024E')")

# COMMAND ----------


df_ts = df.agg({"UpdatedOn": "max"}).collect()[0]
ts = df_ts["max(UpdatedOn)"]
print(ts)

# COMMAND ----------

df_select_col = df.select(col_names)

# COMMAND ----------

df_rename =    df_select_col.withColumnRenamed('MANDT','Client') \
                            .withColumnRenamed('EKORG','PurchasingOrganization') \
                            .withColumnRenamed('EKOTX','PurchasingOrganizationdesc') \
                            .withColumnRenamed('BUKRS','CompanyCode') \
                            .withColumnRenamed('TXADR','SenderLine') \
                            .withColumnRenamed('TXKOP','LetterHeading') \
                            .withColumnRenamed('TXFUS','FooterLines') \
                            .withColumnRenamed('TXGRU','ComplimentaryClose') \
                            .withColumnRenamed('KALSE','CalculationSchemaGroup') \
                            .withColumnRenamed('MKALS','CalculationSchemaMarketPrice') \
                            .withColumnRenamed('BPEFF','EffectivePrice') \
                            .withColumnRenamed('BUKRS_NTR','CompanyCodeForSubsequentSettlement') \
                            .withColumnRenamed('LandingFileTimeStamp','LandingFileTimeStamp') \
                            .withColumnRenamed('UpdatedOn','UpdatedOn') \
                            .withColumn('DataSource',lit('SAP'))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_rename.write.format(write_format).mode("overwrite").save(write_path) 
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_rename.createOrReplaceTempView('stg_purchasing_organizations')

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC MERGE INTO fedw.purchasing_organizations  S
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY Client,PurchasingOrganization ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_purchasing_organizations)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.Client = T.Client 
# MAGIC and 
# MAGIC S.PurchasingOrganization = T.PurchasingOrganization
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC S.Client = T.Client,
# MAGIC S.PurchasingOrganization = T.PurchasingOrganization,
# MAGIC S.PurchasingOrganizationdesc = T.PurchasingOrganizationdesc,
# MAGIC S.CompanyCode = T.CompanyCode,
# MAGIC S.SenderLine = T.SenderLine,
# MAGIC S.LetterHeading = T.LetterHeading,
# MAGIC S.FooterLines = T.FooterLines,
# MAGIC S.ComplimentaryClose = T.ComplimentaryClose,
# MAGIC S.CalculationSchemaGroup = T.CalculationSchemaGroup,
# MAGIC S.CalculationSchemaMarketPrice = T.CalculationSchemaMarketPrice,
# MAGIC S.EffectivePrice = T.EffectivePrice,
# MAGIC S.CompanyCodeForSubsequentSettlement = T.CompanyCodeForSubsequentSettlement,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT
# MAGIC (
# MAGIC  Client,
# MAGIC PurchasingOrganization,
# MAGIC PurchasingOrganizationdesc,
# MAGIC CompanyCode,
# MAGIC SenderLine,
# MAGIC LetterHeading,
# MAGIC FooterLines,
# MAGIC ComplimentaryClose,
# MAGIC CalculationSchemaGroup,
# MAGIC CalculationSchemaMarketPrice,
# MAGIC EffectivePrice,
# MAGIC CompanyCodeForSubsequentSettlement,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.Client ,
# MAGIC T.PurchasingOrganization ,
# MAGIC T.PurchasingOrganizationdesc ,
# MAGIC T.CompanyCode ,
# MAGIC T.SenderLine ,
# MAGIC T.LetterHeading ,
# MAGIC T.FooterLines ,
# MAGIC T.ComplimentaryClose ,
# MAGIC T.CalculationSchemaGroup ,
# MAGIC T.CalculationSchemaMarketPrice ,
# MAGIC T.EffectivePrice ,
# MAGIC T.CompanyCodeForSubsequentSettlement ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC T.DataSource
# MAGIC )

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T024E'".format(ts))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Purchasing_Organizations where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchasing_Organizations')")


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
  .option("dbtable", "Purchasing_Organizations") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_Purchasing_Organizations()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Purchasing_Organizations'".format(ts))

# COMMAND ----------


