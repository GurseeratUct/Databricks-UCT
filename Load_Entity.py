# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Entity'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_entity = [
    "ID",
    "NAME",
    "UpdatedOn",
]


# COMMAND ----------


plant_code_singapore = spark.sql("select variable_value from Config.config_constant where variable_name = 'plant_code_singapore'")
plant_code_singapore = plant_code_singapore.first()[0]
print(plant_code_singapore)

plant_code_shangai = spark.sql("select variable_value from Config.config_constant where variable_name = 'plant_code_shangai'")
plant_code_shangai = plant_code_shangai.first()[0]
print(plant_code_shangai)

# COMMAND ----------

df = spark.sql("select * from VE70.entity where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'ENTITY' and TargetLayerTableName ='ENTITY' and DatabaseName = 'VE70')")
df_ve70_entity = df.select(col_names_entity).withColumn('Plant',lit(plant_code_shangai))
df_ve70_entity.createOrReplaceTempView("ve70_entity_tmp")
df_ts_ve70_entity = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_entity = df_ts_ve70_entity["max(UpdatedOn)"]
print(ts_ve70_entity)

# COMMAND ----------

df = spark.sql("select * from VE72.entity where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'ENTITY' and TargetLayerTableName ='ENTITY' and DatabaseName = 'VE72')")
df_ve72_entity = df.select(col_names_entity).withColumn('Plant',lit(plant_code_singapore))
df_ve72_entity.createOrReplaceTempView("ve72_entity_tmp")
df_ts_ve72_entity = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_entity = df_ts_ve72_entity["max(UpdatedOn)"]
print(ts_ve72_entity)

# COMMAND ----------

df_entity =  spark.sql("select *,'VE70' as DataSource from ve70_entity_tmp union select *,'VE72' as DataSource from ve72_entity_tmp")
df_entity.createOrReplaceTempView("entity_tmp")


# COMMAND ----------

# MAGIC %sql create or replace temp view entity as 
# MAGIC select ID,NAME,DATASOURCE, PLANT, now() as UpdatedOn from entity_tmp

# COMMAND ----------

df_entity = spark.sql("select * from entity")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_entity.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

if(ts_ve70_entity != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'ENTITY' and TargetLayerTableName ='ENTITY' and DatabaseName = 'VE70'".format(ts_ve70_entity))
    

if(ts_ve72_entity != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'ENTITY' and TargetLayerTableName ='ENTITY' and DatabaseName = 'VE72'".format(ts_ve72_entity))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.ENTITY where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Entity' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_entity = ts_sf["max(UpdatedOn)"]
print(ts_entity)

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
  .option("dbtable", "entity") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_ENTITY()""")

# COMMAND ----------

if(ts_entity != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Entity' and DatabaseName = 'FEDW'".format(ts_entity))

# COMMAND ----------


