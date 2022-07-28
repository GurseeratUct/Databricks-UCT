# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp

# COMMAND ----------

table_name = 'SELLING_PRICE'
read_format = 'com.crealytics.spark.excel'
write_format = 'delta'
database_name = 'FLAT_FILE'
#read_path = '/mnt/uct-landing-gen-dev/Flat File/Selling Price/Selling price-6-13-2022.xlsx'
#read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
#archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
#write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(read_path)

# COMMAND ----------

df.show()

# COMMAND ----------

df_renamecolumn = df.withColumnRenamed("Site", "Site").withColumnRenamed("Part ID", "Part_ID").withColumnRenamed("Selling Price", "Selling_Price").withColumnRenamed("Effective date", "Effective_date")

# COMMAND ----------

df_renamecolumn.write.format(write_format).option("header", "true").mode("overwrite").save(write_path) 

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")

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

df_sf = spark.sql("select * from flat_file.selling_price")

# COMMAND ----------

df_sf.write \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("dbtable", "Selling_Price") \
  .mode("OVERWRITE") \
  .save()

# COMMAND ----------


