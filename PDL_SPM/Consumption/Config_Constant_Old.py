# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([\
    StructField("variable_name", StringType(), True),\
    StructField("variable_value", StringType(), True),\
    StructField("description", StringType(), True),\
                    ])

# COMMAND ----------



# COMMAND ----------

#df = spark.read.format("csv").option("header", "true").schema(schema).load("dbfs:/FileStore/shared_uploads/pankaj.kushwah@uct.com/constant.csv")
#df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp()))
#df_add_column.write.format("delta").option("header", True).mode("overwrite").save("/mnt/uct-consumption-gen-dev/Config/Config_Constant")
#spark.sql("CREATE TABLE IF NOT EXISTS config.Config_Constant USING DELTA LOCATION '/mnt/uct-consumption-gen-dev/Config/Config_Constant'")

# COMMAND ----------

#%sql insert into config.Config_Constant(variable_name,variable_value,description,UpdatedOn) values("plant_code_singapore","6311","Plant Code Singapore",now())

# COMMAND ----------

# MAGIC %sql update config.Config_Constant set description = 'Plant Code Shangai'  where variable_value = 6311

# COMMAND ----------

# MAGIC %sql insert into config.Config_Constant(variable_name,variable_value,description,UpdatedOn) values("insecureMode","true","insecureMode",now())

# COMMAND ----------

# MAGIC %sql select * from config.Config_Constant

# COMMAND ----------


