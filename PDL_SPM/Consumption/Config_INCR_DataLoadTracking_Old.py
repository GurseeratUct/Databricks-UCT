# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([\
    StructField("SourceLayer", StringType(), True),\
    StructField("SourceLayerTableName", StringType(), True),\
    StructField("TargetLayer", StringType(), True),\
    StructField("TargetLayerTableName", StringType(), True), \
    StructField("LastUpdatedOn", TimestampType(), True)
                    ])

# COMMAND ----------

#df.write.format("delta").option("header", True).mode("overwrite").save("/mnt/uct-consumption-gen-dev/Config/INCR_DataLoadTracking")

# COMMAND ----------

#spark.sql("CREATE TABLE IF NOT EXISTS INCR_DataLoadTracking USING DELTA LOCATION '/mnt/uct-consumption-gen-dev/Config/INCR_DataLoadTracking'")

# COMMAND ----------

#%sql select * from config.INCR_DataLoadTracking vendor_quote

# COMMAND ----------

# MAGIC %sql insert into INCR_DataLoadTracking values("Transform","CommodityCode","Consumption","CommodityCode","1900-01-01T12:00:00.000+0000","FLAT_FILE")

# COMMAND ----------

#%sql update Config.INCR_DataLoadTracking set DatabaseName = 'FEDW' where TargetLayer = 'Snowflake' and TargetLayerTableName = 'DimMaterial'

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, "/mnt/uct-consumption-gen-dev/Config/INCR_DataLoadTracking")

#deltaTable.restoreToVersion(963)

#fullHistoryDF = deltaTable.history()  
#fullHistoryDF.show()

# COMMAND ----------

# MAGIC %sql select * from INCR_DataLoadTracking

# COMMAND ----------

# MAGIC %sql select * from INCR_DataLoadTracking where TargetLayerTableName = 'PO_Detail'

# COMMAND ----------

# MAGIC %sql update INCR_DataLoadTracking set LastUpdatedOn = '1900-01-01T12:00:00.000+0000' where TargetLayerTableName = 'PO_Detail'

# COMMAND ----------

# MAGIC %sql delete from INCR_DataLoadTracking where TargetLayerTableName = 'PO_Detail' 

# COMMAND ----------

# MAGIC %sql select * from config.INCR_DataLoadTracking where TargetLayerTableName = 'Purchase_Source_List'

# COMMAND ----------

ztc2p_mat_map

# COMMAND ----------

# MAGIC %sql delete from where Currency_Exchange

# COMMAND ----------

# MAGIC %sql insert into config.INCR_DataLoadTracking values("Consumption","Currency_Code_Name","Snowflake","Currency_Code_Name","1900-01-01T12:00:00.000+0000","FEDW")

# COMMAND ----------

# MAGIC %sql insert into config.INCR_DataLoadTracking values("Transform","TCURT","Consumption","Currency_Code_Name","1900-01-01T12:00:00.000+0000","S42")

# COMMAND ----------


