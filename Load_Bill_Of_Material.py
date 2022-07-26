# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Bill_Of_Material'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Fact/'+consumption_table_name+'/' 
database_name = 'FEDW'

# COMMAND ----------

col_names_requirement = [
"WORKORDER_TYPE",
"WORKORDER_BASE_ID",
"WORKORDER_SPLIT_ID",
"WORKORDER_LOT_ID",
"WORKORDER_SUB_ID",
"Lookup From Part Table",
"DESIRED_QTY",
"EFFECTIVE_DATE",
"Lookup From Part Table",
"QTY_PER",
"PART_ID",
"WORKORDER_BASE_ID",
"Hard Code = 3101 if Singapore, 3201 if China",
"Hard Code = 1",
"STATUS",
"QTY_PER_TYPE",
"FIXED_QTY",
"SCRAP_PERCENT",
"CALC_QTY",
"COMMODITY_CODE",
"ENTITY_ID"
]


# COMMAND ----------

col_names_work_order = [
"TYPE",
"BASE_ID",
"SPLIT_ID",
"LOT_ID",
"SUB_ID",
"DESIRED_QTY"
]


# COMMAND ----------

col_names_stpo = [
 "STLTY",
"STLNR",
"STPOZ",
"STLKN",
"LKENZ",
"ANDAT",
"ANNAM",
"AEDAT",
"AENAM",
"POSNR",
"FMENG",
"AUSCH",
"POTX1",
"POTX2",
"EKORG",
"VALID_TO",
"VALID_TO_RKEY",
"AEDAT",
"AENAM",
"DATUV",
"MEINS",
"MENGE",
"IDNRK",
"STLAN"   
]

# COMMAND ----------

col_names_stko = [
"STLTY",
"STLNR",
"STLAL",
"STKOZ",
"STKTX",
"STLST",
"BMEIN",
"BMENG"
]

# COMMAND ----------

col_names_mast = [
"MATNR",
"WERKS"
]

# COMMAND ----------

df = spark.sql("select * from VE70.PURCHASE_ORDER where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PURCHASE_ORDER' and DatabaseName = 'VE70')")
df_ve70_purchase_order = df.select(col_names_purchaseorder).withColumn('CompanyCode',lit(company_code_shangai)).withColumn('Plant',lit(plant_code_shangai)).withColumn('DataSource',lit('VE70'))
df_ve70_purchase_order.createOrReplaceTempView("ve70_purchase_order_tmp")
df_ts_ve70_purchase_order = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_purchase_order = df_ts_ve70_purchase_order["max(UpdatedOn)"]
print(ts_ve70_purchase_order)
