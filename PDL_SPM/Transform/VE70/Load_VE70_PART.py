# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'PART'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE70'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.options(header='True', inferSchema='True', delimiter='^').csv(read_path)

# COMMAND ----------

schema = StructType([ \
StructField('ID',StringType(),True),\
StructField('DESCRIPTION',StringType(),True),\
StructField('STOCK_UM',StringType(),True),\
StructField('PLANNING_LEADTIME',IntegerType(),True),\
StructField('ORDER_POLICY',StringType(),True),\
StructField('ORDER_POINT',StringType(),True),\
StructField('SAFETY_STOCK_QTY',DoubleType(),True),\
StructField('FIXED_ORDER_QTY',DoubleType(),True),\
StructField('DAYS_OF_SUPPLY',DoubleType(),True),\
StructField('MINIMUM_ORDER_QTY',DoubleType(),True),\
StructField('MAXIMUM_ORDER_QTY',DoubleType(),True),\
StructField('ENGINEERING_MSTR',DoubleType(),True),\
StructField('PRODUCT_CODE',StringType(),True),\
StructField('COMMODITY_CODE',StringType(),True),\
StructField('MFG_NAME',StringType(),True),\
StructField('MFG_PART_ID',StringType(),True),\
StructField('FABRICATED',StringType(),True),\
StructField('PURCHASED',StringType(),True),\
StructField('INSPECTION_REQD',StringType(),True),\
StructField('PREF_VENDOR_ID',StringType(),True),\
StructField('PRIMARY_WHS_ID',StringType(),True),\
StructField('PRIMARY_LOC_ID',StringType(),True),\
StructField('AUTO_BACKFLUSH',StringType(),True),\
StructField('PLANNER_USER_ID',StringType(),True),\
StructField('BUYER_USER_ID',StringType(),True),\
StructField('ABC_CODE',StringType(),True),\
StructField('INVENTORY_LOCKED',StringType(),True),\
StructField('UNIT_MATERIAL_COST',DoubleType(),True),\
StructField('UNIT_LABOR_COST',DoubleType(),True),\
StructField('UNIT_BURDEN_COST',DoubleType(),True),\
StructField('UNIT_SERVICE_COST',DoubleType(),True),\
StructField('PURC_BUR_PERCENT',DoubleType(),True),\
StructField('QTY_AVAILABLE_ISS',DoubleType(),True),\
StructField('USER_4',StringType(),True),\
StructField('USER_5',StringType(),True),\
StructField('USER_6',StringType(),True),\
StructField('MULTIPLE_ORDER_QTY',DoubleType(),True),\
StructField('CONSUMABLE',StringType(),True),\
StructField('STATUS',StringType(),True),\
StructField('REVISION_ID',StringType(),True),\
StructField('STAGE_ID',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
                    ])


# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE70'))

# COMMAND ----------

df_transform = df_add_column.na.fill(0).withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-',''))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE70.PART as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY ID ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_PART )A where A.rn = 1 ) as S 
# MAGIC ON T.ID = S.ID 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`ID` = S.`ID`,
# MAGIC T.`DESCRIPTION` = S.`DESCRIPTION`,
# MAGIC T.`STOCK_UM` = S.`STOCK_UM`,
# MAGIC T.`PLANNING_LEADTIME` = S.`PLANNING_LEADTIME`,
# MAGIC T.`ORDER_POLICY` = S.`ORDER_POLICY`,
# MAGIC T.`ORDER_POINT` = S.`ORDER_POINT`,
# MAGIC T.`SAFETY_STOCK_QTY` = S.`SAFETY_STOCK_QTY`,
# MAGIC T.`FIXED_ORDER_QTY` = S.`FIXED_ORDER_QTY`,
# MAGIC T.`DAYS_OF_SUPPLY` = S.`DAYS_OF_SUPPLY`,
# MAGIC T.`MINIMUM_ORDER_QTY` = S.`MINIMUM_ORDER_QTY`,
# MAGIC T.`MAXIMUM_ORDER_QTY` = S.`MAXIMUM_ORDER_QTY`,
# MAGIC T.`ENGINEERING_MSTR` = S.`ENGINEERING_MSTR`,
# MAGIC T.`PRODUCT_CODE` = S.`PRODUCT_CODE`,
# MAGIC T.`COMMODITY_CODE` = S.`COMMODITY_CODE`,
# MAGIC T.`MFG_NAME` = S.`MFG_NAME`,
# MAGIC T.`MFG_PART_ID` = S.`MFG_PART_ID`,
# MAGIC T.`FABRICATED` = S.`FABRICATED`,
# MAGIC T.`PURCHASED` = S.`PURCHASED`,
# MAGIC T.`INSPECTION_REQD` = S.`INSPECTION_REQD`,
# MAGIC T.`PREF_VENDOR_ID` = S.`PREF_VENDOR_ID`,
# MAGIC T.`PRIMARY_WHS_ID` = S.`PRIMARY_WHS_ID`,
# MAGIC T.`PRIMARY_LOC_ID` = S.`PRIMARY_LOC_ID`,
# MAGIC T.`AUTO_BACKFLUSH` = S.`AUTO_BACKFLUSH`,
# MAGIC T.`PLANNER_USER_ID` = S.`PLANNER_USER_ID`,
# MAGIC T.`BUYER_USER_ID` = S.`BUYER_USER_ID`,
# MAGIC T.`ABC_CODE` = S.`ABC_CODE`,
# MAGIC T.`INVENTORY_LOCKED` = S.`INVENTORY_LOCKED`,
# MAGIC T.`UNIT_MATERIAL_COST` = S.`UNIT_MATERIAL_COST`,
# MAGIC T.`UNIT_LABOR_COST` = S.`UNIT_LABOR_COST`,
# MAGIC T.`UNIT_BURDEN_COST` = S.`UNIT_BURDEN_COST`,
# MAGIC T.`UNIT_SERVICE_COST` = S.`UNIT_SERVICE_COST`,
# MAGIC T.`PURC_BUR_PERCENT` = S.`PURC_BUR_PERCENT`,
# MAGIC T.`QTY_AVAILABLE_ISS` = S.`QTY_AVAILABLE_ISS`,
# MAGIC T.`USER_4` = S.`USER_4`,
# MAGIC T.`USER_5` = S.`USER_5`,
# MAGIC T.`USER_6` = S.`USER_6`,
# MAGIC T.`MULTIPLE_ORDER_QTY` = S.`MULTIPLE_ORDER_QTY`,
# MAGIC T.`CONSUMABLE` = S.`CONSUMABLE`,
# MAGIC T.`STATUS` = S.`STATUS`,
# MAGIC T.`REVISION_ID` = S.`REVISION_ID`,
# MAGIC T.`STAGE_ID` = S.`STAGE_ID`,
# MAGIC T.`LandingFileTimeStamp` = S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `ID`,
# MAGIC `DESCRIPTION`,
# MAGIC `STOCK_UM`,
# MAGIC `PLANNING_LEADTIME`,
# MAGIC `ORDER_POLICY`,
# MAGIC `ORDER_POINT`,
# MAGIC `SAFETY_STOCK_QTY`,
# MAGIC `FIXED_ORDER_QTY`,
# MAGIC `DAYS_OF_SUPPLY`,
# MAGIC `MINIMUM_ORDER_QTY`,
# MAGIC `MAXIMUM_ORDER_QTY`,
# MAGIC `ENGINEERING_MSTR`,
# MAGIC `PRODUCT_CODE`,
# MAGIC `COMMODITY_CODE`,
# MAGIC `MFG_NAME`,
# MAGIC `MFG_PART_ID`,
# MAGIC `FABRICATED`,
# MAGIC `PURCHASED`,
# MAGIC `INSPECTION_REQD`,
# MAGIC `PREF_VENDOR_ID`,
# MAGIC `PRIMARY_WHS_ID`,
# MAGIC `PRIMARY_LOC_ID`,
# MAGIC `AUTO_BACKFLUSH`,
# MAGIC `PLANNER_USER_ID`,
# MAGIC `BUYER_USER_ID`,
# MAGIC `ABC_CODE`,
# MAGIC `INVENTORY_LOCKED`,
# MAGIC `UNIT_MATERIAL_COST`,
# MAGIC `UNIT_LABOR_COST`,
# MAGIC `UNIT_BURDEN_COST`,
# MAGIC `UNIT_SERVICE_COST`,
# MAGIC `PURC_BUR_PERCENT`,
# MAGIC `QTY_AVAILABLE_ISS`,
# MAGIC `USER_4`,
# MAGIC `USER_5`,
# MAGIC `USER_6`,
# MAGIC `MULTIPLE_ORDER_QTY`,
# MAGIC `CONSUMABLE`,
# MAGIC `STATUS`,
# MAGIC `REVISION_ID`,
# MAGIC `STAGE_ID`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC ) values
# MAGIC (
# MAGIC S.`ID`,
# MAGIC S.`DESCRIPTION`,
# MAGIC S.`STOCK_UM`,
# MAGIC S.`PLANNING_LEADTIME`,
# MAGIC S.`ORDER_POLICY`,
# MAGIC S.`ORDER_POINT`,
# MAGIC S.`SAFETY_STOCK_QTY`,
# MAGIC S.`FIXED_ORDER_QTY`,
# MAGIC S.`DAYS_OF_SUPPLY`,
# MAGIC S.`MINIMUM_ORDER_QTY`,
# MAGIC S.`MAXIMUM_ORDER_QTY`,
# MAGIC S.`ENGINEERING_MSTR`,
# MAGIC S.`PRODUCT_CODE`,
# MAGIC S.`COMMODITY_CODE`,
# MAGIC S.`MFG_NAME`,
# MAGIC S.`MFG_PART_ID`,
# MAGIC S.`FABRICATED`,
# MAGIC S.`PURCHASED`,
# MAGIC S.`INSPECTION_REQD`,
# MAGIC S.`PREF_VENDOR_ID`,
# MAGIC S.`PRIMARY_WHS_ID`,
# MAGIC S.`PRIMARY_LOC_ID`,
# MAGIC S.`AUTO_BACKFLUSH`,
# MAGIC S.`PLANNER_USER_ID`,
# MAGIC S.`BUYER_USER_ID`,
# MAGIC S.`ABC_CODE`,
# MAGIC S.`INVENTORY_LOCKED`,
# MAGIC S.`UNIT_MATERIAL_COST`,
# MAGIC S.`UNIT_LABOR_COST`,
# MAGIC S.`UNIT_BURDEN_COST`,
# MAGIC S.`UNIT_SERVICE_COST`,
# MAGIC S.`PURC_BUR_PERCENT`,
# MAGIC S.`QTY_AVAILABLE_ISS`,
# MAGIC S.`USER_4`,
# MAGIC S.`USER_5`,
# MAGIC S.`USER_6`,
# MAGIC S.`MULTIPLE_ORDER_QTY`,
# MAGIC S.`CONSUMABLE`,
# MAGIC S.`STATUS`,
# MAGIC S.`REVISION_ID`,
# MAGIC S.`STAGE_ID`,
# MAGIC S.LandingFileTimeStamp,
# MAGIC 'VE70',
# MAGIC now()
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


