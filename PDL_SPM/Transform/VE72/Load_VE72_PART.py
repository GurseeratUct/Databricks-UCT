# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'PART'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE72'
delimiter = '^'
#read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/PART_05122022-120937.csv'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name+'_VE72'

# COMMAND ----------

schema = StructType([ \
StructField('ROWID',StringType(),True),\
StructField('ID',StringType(),True),\
StructField('DESCRIPTION',StringType(),True),\
StructField('STOCK_UM',StringType(),True),\
StructField('PLANNING_LEADTIME',StringType(),True),\
StructField('ORDER_POLICY',StringType(),True),\
StructField('ORDER_POINT',StringType(),True),\
StructField('SAFETY_STOCK_QTY',StringType(),True),\
StructField('FIXED_ORDER_QTY',StringType(),True),\
StructField('DAYS_OF_SUPPLY',StringType(),True),\
StructField('MINIMUM_ORDER_QTY',StringType(),True),\
StructField('MAXIMUM_ORDER_QTY',StringType(),True),\
StructField('ENGINEERING_MSTR',StringType(),True),\
StructField('PRODUCT_CODE',StringType(),True),\
StructField('COMMODITY_CODE',StringType(),True),\
StructField('MFG_NAME',StringType(),True),\
StructField('MFG_PART_ID',StringType(),True),\
StructField('FABRICATED',StringType(),True),\
StructField('PURCHASED',StringType(),True),\
StructField('STOCKED',StringType(),True),\
StructField('DETAIL_ONLY',StringType(),True),\
StructField('DEMAND_HISTORY',StringType(),True),\
StructField('TOOL_OR_FIXTURE',StringType(),True),\
StructField('INSPECTION_REQD',StringType(),True),\
StructField('WEIGHT',StringType(),True),\
StructField('WEIGHT_UM',StringType(),True),\
StructField('DRAWING_ID',StringType(),True),\
StructField('DRAWING_REV_NO',StringType(),True),\
StructField('PREF_VENDOR_ID',StringType(),True),\
StructField('PRIMARY_WHS_ID',StringType(),True),\
StructField('PRIMARY_LOC_ID',StringType(),True),\
StructField('BACKFLUSH_WHS_ID',StringType(),True),\
StructField('BACKFLUSH_LOC_ID',StringType(),True),\
StructField('INSPECT_WHS_ID',StringType(),True),\
StructField('INSPECT_LOC_ID',StringType(),True),\
StructField('MRP_REQUIRED',StringType(),True),\
StructField('MRP_EXCEPTIONS',StringType(),True),\
StructField('PRIVATE_UM_CONV',StringType(),True),\
StructField('AUTO_BACKFLUSH',StringType(),True),\
StructField('PLANNER_USER_ID',StringType(),True),\
StructField('BUYER_USER_ID',StringType(),True),\
StructField('ABC_CODE',StringType(),True),\
StructField('ANNUAL_USAGE_QTY',StringType(),True),\
StructField('INVENTORY_LOCKED',StringType(),True),\
StructField('UNIT_MATERIAL_COST',StringType(),True),\
StructField('UNIT_LABOR_COST',StringType(),True),\
StructField('UNIT_BURDEN_COST',StringType(),True),\
StructField('UNIT_SERVICE_COST',StringType(),True),\
StructField('BURDEN_PERCENT',StringType(),True),\
StructField('BURDEN_PER_UNIT',StringType(),True),\
StructField('PURC_BUR_PERCENT',StringType(),True),\
StructField('PURC_BUR_PER_UNIT',StringType(),True),\
StructField('FIXED_COST',StringType(),True),\
StructField('UNIT_PRICE',StringType(),True),\
StructField('NEW_MATERIAL_COST',StringType(),True),\
StructField('NEW_LABOR_COST',StringType(),True),\
StructField('NEW_BURDEN_COST',StringType(),True),\
StructField('NEW_SERVICE_COST',StringType(),True),\
StructField('NEW_BURDEN_PERCENT',StringType(),True),\
StructField('NEW_BURDEN_PERUNIT',StringType(),True),\
StructField('NEW_FIXED_COST',StringType(),True),\
StructField('MAT_GL_ACCT_ID',StringType(),True),\
StructField('LAB_GL_ACCT_ID',StringType(),True),\
StructField('BUR_GL_ACCT_ID',StringType(),True),\
StructField('SER_GL_ACCT_ID',StringType(),True),\
StructField('QTY_ON_HAND',StringType(),True),\
StructField('QTY_AVAILABLE_ISS',StringType(),True),\
StructField('QTY_AVAILABLE_MRP',StringType(),True),\
StructField('QTY_ON_ORDER',StringType(),True),\
StructField('QTY_IN_DEMAND',StringType(),True),\
StructField('USER_1',StringType(),True),\
StructField('USER_2',StringType(),True),\
StructField('USER_3',StringType(),True),\
StructField('USER_4',StringType(),True),\
StructField('USER_5',StringType(),True),\
StructField('USER_6',StringType(),True),\
StructField('USER_7',StringType(),True),\
StructField('USER_8',StringType(),True),\
StructField('USER_9',StringType(),True),\
StructField('USER_10',StringType(),True),\
StructField('NMFC_CODE_ID',StringType(),True),\
StructField('PACKAGE_TYPE',StringType(),True),\
StructField('WHSALE_UNIT_COST',StringType(),True),\
StructField('MRP_EXCEPTION_INFO',StringType(),True),\
StructField('MULTIPLE_ORDER_QTY',StringType(),True),\
StructField('ADD_FORECAST',StringType(),True),\
StructField('UDF_LAYOUT_ID',StringType(),True),\
StructField('PIECE_TRACKED',StringType(),True),\
StructField('LENGTH_REQD',StringType(),True),\
StructField('WIDTH_REQD',StringType(),True),\
StructField('HEIGHT_REQD',StringType(),True),\
StructField('DIMENSIONS_UM',StringType(),True),\
StructField('SHIP_DIMENSIONS',StringType(),True),\
StructField('DRAWING_FILE',StringType(),True),\
StructField('TARIFF_CODE',StringType(),True),\
StructField('TARIFF_TYPE',StringType(),True),\
StructField('ORIG_COUNTRY_ID',StringType(),True),\
StructField('NET_WEIGHT_2',StringType(),True),\
StructField('GROSS_WEIGHT_2',StringType(),True),\
StructField('WEIGHT_UM_2',StringType(),True),\
StructField('VOLUME',StringType(),True),\
StructField('VOLUME_UM',StringType(),True),\
StructField('EXCISE_UNIT_PRICE',StringType(),True),\
StructField('VAT_CODE',StringType(),True),\
StructField('DEMAND_FENCE_1',StringType(),True),\
StructField('DEMAND_FENCE_2',StringType(),True),\
StructField('ROLL_FORECAST',StringType(),True),\
StructField('CONSUMABLE',StringType(),True),\
StructField('PRIMARY_SOURCE',StringType(),True),\
StructField('LABEL_UM',StringType(),True),\
StructField('HTS_CODE',StringType(),True),\
StructField('DEF_ORIG_COUNTRY',StringType(),True),\
StructField('MATERIAL_CODE',StringType(),True),\
StructField('DEF_LBL_FORMAT_ID',StringType(),True),\
StructField('VOLATILE_LEADTIME',StringType(),True),\
StructField('LT_PLUS_DAYS',StringType(),True),\
StructField('LT_MINUS_DAYS',StringType(),True),\
StructField('STATUS',StringType(),True),\
StructField('USE_SUPPLY_BEF_LT',StringType(),True),\
StructField('QTY_COMMITTED',StringType(),True),\
StructField('INTRASTAT_EXEMPT',StringType(),True),\
StructField('CASE_QTY',StringType(),True),\
StructField('PALLET_QTY',StringType(),True),\
StructField('MINIMUM_LEADTIME',StringType(),True),\
StructField('LEADTIME_BUFFER',StringType(),True),\
StructField('EMERGENCY_STOCKPCT',StringType(),True),\
StructField('REPLENISH_LEVEL',StringType(),True),\
StructField('MIN_BATCH_SIZE',StringType(),True),\
StructField('EFF_DATE_PRICE',StringType(),True),\
StructField('ECN_REVISION',StringType(),True),\
StructField('REVISION_ID',StringType(),True),\
StructField('STAGE_ID',StringType(),True),\
StructField('ECN_REV_CONTROL',StringType(),True),\
StructField('IS_KIT',StringType(),True),\
StructField('YELLOW_STOCKPCT',StringType(),True),\
StructField('UNIV_PLAN_MATERIAL',StringType(),True),\
StructField('RLS_NEAR_DAYS',StringType(),True),\
StructField('SUGG_RLS_NEAR_DAYS',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
                    ])


# COMMAND ----------

col_names = ["ID",
"DESCRIPTION",
"STOCK_UM",
"PLANNING_LEADTIME",
"ORDER_POLICY",
"ORDER_POINT",
"SAFETY_STOCK_QTY",
"FIXED_ORDER_QTY",
"DAYS_OF_SUPPLY",
"MINIMUM_ORDER_QTY",
"MAXIMUM_ORDER_QTY",
"ENGINEERING_MSTR",
"PRODUCT_CODE",
"COMMODITY_CODE",
"MFG_NAME",
"MFG_PART_ID",
"FABRICATED",
"PURCHASED",
"INSPECTION_REQD",
"PREF_VENDOR_ID",
"PRIMARY_WHS_ID",
"PRIMARY_LOC_ID",
"AUTO_BACKFLUSH",
"PLANNER_USER_ID",
"BUYER_USER_ID",
"ABC_CODE",
"INVENTORY_LOCKED",
"UNIT_MATERIAL_COST",
"UNIT_LABOR_COST",
"UNIT_BURDEN_COST",
"UNIT_SERVICE_COST",
"PURC_BUR_PERCENT",
"QTY_AVAILABLE_ISS",
"USER_4",
"USER_5",
"USER_6",
"MULTIPLE_ORDER_QTY",
"CONSUMABLE",
"STATUS",
"REVISION_ID",
"STAGE_ID", "LandingFileTimeStamp"]

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path) \
      .select(col_names)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE72'))

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
# MAGIC MERGE INTO VE72.PART as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY ID ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_PART_VE72 )A where A.rn = 1 and ID is not null ) as S 
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
# MAGIC 'VE72',
# MAGIC now()
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


