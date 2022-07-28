# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'REQUIREMENT'
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
StructField('WORKORDER_TYPE',StringType(),True),\
StructField('WORKORDER_BASE_ID',StringType(),True),\
StructField('WORKORDER_LOT_ID',StringType(),True),\
StructField('WORKORDER_SPLIT_ID',IntegerType(),True),\
StructField('WORKORDER_SUB_ID',IntegerType(),True),\
StructField('OPERATION_SEQ_NO',IntegerType(),True),\
StructField('PIECE_NO',IntegerType(),True),\
StructField('SUBORD_WO_SUB_ID',IntegerType(),True),\
StructField('PART_ID',StringType(),True),\
StructField('STATUS',StringType(),True),\
StructField('QTY_PER',DoubleType(),True),\
StructField('QTY_PER_TYPE',StringType(),True),\
StructField('FIXED_QTY',DoubleType(),True),\
StructField('USAGE_UM',StringType(),True),\
StructField('EFFECTIVE_DATE',StringType(),True),\
StructField('DISCONTINUE_DATE',StringType(),True),\
StructField('CALC_QTY',DoubleType(),True),\
StructField('ISSUED_QTY',DoubleType(),True),\
StructField('REQUIRED_DATE',StringType(),True),\
StructField('CLOSE_DATE',StringType(),True),\
StructField('UNIT_MATERIAL_COST',DoubleType(),True),\
StructField('UNIT_LABOR_COST',DoubleType(),True),\
StructField('UNIT_BURDEN_COST',DoubleType(),True),\
StructField('UNIT_SERVICE_COST',DoubleType(),True),\
StructField('BURDEN_PERCENT',DoubleType(),True),\
StructField('BURDEN_PER_UNIT',DoubleType(),True),\
StructField('FIXED_COST',DoubleType(),True),\
StructField('VENDOR_ID',StringType(),True),\
StructField('VENDOR_PART_ID',StringType(),True),\
StructField('EST_MATERIAL_COST',DoubleType(),True),\
StructField('EST_LABOR_COST',DoubleType(),True),\
StructField('EST_BURDEN_COST',DoubleType(),True),\
StructField('EST_SERVICE_COST',DoubleType(),True),\
StructField('ACT_MATERIAL_COST',DoubleType(),True),\
StructField('ACT_LABOR_COST',DoubleType(),True),\
StructField('ACT_BURDEN_COST',DoubleType(),True),\
StructField('ACT_SERVICE_COST',DoubleType(),True),\
StructField('USER_1',IntegerType(),True),\
StructField('USER_2',StringType(),True),\
StructField('USER_3',StringType(),True),\
StructField('USER_4',StringType(),True),\
StructField('USER_5',StringType(),True),\
StructField('USER_6',StringType(),True),\
StructField('USER_7',StringType(),True),\
StructField('USER_8',StringType(),True),\
StructField('USER_9',StringType(),True),\
StructField('USER_10',StringType(),True),\
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

df_transform = df_add_column.na.fill(0).withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
                                       .withColumn("EFFECTIVE_DATE", to_timestamp(regexp_replace(df_add_column.EFFECTIVE_DATE,'\.','-'))) \
                                        .withColumn("DISCONTINUE_DATE", to_timestamp(regexp_replace(df_add_column.DISCONTINUE_DATE,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE70.REQUIREMENT as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY WORKORDER_TYPE,WORKORDER_BASE_ID,WORKORDER_LOT_ID,WORKORDER_SPLIT_ID,WORKORDER_SUB_ID,OPERATION_SEQ_NO,PIECE_NO ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_REQUIREMENT)A where A.rn = 1) as S 
# MAGIC ON T.`WORKORDER_TYPE`	 = S.`WORKORDER_TYPE`
# MAGIC and T.`WORKORDER_BASE_ID`	 = S.`WORKORDER_BASE_ID`
# MAGIC and T.`WORKORDER_LOT_ID`	 = S.`WORKORDER_LOT_ID`
# MAGIC and T.`WORKORDER_SPLIT_ID`	 = S.`WORKORDER_SPLIT_ID`
# MAGIC and T.`WORKORDER_SUB_ID`	 = S.`WORKORDER_SUB_ID`
# MAGIC and T.`OPERATION_SEQ_NO`	 = S.`OPERATION_SEQ_NO`
# MAGIC and T.`PIECE_NO`	 = S.`PIECE_NO`
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`WORKORDER_TYPE`	 = S.`WORKORDER_TYPE`,
# MAGIC T.`WORKORDER_BASE_ID`	 = S.`WORKORDER_BASE_ID`,
# MAGIC T.`WORKORDER_LOT_ID`	 = S.`WORKORDER_LOT_ID`,
# MAGIC T.`WORKORDER_SPLIT_ID`	 = S.`WORKORDER_SPLIT_ID`,
# MAGIC T.`WORKORDER_SUB_ID`	 = S.`WORKORDER_SUB_ID`,
# MAGIC T.`OPERATION_SEQ_NO`	 = S.`OPERATION_SEQ_NO`,
# MAGIC T.`PIECE_NO`	 = S.`PIECE_NO`,
# MAGIC T.`SUBORD_WO_SUB_ID`	 = S.`SUBORD_WO_SUB_ID`,
# MAGIC T.`PART_ID`	 = S.`PART_ID`,
# MAGIC T.`STATUS`	 = S.`STATUS`,
# MAGIC T.`QTY_PER`	 = S.`QTY_PER`,
# MAGIC T.`QTY_PER_TYPE`	 = S.`QTY_PER_TYPE`,
# MAGIC T.`FIXED_QTY`	 = S.`FIXED_QTY`,
# MAGIC T.`USAGE_UM`	 = S.`USAGE_UM`,
# MAGIC T.`EFFECTIVE_DATE`	 = S.`EFFECTIVE_DATE`,
# MAGIC T.`DISCONTINUE_DATE`	 = S.`DISCONTINUE_DATE`,
# MAGIC T.`CALC_QTY`	 = S.`CALC_QTY`,
# MAGIC T.`ISSUED_QTY`	 = S.`ISSUED_QTY`,
# MAGIC T.`REQUIRED_DATE`	 = S.`REQUIRED_DATE`,
# MAGIC T.`CLOSE_DATE`	 = S.`CLOSE_DATE`,
# MAGIC T.`UNIT_MATERIAL_COST`	 = S.`UNIT_MATERIAL_COST`,
# MAGIC T.`UNIT_LABOR_COST`	 = S.`UNIT_LABOR_COST`,
# MAGIC T.`UNIT_BURDEN_COST`	 = S.`UNIT_BURDEN_COST`,
# MAGIC T.`UNIT_SERVICE_COST`	 = S.`UNIT_SERVICE_COST`,
# MAGIC T.`BURDEN_PERCENT`	 = S.`BURDEN_PERCENT`,
# MAGIC T.`BURDEN_PER_UNIT`	 = S.`BURDEN_PER_UNIT`,
# MAGIC T.`FIXED_COST`	 = S.`FIXED_COST`,
# MAGIC T.`VENDOR_ID`	 = S.`VENDOR_ID`,
# MAGIC T.`VENDOR_PART_ID`	 = S.`VENDOR_PART_ID`,
# MAGIC T.`EST_MATERIAL_COST`	 = S.`EST_MATERIAL_COST`,
# MAGIC T.`EST_LABOR_COST`	 = S.`EST_LABOR_COST`,
# MAGIC T.`EST_BURDEN_COST`	 = S.`EST_BURDEN_COST`,
# MAGIC T.`EST_SERVICE_COST`	 = S.`EST_SERVICE_COST`,
# MAGIC T.`ACT_MATERIAL_COST`	 = S.`ACT_MATERIAL_COST`,
# MAGIC T.`ACT_LABOR_COST`	 = S.`ACT_LABOR_COST`,
# MAGIC T.`ACT_BURDEN_COST`	 = S.`ACT_BURDEN_COST`,
# MAGIC T.`ACT_SERVICE_COST`	 = S.`ACT_SERVICE_COST`,
# MAGIC T.`USER_1`	 = S.`USER_1`,
# MAGIC T.`USER_2`	 = S.`USER_2`,
# MAGIC T.`USER_3`	 = S.`USER_3`,
# MAGIC T.`USER_4`	 = S.`USER_4`,
# MAGIC T.`USER_5`	 = S.`USER_5`,
# MAGIC T.`USER_6`	 = S.`USER_6`,
# MAGIC T.`USER_7`	 = S.`USER_7`,
# MAGIC T.`USER_8`	 = S.`USER_8`,
# MAGIC T.`USER_9`	 = S.`USER_9`,
# MAGIC T.`USER_10`	 = S.`USER_10`,
# MAGIC T.`LandingFileTimeStamp`	 = S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` =  now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `WORKORDER_TYPE`,
# MAGIC `WORKORDER_BASE_ID`,
# MAGIC `WORKORDER_LOT_ID`,
# MAGIC `WORKORDER_SPLIT_ID`,
# MAGIC `WORKORDER_SUB_ID`,
# MAGIC `OPERATION_SEQ_NO`,
# MAGIC `PIECE_NO`,
# MAGIC `SUBORD_WO_SUB_ID`,
# MAGIC `PART_ID`,
# MAGIC `STATUS`,
# MAGIC `QTY_PER`,
# MAGIC `QTY_PER_TYPE`,
# MAGIC `FIXED_QTY`,
# MAGIC `USAGE_UM`,
# MAGIC `EFFECTIVE_DATE`,
# MAGIC `DISCONTINUE_DATE`,
# MAGIC `CALC_QTY`,
# MAGIC `ISSUED_QTY`,
# MAGIC `REQUIRED_DATE`,
# MAGIC `CLOSE_DATE`,
# MAGIC `UNIT_MATERIAL_COST`,
# MAGIC `UNIT_LABOR_COST`,
# MAGIC `UNIT_BURDEN_COST`,
# MAGIC `UNIT_SERVICE_COST`,
# MAGIC `BURDEN_PERCENT`,
# MAGIC `BURDEN_PER_UNIT`,
# MAGIC `FIXED_COST`,
# MAGIC `VENDOR_ID`,
# MAGIC `VENDOR_PART_ID`,
# MAGIC `EST_MATERIAL_COST`,
# MAGIC `EST_LABOR_COST`,
# MAGIC `EST_BURDEN_COST`,
# MAGIC `EST_SERVICE_COST`,
# MAGIC `ACT_MATERIAL_COST`,
# MAGIC `ACT_LABOR_COST`,
# MAGIC `ACT_BURDEN_COST`,
# MAGIC `ACT_SERVICE_COST`,
# MAGIC `USER_1`,
# MAGIC `USER_2`,
# MAGIC `USER_3`,
# MAGIC `USER_4`,
# MAGIC `USER_5`,
# MAGIC `USER_6`,
# MAGIC `USER_7`,
# MAGIC `USER_8`,
# MAGIC `USER_9`,
# MAGIC `USER_10`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`WORKORDER_TYPE`,
# MAGIC S.`WORKORDER_BASE_ID`,
# MAGIC S.`WORKORDER_LOT_ID`,
# MAGIC S.`WORKORDER_SPLIT_ID`,
# MAGIC S.`WORKORDER_SUB_ID`,
# MAGIC S.`OPERATION_SEQ_NO`,
# MAGIC S.`PIECE_NO`,
# MAGIC S.`SUBORD_WO_SUB_ID`,
# MAGIC S.`PART_ID`,
# MAGIC S.`STATUS`,
# MAGIC S.`QTY_PER`,
# MAGIC S.`QTY_PER_TYPE`,
# MAGIC S.`FIXED_QTY`,
# MAGIC S.`USAGE_UM`,
# MAGIC S.`EFFECTIVE_DATE`,
# MAGIC S.`DISCONTINUE_DATE`,
# MAGIC S.`CALC_QTY`,
# MAGIC S.`ISSUED_QTY`,
# MAGIC S.`REQUIRED_DATE`,
# MAGIC S.`CLOSE_DATE`,
# MAGIC S.`UNIT_MATERIAL_COST`,
# MAGIC S.`UNIT_LABOR_COST`,
# MAGIC S.`UNIT_BURDEN_COST`,
# MAGIC S.`UNIT_SERVICE_COST`,
# MAGIC S.`BURDEN_PERCENT`,
# MAGIC S.`BURDEN_PER_UNIT`,
# MAGIC S.`FIXED_COST`,
# MAGIC S.`VENDOR_ID`,
# MAGIC S.`VENDOR_PART_ID`,
# MAGIC S.`EST_MATERIAL_COST`,
# MAGIC S.`EST_LABOR_COST`,
# MAGIC S.`EST_BURDEN_COST`,
# MAGIC S.`EST_SERVICE_COST`,
# MAGIC S.`ACT_MATERIAL_COST`,
# MAGIC S.`ACT_LABOR_COST`,
# MAGIC S.`ACT_BURDEN_COST`,
# MAGIC S.`ACT_SERVICE_COST`,
# MAGIC S.`USER_1`,
# MAGIC S.`USER_2`,
# MAGIC S.`USER_3`,
# MAGIC S.`USER_4`,
# MAGIC S.`USER_5`,
# MAGIC S.`USER_6`,
# MAGIC S.`USER_7`,
# MAGIC S.`USER_8`,
# MAGIC S.`USER_9`,
# MAGIC S.`USER_10`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'VE70',
# MAGIC now()
# MAGIC )
# MAGIC   

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


