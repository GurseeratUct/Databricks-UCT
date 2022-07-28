# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'WORK_ORDER'
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
StructField('TYPE',StringType(),True),\
StructField('BASE_ID',StringType(),True),\
StructField('LOT_ID',StringType(),True),\
StructField('SPLIT_ID',IntegerType(),True),\
StructField('SUB_ID',IntegerType(),True),\
StructField('PART_ID',StringType(),True),\
StructField('GLOBAL_RANK',IntegerType(),True),\
StructField('DESIRED_QTY',DoubleType(),True),\
StructField('RECEIVED_QTY',DoubleType(),True),\
StructField('CREATE_DATE',StringType(),True),\
StructField('DESIRED_RLS_DATE',StringType(),True),\
StructField('DESIRED_WANT_DATE',StringType(),True),\
StructField('CLOSE_DATE',StringType(),True),\
StructField('COSTED_DATE',StringType(),True),\
StructField('STATUS',StringType(),True),\
StructField('DRAWING_ID',StringType(),True),\
StructField('DRAWING_REV_NO',StringType(),True),\
StructField('ENTITY_ID',StringType(),True),\
StructField('EST_MATERIAL_COST',DoubleType(),True),\
StructField('EST_LABOR_COST',DoubleType(),True),\
StructField('EST_BURDEN_COST',DoubleType(),True),\
StructField('EST_SERVICE_COST',DoubleType(),True),\
StructField('ACT_MATERIAL_COST',DoubleType(),True),\
StructField('ACT_LABOR_COST',DoubleType(),True),\
StructField('ACT_BURDEN_COST',DoubleType(),True),\
StructField('ACT_SERVICE_COST',DoubleType(),True),\
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
StructField('ORIG_STAGE_REVISION_ID',StringType(),True),\
StructField('USER_ID',StringType(),True),\
StructField('UCT_ORIG_REV',StringType(),True),\
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
                                       .withColumn("CREATE_DATE", to_timestamp(regexp_replace(df_add_column.CREATE_DATE,'\.','-'))) \
                                        .withColumn("CLOSE_DATE", to_timestamp(regexp_replace(df_add_column.CLOSE_DATE,'\.','-'))) \
                                        .withColumn("DESIRED_RLS_DATE", to_timestamp(regexp_replace(df_add_column.DESIRED_RLS_DATE,'\.','-'))) \
                                        .withColumn("DESIRED_WANT_DATE", to_timestamp(regexp_replace(df_add_column.DESIRED_WANT_DATE,'\.','-')))


# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE70.WORK_ORDER as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY TYPE,BASE_ID,LOT_ID,SPLIT_ID,SUB_ID ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_WORK_ORDER)A where A.rn = 1) as S 
# MAGIC ON T.`TYPE`	 = S.`TYPE`
# MAGIC and T.`BASE_ID`	 = S.`BASE_ID`
# MAGIC and T.`LOT_ID`	 = S.`LOT_ID`
# MAGIC and T.`SPLIT_ID`	 = S.`SPLIT_ID`
# MAGIC and T.`SUB_ID`	 = S.`SUB_ID`
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`TYPE`	 = S.`TYPE`,
# MAGIC T.`BASE_ID`	 = S.`BASE_ID`,
# MAGIC T.`LOT_ID`	 = S.`LOT_ID`,
# MAGIC T.`SPLIT_ID`	 = S.`SPLIT_ID`,
# MAGIC T.`SUB_ID`	 = S.`SUB_ID`,
# MAGIC T.`PART_ID`	 = S.`PART_ID`,
# MAGIC T.`GLOBAL_RANK`	 = S.`GLOBAL_RANK`,
# MAGIC T.`DESIRED_QTY`	 = S.`DESIRED_QTY`,
# MAGIC T.`RECEIVED_QTY`	 = S.`RECEIVED_QTY`,
# MAGIC T.`CREATE_DATE`	 = S.`CREATE_DATE`,
# MAGIC T.`DESIRED_RLS_DATE`	 = S.`DESIRED_RLS_DATE`,
# MAGIC T.`DESIRED_WANT_DATE`	 = S.`DESIRED_WANT_DATE`,
# MAGIC T.`CLOSE_DATE`	 = S.`CLOSE_DATE`,
# MAGIC T.`COSTED_DATE`	 = S.`COSTED_DATE`,
# MAGIC T.`STATUS`	 = S.`STATUS`,
# MAGIC T.`DRAWING_ID`	 = S.`DRAWING_ID`,
# MAGIC T.`DRAWING_REV_NO`	 = S.`DRAWING_REV_NO`,
# MAGIC T.`ENTITY_ID`	 = S.`ENTITY_ID`,
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
# MAGIC T.`ORIG_STAGE_REVISION_ID`	 = S.`ORIG_STAGE_REVISION_ID`,
# MAGIC T.`USER_ID`	 = S.`USER_ID`,
# MAGIC T.`UCT_ORIG_REV`	 = S.`UCT_ORIG_REV`,
# MAGIC T.`LandingFileTimeStamp`	 = S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` =  now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `TYPE`,
# MAGIC `BASE_ID`,
# MAGIC `LOT_ID`,
# MAGIC `SPLIT_ID`,
# MAGIC `SUB_ID`,
# MAGIC `PART_ID`,
# MAGIC `GLOBAL_RANK`,
# MAGIC `DESIRED_QTY`,
# MAGIC `RECEIVED_QTY`,
# MAGIC `CREATE_DATE`,
# MAGIC `DESIRED_RLS_DATE`,
# MAGIC `DESIRED_WANT_DATE`,
# MAGIC `CLOSE_DATE`,
# MAGIC `COSTED_DATE`,
# MAGIC `STATUS`,
# MAGIC `DRAWING_ID`,
# MAGIC `DRAWING_REV_NO`,
# MAGIC `ENTITY_ID`,
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
# MAGIC `ORIG_STAGE_REVISION_ID`,
# MAGIC `USER_ID`,
# MAGIC `UCT_ORIG_REV`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`TYPE`,
# MAGIC S.`BASE_ID`,
# MAGIC S.`LOT_ID`,
# MAGIC S.`SPLIT_ID`,
# MAGIC S.`SUB_ID`,
# MAGIC S.`PART_ID`,
# MAGIC S.`GLOBAL_RANK`,
# MAGIC S.`DESIRED_QTY`,
# MAGIC S.`RECEIVED_QTY`,
# MAGIC S.`CREATE_DATE`,
# MAGIC S.`DESIRED_RLS_DATE`,
# MAGIC S.`DESIRED_WANT_DATE`,
# MAGIC S.`CLOSE_DATE`,
# MAGIC S.`COSTED_DATE`,
# MAGIC S.`STATUS`,
# MAGIC S.`DRAWING_ID`,
# MAGIC S.`DRAWING_REV_NO`,
# MAGIC S.`ENTITY_ID`,
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
# MAGIC S.`ORIG_STAGE_REVISION_ID`,
# MAGIC S.`USER_ID`,
# MAGIC S.`UCT_ORIG_REV`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'VE70',
# MAGIC now()
# MAGIC )
# MAGIC   

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


