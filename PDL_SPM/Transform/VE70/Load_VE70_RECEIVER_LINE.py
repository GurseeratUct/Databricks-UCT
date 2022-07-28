# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'RECEIVER_LINE'
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
StructField('ROWID',IntegerType(),True),\
StructField('RECEIVER_ID',StringType(),True),\
StructField('LINE_NO',IntegerType(),True),\
StructField('PURC_ORDER_ID',StringType(),True),\
StructField('PURC_ORDER_LINE_NO',IntegerType(),True),\
StructField('USER_RECEIVED_QTY',DoubleType(),True),\
StructField('RECEIVED_QTY',DoubleType(),True),\
StructField('INSPECT_QTY',DoubleType(),True),\
StructField('ACT_FREIGHT',DoubleType(),True),\
StructField('INVOICE_ID',StringType(),True),\
StructField('INVOICED_DATE',StringType(),True),\
StructField('WAREHOUSE_ID',StringType(),True),\
StructField('LOCATION_ID',StringType(),True),\
StructField('TRANSACTION_ID',IntegerType(),True),\
StructField('SERV_TRANS_ID',IntegerType(),True),\
StructField('PIECE_COUNT',DoubleType(),True),\
StructField('LENGTH',StringType(),True),\
StructField('WIDTH',StringType(),True),\
StructField('HEIGHT',StringType(),True),\
StructField('DIMENSIONS_UM',StringType(),True),\
StructField('REJECTED_QTY',StringType(),True),\
StructField('HTS_CODE',StringType(),True),\
StructField('ORIG_COUNTRY_ID',StringType(),True),\
StructField('UNIT_PRICE',DoubleType(),True),\
StructField('LANDED_UNIT_PRICE',DoubleType(),True),\
StructField('FIXED_CHARGE',DoubleType(),True),\
StructField('INTRASTAT_AMOUNT',StringType(),True),\
StructField('DATA_COLL_COMP',StringType(),True),\
StructField('GROSS_WEIGHT',StringType(),True),\
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
                                        .withColumn("INVOICED_DATE", to_timestamp(regexp_replace(df_add_column.INVOICED_DATE,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE70.RECEIVER_LINE as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY RECEIVER_ID,LINE_NO ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_RECEIVER_LINE)A where A.rn = 1 ) as S 
# MAGIC ON T.RECEIVER_ID = S.RECEIVER_ID and
# MAGIC T.LINE_NO = S.LINE_NO
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`ROWID` =  S.`ROWID`,
# MAGIC T.`RECEIVER_ID` =  S.`RECEIVER_ID`,
# MAGIC T.`LINE_NO` =  S.`LINE_NO`,
# MAGIC T.`PURC_ORDER_ID` =  S.`PURC_ORDER_ID`,
# MAGIC T.`PURC_ORDER_LINE_NO` =  S.`PURC_ORDER_LINE_NO`,
# MAGIC T.`USER_RECEIVED_QTY` =  S.`USER_RECEIVED_QTY`,
# MAGIC T.`RECEIVED_QTY` =  S.`RECEIVED_QTY`,
# MAGIC T.`INSPECT_QTY` =  S.`INSPECT_QTY`,
# MAGIC T.`ACT_FREIGHT` =  S.`ACT_FREIGHT`,
# MAGIC T.`INVOICE_ID` =  S.`INVOICE_ID`,
# MAGIC T.`INVOICED_DATE` =  S.`INVOICED_DATE`,
# MAGIC T.`WAREHOUSE_ID` =  S.`WAREHOUSE_ID`,
# MAGIC T.`LOCATION_ID` =  S.`LOCATION_ID`,
# MAGIC T.`TRANSACTION_ID` =  S.`TRANSACTION_ID`,
# MAGIC T.`SERV_TRANS_ID` =  S.`SERV_TRANS_ID`,
# MAGIC T.`PIECE_COUNT` =  S.`PIECE_COUNT`,
# MAGIC T.`LENGTH` =  S.`LENGTH`,
# MAGIC T.`WIDTH` =  S.`WIDTH`,
# MAGIC T.`HEIGHT` =  S.`HEIGHT`,
# MAGIC T.`DIMENSIONS_UM` =  S.`DIMENSIONS_UM`,
# MAGIC T.`REJECTED_QTY` =  S.`REJECTED_QTY`,
# MAGIC T.`HTS_CODE` =  S.`HTS_CODE`,
# MAGIC T.`ORIG_COUNTRY_ID` =  S.`ORIG_COUNTRY_ID`,
# MAGIC T.`UNIT_PRICE` =  S.`UNIT_PRICE`,
# MAGIC T.`LANDED_UNIT_PRICE` =  S.`LANDED_UNIT_PRICE`,
# MAGIC T.`FIXED_CHARGE` =  S.`FIXED_CHARGE`,
# MAGIC T.`INTRASTAT_AMOUNT` =  S.`INTRASTAT_AMOUNT`,
# MAGIC T.`DATA_COLL_COMP` =  S.`DATA_COLL_COMP`,
# MAGIC T.`GROSS_WEIGHT` =  S.`GROSS_WEIGHT`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` =  now()
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC  `ROWID`,
# MAGIC `RECEIVER_ID`,
# MAGIC `LINE_NO`,
# MAGIC `PURC_ORDER_ID`,
# MAGIC `PURC_ORDER_LINE_NO`,
# MAGIC `USER_RECEIVED_QTY`,
# MAGIC `RECEIVED_QTY`,
# MAGIC `INSPECT_QTY`,
# MAGIC `ACT_FREIGHT`,
# MAGIC `INVOICE_ID`,
# MAGIC `INVOICED_DATE`,
# MAGIC `WAREHOUSE_ID`,
# MAGIC `LOCATION_ID`,
# MAGIC `TRANSACTION_ID`,
# MAGIC `SERV_TRANS_ID`,
# MAGIC `PIECE_COUNT`,
# MAGIC `LENGTH`,
# MAGIC `WIDTH`,
# MAGIC `HEIGHT`,
# MAGIC `DIMENSIONS_UM`,
# MAGIC `REJECTED_QTY`,
# MAGIC `HTS_CODE`,
# MAGIC `ORIG_COUNTRY_ID`,
# MAGIC `UNIT_PRICE`,
# MAGIC `LANDED_UNIT_PRICE`,
# MAGIC `FIXED_CHARGE`,
# MAGIC `INTRASTAT_AMOUNT`,
# MAGIC `DATA_COLL_COMP`,
# MAGIC `GROSS_WEIGHT`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC  DataSource,
# MAGIC   UpdatedOn
# MAGIC   )
# MAGIC   VALUES
# MAGIC (
# MAGIC S.`ROWID`,
# MAGIC S.`RECEIVER_ID`,
# MAGIC S.`LINE_NO`,
# MAGIC S.`PURC_ORDER_ID`,
# MAGIC S.`PURC_ORDER_LINE_NO`,
# MAGIC S.`USER_RECEIVED_QTY`,
# MAGIC S.`RECEIVED_QTY`,
# MAGIC S.`INSPECT_QTY`,
# MAGIC S.`ACT_FREIGHT`,
# MAGIC S.`INVOICE_ID`,
# MAGIC S.`INVOICED_DATE`,
# MAGIC S.`WAREHOUSE_ID`,
# MAGIC S.`LOCATION_ID`,
# MAGIC S.`TRANSACTION_ID`,
# MAGIC S.`SERV_TRANS_ID`,
# MAGIC S.`PIECE_COUNT`,
# MAGIC S.`LENGTH`,
# MAGIC S.`WIDTH`,
# MAGIC S.`HEIGHT`,
# MAGIC S.`DIMENSIONS_UM`,
# MAGIC S.`REJECTED_QTY`,
# MAGIC S.`HTS_CODE`,
# MAGIC S.`ORIG_COUNTRY_ID`,
# MAGIC S.`UNIT_PRICE`,
# MAGIC S.`LANDED_UNIT_PRICE`,
# MAGIC S.`FIXED_CHARGE`,
# MAGIC S.`INTRASTAT_AMOUNT`,
# MAGIC S.`DATA_COLL_COMP`,
# MAGIC S.`GROSS_WEIGHT`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'VE70',
# MAGIC now()
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


