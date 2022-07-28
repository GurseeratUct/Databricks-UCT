# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'PURC_ORDER_LINE'
read_format = 'csv'
write_format = 'delta'
database_name = 'VE72'
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
StructField('PURC_ORDER_ID',StringType(),True),\
StructField('LINE_NO',IntegerType(),True),\
StructField('PART_ID',StringType(),True),\
StructField('VENDOR_PART_ID',StringType(),True),\
StructField('SERVICE_ID',StringType(),True),\
StructField('USER_ORDER_QTY',DoubleType(),True),\
StructField('ORDER_QTY',DoubleType(),True),\
StructField('PURCHASE_UM',StringType(),True),\
StructField('UNIT_PRICE',DoubleType(),True),\
StructField('GL_EXPENSE_ACCT_ID',StringType(),True),\
StructField('DESIRED_RECV_DATE',StringType(),True),\
StructField('LINE_STATUS',StringType(),True),\
StructField('LAST_RECEIVED_DATE',StringType(),True),\
StructField('TOTAL_USR_RECD_QTY',DoubleType(),True),\
StructField('TOTAL_RECEIVED_QTY',DoubleType(),True),\
StructField('TOTAL_AMT_RECVD',DoubleType(),True),\
StructField('TOTAL_AMT_ORDERED',DoubleType(),True),\
StructField('MFG_NAME',StringType(),True),\
StructField('MFG_PART_ID',StringType(),True),\
StructField('PROMISE_DATE',StringType(),True),\
StructField('VAT_CODE',StringType(),True),\
StructField('USER_1',StringType(),True),\
StructField('USER_2',StringType(),True),\
StructField('USER_3',StringType(),True),\
StructField('USER_4',StringType(),True),\
StructField('VAT_AMOUNT',DoubleType(),True),\
StructField('VAT_RCV_AMOUNT',DoubleType(),True),\
StructField('ORIG_STAGE_REVISION_ID',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
])


# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('VE72'))

# COMMAND ----------

df_transform = df_add_column.na.fill(0).withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
                                        .withColumn("DESIRED_RECV_DATE", to_timestamp(regexp_replace(df_add_column.DESIRED_RECV_DATE,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE72.PURC_ORDER_LINE as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY PURC_ORDER_ID,LINE_NO ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_PURC_ORDER_LINE)A where A.rn = 1 and A.LandingFileTimestamp IS NOT NULL ) as S 
# MAGIC ON T.PURC_ORDER_ID = S.PURC_ORDER_ID and
# MAGIC T.LINE_NO = S.LINE_NO
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`ROWID` =  S.`ROWID`,
# MAGIC T.`PURC_ORDER_ID` =  S.`PURC_ORDER_ID`,
# MAGIC T.`LINE_NO` =  S.`LINE_NO`,
# MAGIC T.`PART_ID` =  S.`PART_ID`,
# MAGIC T.`VENDOR_PART_ID` =  S.`VENDOR_PART_ID`,
# MAGIC T.`SERVICE_ID` =  S.`SERVICE_ID`,
# MAGIC T.`USER_ORDER_QTY` =  S.`USER_ORDER_QTY`,
# MAGIC T.`ORDER_QTY` =  S.`ORDER_QTY`,
# MAGIC T.`PURCHASE_UM` =  S.`PURCHASE_UM`,
# MAGIC T.`UNIT_PRICE` =  S.`UNIT_PRICE`,
# MAGIC T.`GL_EXPENSE_ACCT_ID` =  S.`GL_EXPENSE_ACCT_ID`,
# MAGIC T.`DESIRED_RECV_DATE` =  S.`DESIRED_RECV_DATE`,
# MAGIC T.`LINE_STATUS` =  S.`LINE_STATUS`,
# MAGIC T.`LAST_RECEIVED_DATE` =  S.`LAST_RECEIVED_DATE`,
# MAGIC T.`TOTAL_USR_RECD_QTY` =  S.`TOTAL_USR_RECD_QTY`,
# MAGIC T.`TOTAL_RECEIVED_QTY` =  S.`TOTAL_RECEIVED_QTY`,
# MAGIC T.`TOTAL_AMT_RECVD` =  S.`TOTAL_AMT_RECVD`,
# MAGIC T.`TOTAL_AMT_ORDERED` =  S.`TOTAL_AMT_ORDERED`,
# MAGIC T.`MFG_NAME` =  S.`MFG_NAME`,
# MAGIC T.`MFG_PART_ID` =  S.`MFG_PART_ID`,
# MAGIC T.`PROMISE_DATE` =  S.`PROMISE_DATE`,
# MAGIC T.`VAT_CODE` =  S.`VAT_CODE`,
# MAGIC T.`USER_1` =  S.`USER_1`,
# MAGIC T.`USER_2` =  S.`USER_2`,
# MAGIC T.`USER_3` =  S.`USER_3`,
# MAGIC T.`USER_4` =  S.`USER_4`,
# MAGIC T.`VAT_AMOUNT` =  S.`VAT_AMOUNT`,
# MAGIC T.`VAT_RCV_AMOUNT` =  S.`VAT_RCV_AMOUNT`,
# MAGIC T.`ORIG_STAGE_REVISION_ID` =  S.`ORIG_STAGE_REVISION_ID`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` =  now()
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `ROWID`,
# MAGIC `PURC_ORDER_ID`,
# MAGIC `LINE_NO`,
# MAGIC `PART_ID`,
# MAGIC `VENDOR_PART_ID`,
# MAGIC `SERVICE_ID`,
# MAGIC `USER_ORDER_QTY`,
# MAGIC `ORDER_QTY`,
# MAGIC `PURCHASE_UM`,
# MAGIC `UNIT_PRICE`,
# MAGIC `GL_EXPENSE_ACCT_ID`,
# MAGIC `DESIRED_RECV_DATE`,
# MAGIC `LINE_STATUS`,
# MAGIC `LAST_RECEIVED_DATE`,
# MAGIC `TOTAL_USR_RECD_QTY`,
# MAGIC `TOTAL_RECEIVED_QTY`,
# MAGIC `TOTAL_AMT_RECVD`,
# MAGIC `TOTAL_AMT_ORDERED`,
# MAGIC `MFG_NAME`,
# MAGIC `MFG_PART_ID`,
# MAGIC `PROMISE_DATE`,
# MAGIC `VAT_CODE`,
# MAGIC `USER_1`,
# MAGIC `USER_2`,
# MAGIC `USER_3`,
# MAGIC `USER_4`,
# MAGIC `VAT_AMOUNT`,
# MAGIC `VAT_RCV_AMOUNT`,
# MAGIC `ORIG_STAGE_REVISION_ID`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC  DataSource,
# MAGIC   UpdatedOn
# MAGIC   )
# MAGIC   VALUES
# MAGIC (
# MAGIC S.`ROWID`,
# MAGIC S.`PURC_ORDER_ID`,
# MAGIC S.`LINE_NO`,
# MAGIC S.`PART_ID`,
# MAGIC S.`VENDOR_PART_ID`,
# MAGIC S.`SERVICE_ID`,
# MAGIC S.`USER_ORDER_QTY`,
# MAGIC S.`ORDER_QTY`,
# MAGIC S.`PURCHASE_UM`,
# MAGIC S.`UNIT_PRICE`,
# MAGIC S.`GL_EXPENSE_ACCT_ID`,
# MAGIC S.`DESIRED_RECV_DATE`,
# MAGIC S.`LINE_STATUS`,
# MAGIC S.`LAST_RECEIVED_DATE`,
# MAGIC S.`TOTAL_USR_RECD_QTY`,
# MAGIC S.`TOTAL_RECEIVED_QTY`,
# MAGIC S.`TOTAL_AMT_RECVD`,
# MAGIC S.`TOTAL_AMT_ORDERED`,
# MAGIC S.`MFG_NAME`,
# MAGIC S.`MFG_PART_ID`,
# MAGIC S.`PROMISE_DATE`,
# MAGIC S.`VAT_CODE`,
# MAGIC S.`USER_1`,
# MAGIC S.`USER_2`,
# MAGIC S.`USER_3`,
# MAGIC S.`USER_4`,
# MAGIC S.`VAT_AMOUNT`,
# MAGIC S.`VAT_RCV_AMOUNT`,
# MAGIC S.`ORIG_STAGE_REVISION_ID`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'VE72',
# MAGIC now()
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from ve72.purc_order_line limit 5;
# MAGIC show columns in purc_order_line in ve72;

# COMMAND ----------


