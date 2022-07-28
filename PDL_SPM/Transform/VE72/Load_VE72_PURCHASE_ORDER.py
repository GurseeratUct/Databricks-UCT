# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'PURCHASE_ORDER'
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
StructField('ID',StringType(),True),\
StructField('VENDOR_ID',StringType(),True),\
StructField('PURC_ORD_ADDR_NO',DoubleType(),True),\
StructField('SHIPTO_ADDR_NO',DoubleType(),True),\
StructField('ORDER_DATE',StringType(),True),\
StructField('DESIRED_RECV_DATE',StringType(),True),\
StructField('BUYER',StringType(),True),\
StructField('FREE_ON_BOARD',StringType(),True),\
StructField('SHIP_VIA',StringType(),True),\
StructField('STATUS',StringType(),True),\
StructField('SELL_RATE',DoubleType(),True),\
StructField('BUY_RATE',StringType(),True),\
StructField('ENTITY_ID',StringType(),True),\
StructField('TOTAL_AMT_ORDERED',DoubleType(),True),\
StructField('PROMISE_DATE',StringType(),True),\
StructField('SHIPTO_ID',StringType(),True),\
StructField('TERMS_NET_TYPE',StringType(),True),\
StructField('TERMS_NET_DAYS',StringType(),True),\
StructField('TERMS_NET_DATE',StringType(),True),\
StructField('TERMS_DISC_TYPE',StringType(),True),\
StructField('TERMS_DISC_DAYS',StringType(),True),\
StructField('TERMS_DISC_DATE',StringType(),True),\
StructField('TERMS_DISC_PERCENT',DoubleType(),True),\
StructField('TERMS_DESCRIPTION',StringType(),True),\
StructField('CURRENCY_ID',StringType(),True),\
StructField('CREATE_DATE',StringType(),True),\
StructField('SHIPFROM_ID',StringType(),True),\
StructField('UCT_ASSOCIATED_CO',StringType(),True),\
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
                                        .withColumn("ORDER_DATE", to_timestamp(regexp_replace(df_add_column.ORDER_DATE,'\.','-'))) \
                                        .withColumn("DESIRED_RECV_DATE", to_timestamp(regexp_replace(df_add_column.DESIRED_RECV_DATE,'\.','-'))) \
                                        .withColumn("CREATE_DATE", to_timestamp(regexp_replace(df_add_column.CREATE_DATE,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE70.PURCHASE_ORDER as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY ID ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_PURCHASE_ORDER)A where A.rn = 1 ) as S 
# MAGIC ON T.ID = S.ID 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`ROWID` =  S.`ROWID`,
# MAGIC T.`ID` =  S.`ID`,
# MAGIC T.`VENDOR_ID` =  S.`VENDOR_ID`,
# MAGIC T.`PURC_ORD_ADDR_NO` =  S.`PURC_ORD_ADDR_NO`,
# MAGIC T.`SHIPTO_ADDR_NO` =  S.`SHIPTO_ADDR_NO`,
# MAGIC T.`ORDER_DATE` =  S.`ORDER_DATE`,
# MAGIC T.`DESIRED_RECV_DATE` =  S.`DESIRED_RECV_DATE`,
# MAGIC T.`BUYER` =  S.`BUYER`,
# MAGIC T.`FREE_ON_BOARD` =  S.`FREE_ON_BOARD`,
# MAGIC T.`SHIP_VIA` =  S.`SHIP_VIA`,
# MAGIC T.`STATUS` =  S.`STATUS`,
# MAGIC T.`SELL_RATE` =  S.`SELL_RATE`,
# MAGIC T.`BUY_RATE` =  S.`BUY_RATE`,
# MAGIC T.`ENTITY_ID` =  S.`ENTITY_ID`,
# MAGIC T.`TOTAL_AMT_ORDERED` =  S.`TOTAL_AMT_ORDERED`,
# MAGIC T.`PROMISE_DATE` =  S.`PROMISE_DATE`,
# MAGIC T.`SHIPTO_ID` =  S.`SHIPTO_ID`,
# MAGIC T.`TERMS_NET_TYPE` =  S.`TERMS_NET_TYPE`,
# MAGIC T.`TERMS_NET_DAYS` =  S.`TERMS_NET_DAYS`,
# MAGIC T.`TERMS_NET_DATE` =  S.`TERMS_NET_DATE`,
# MAGIC T.`TERMS_DISC_TYPE` =  S.`TERMS_DISC_TYPE`,
# MAGIC T.`TERMS_DISC_DAYS` =  S.`TERMS_DISC_DAYS`,
# MAGIC T.`TERMS_DISC_DATE` =  S.`TERMS_DISC_DATE`,
# MAGIC T.`TERMS_DISC_PERCENT` =  S.`TERMS_DISC_PERCENT`,
# MAGIC T.`TERMS_DESCRIPTION` =  S.`TERMS_DESCRIPTION`,
# MAGIC T.`CURRENCY_ID` =  S.`CURRENCY_ID`,
# MAGIC T.`CREATE_DATE` =  S.`CREATE_DATE`,
# MAGIC T.`SHIPFROM_ID` =  S.`SHIPFROM_ID`,
# MAGIC T.`UCT_ASSOCIATED_CO` =  S.`UCT_ASSOCIATED_CO`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` =  now()
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `ROWID`,
# MAGIC `ID`,
# MAGIC `VENDOR_ID`,
# MAGIC `PURC_ORD_ADDR_NO`,
# MAGIC `SHIPTO_ADDR_NO`,
# MAGIC `ORDER_DATE`,
# MAGIC `DESIRED_RECV_DATE`,
# MAGIC `BUYER`,
# MAGIC `FREE_ON_BOARD`,
# MAGIC `SHIP_VIA`,
# MAGIC `STATUS`,
# MAGIC `SELL_RATE`,
# MAGIC `BUY_RATE`,
# MAGIC `ENTITY_ID`,
# MAGIC `TOTAL_AMT_ORDERED`,
# MAGIC `PROMISE_DATE`,
# MAGIC `SHIPTO_ID`,
# MAGIC `TERMS_NET_TYPE`,
# MAGIC `TERMS_NET_DAYS`,
# MAGIC `TERMS_NET_DATE`,
# MAGIC `TERMS_DISC_TYPE`,
# MAGIC `TERMS_DISC_DAYS`,
# MAGIC `TERMS_DISC_DATE`,
# MAGIC `TERMS_DISC_PERCENT`,
# MAGIC `TERMS_DESCRIPTION`,
# MAGIC `CURRENCY_ID`,
# MAGIC `CREATE_DATE`,
# MAGIC `SHIPFROM_ID`,
# MAGIC `UCT_ASSOCIATED_CO`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC  DataSource,
# MAGIC   UpdatedOn
# MAGIC     )
# MAGIC   VALUES
# MAGIC (
# MAGIC S.`ROWID`,
# MAGIC S.`ID`,
# MAGIC S.`VENDOR_ID`,
# MAGIC S.`PURC_ORD_ADDR_NO`,
# MAGIC S.`SHIPTO_ADDR_NO`,
# MAGIC S.`ORDER_DATE`,
# MAGIC S.`DESIRED_RECV_DATE`,
# MAGIC S.`BUYER`,
# MAGIC S.`FREE_ON_BOARD`,
# MAGIC S.`SHIP_VIA`,
# MAGIC S.`STATUS`,
# MAGIC S.`SELL_RATE`,
# MAGIC S.`BUY_RATE`,
# MAGIC S.`ENTITY_ID`,
# MAGIC S.`TOTAL_AMT_ORDERED`,
# MAGIC S.`PROMISE_DATE`,
# MAGIC S.`SHIPTO_ID`,
# MAGIC S.`TERMS_NET_TYPE`,
# MAGIC S.`TERMS_NET_DAYS`,
# MAGIC S.`TERMS_NET_DATE`,
# MAGIC S.`TERMS_DISC_TYPE`,
# MAGIC S.`TERMS_DISC_DAYS`,
# MAGIC S.`TERMS_DISC_DATE`,
# MAGIC S.`TERMS_DISC_PERCENT`,
# MAGIC S.`TERMS_DESCRIPTION`,
# MAGIC S.`CURRENCY_ID`,
# MAGIC S.`CREATE_DATE`,
# MAGIC S.`SHIPFROM_ID`,
# MAGIC S.`UCT_ASSOCIATED_CO`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'VE72',
# MAGIC now()
# MAGIC )

# COMMAND ----------


