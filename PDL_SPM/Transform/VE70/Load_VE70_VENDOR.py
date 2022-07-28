# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'VENDOR'
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
StructField('ROWID',StringType(),True),\
StructField('ID',StringType(),True),\
StructField('NAME',StringType(),True),\
StructField('ADDR_1',StringType(),True),\
StructField('ADDR_2',StringType(),True),\
StructField('ADDR_3',StringType(),True),\
StructField('CITY',StringType(),True),\
StructField('STATE',StringType(),True),\
StructField('ZIPCODE',StringType(),True),\
StructField('COUNTRY',StringType(),True),\
StructField('CONTACT_FIRST_NAME',StringType(),True),\
StructField('CONTACT_LAST_NAME',StringType(),True),\
StructField('CONTACT_PHONE',StringType(),True),\
StructField('CONTACT_FAX',StringType(),True),\
StructField('REMIT_TO_NAME',StringType(),True),\
StructField('REMIT_TO_ADDR_1',StringType(),True),\
StructField('REMIT_TO_ADDR_2',StringType(),True),\
StructField('REMIT_TO_ADDR_3',StringType(),True),\
StructField('REMIT_TO_CITY',StringType(),True),\
StructField('REMIT_TO_STATE',StringType(),True),\
StructField('REMIT_TO_ZIPCODE',StringType(),True),\
StructField('REMIT_TO_COUNTRY',StringType(),True),\
StructField('FREE_ON_BOARD',StringType(),True),\
StructField('SHIP_VIA',StringType(),True),\
StructField('BUYER',StringType(),True),\
StructField('REPORT_1099_MISC',StringType(),True),\
StructField('TERMS_NET_TYPE',StringType(),True),\
StructField('TERMS_NET_DAYS',StringType(),True),\
StructField('TERMS_NET_DATE',StringType(),True),\
StructField('TERMS_DISC_TYPE',StringType(),True),\
StructField('TERMS_DISC_DAYS',StringType(),True),\
StructField('TERMS_DISC_DATE',StringType(),True),\
StructField('TERMS_DISC_PERCENT',DoubleType(),True),\
StructField('TERMS_DESCRIPTION',StringType(),True),\
StructField('CURRENCY_ID',StringType(),True),\
StructField('TAX_ID_NUMBER',StringType(),True),\
StructField('ENTITY_ID',StringType(),True),\
StructField('LAST_ORDER_DATE',StringType(),True),\
StructField('OPEN_DATE',StringType(),True),\
StructField('MODIFY_DATE',StringType(),True),\
StructField('COUNTRY_ID',StringType(),True),\
StructField('VAT_EXEMPT',StringType(),True),\
StructField('PAYMENT_METHOD',StringType(),True),\
StructField('CONTACT_EMAIL',StringType(),True),\
StructField('SHIPTO_ID',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True) \
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
                                       .withColumn("LAST_ORDER_DATE", to_timestamp(regexp_replace(df_add_column.LAST_ORDER_DATE,'\.','-'))) \
                                        .withColumn("OPEN_DATE", to_timestamp(regexp_replace(df_add_column.OPEN_DATE,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE70.VENDOR as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY ID ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_VENDOR)A where A.rn = 1) as S 
# MAGIC ON T.ID = S.ID 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`ROWID` =  S.`ROWID`,
# MAGIC T.`ID` =  S.`ID`,
# MAGIC T.`NAME` =  S.`NAME`,
# MAGIC T.`ADDR_1` =  S.`ADDR_1`,
# MAGIC T.`ADDR_2` =  S.`ADDR_2`,
# MAGIC T.`ADDR_3` =  S.`ADDR_3`,
# MAGIC T.`CITY` =  S.`CITY`,
# MAGIC T.`STATE` =  S.`STATE`,
# MAGIC T.`ZIPCODE` =  S.`ZIPCODE`,
# MAGIC T.`COUNTRY` =  S.`COUNTRY`,
# MAGIC T.`CONTACT_FIRST_NAME` =  S.`CONTACT_FIRST_NAME`,
# MAGIC T.`CONTACT_LAST_NAME` =  S.`CONTACT_LAST_NAME`,
# MAGIC T.`CONTACT_PHONE` =  S.`CONTACT_PHONE`,
# MAGIC T.`CONTACT_FAX` =  S.`CONTACT_FAX`,
# MAGIC T.`REMIT_TO_NAME` =  S.`REMIT_TO_NAME`,
# MAGIC T.`REMIT_TO_ADDR_1` =  S.`REMIT_TO_ADDR_1`,
# MAGIC T.`REMIT_TO_ADDR_2` =  S.`REMIT_TO_ADDR_2`,
# MAGIC T.`REMIT_TO_ADDR_3` =  S.`REMIT_TO_ADDR_3`,
# MAGIC T.`REMIT_TO_CITY` =  S.`REMIT_TO_CITY`,
# MAGIC T.`REMIT_TO_STATE` =  S.`REMIT_TO_STATE`,
# MAGIC T.`REMIT_TO_ZIPCODE` =  S.`REMIT_TO_ZIPCODE`,
# MAGIC T.`REMIT_TO_COUNTRY` =  S.`REMIT_TO_COUNTRY`,
# MAGIC T.`FREE_ON_BOARD` =  S.`FREE_ON_BOARD`,
# MAGIC T.`SHIP_VIA` =  S.`SHIP_VIA`,
# MAGIC T.`BUYER` =  S.`BUYER`,
# MAGIC T.`REPORT_1099_MISC` =  S.`REPORT_1099_MISC`,
# MAGIC T.`TERMS_NET_TYPE` =  S.`TERMS_NET_TYPE`,
# MAGIC T.`TERMS_NET_DAYS` =  S.`TERMS_NET_DAYS`,
# MAGIC T.`TERMS_NET_DATE` =  S.`TERMS_NET_DATE`,
# MAGIC T.`TERMS_DISC_TYPE` =  S.`TERMS_DISC_TYPE`,
# MAGIC T.`TERMS_DISC_DAYS` =  S.`TERMS_DISC_DAYS`,
# MAGIC T.`TERMS_DISC_DATE` =  S.`TERMS_DISC_DATE`,
# MAGIC T.`TERMS_DISC_PERCENT` =  S.`TERMS_DISC_PERCENT`,
# MAGIC T.`TERMS_DESCRIPTION` =  S.`TERMS_DESCRIPTION`,
# MAGIC T.`CURRENCY_ID` =  S.`CURRENCY_ID`,
# MAGIC T.`TAX_ID_NUMBER` =  S.`TAX_ID_NUMBER`,
# MAGIC T.`ENTITY_ID` =  S.`ENTITY_ID`,
# MAGIC T.`LAST_ORDER_DATE` =  S.`LAST_ORDER_DATE`,
# MAGIC T.`OPEN_DATE` =  S.`OPEN_DATE`,
# MAGIC T.`MODIFY_DATE` =  S.`MODIFY_DATE`,
# MAGIC T.`COUNTRY_ID` =  S.`COUNTRY_ID`,
# MAGIC T.`VAT_EXEMPT` =  S.`VAT_EXEMPT`,
# MAGIC T.`PAYMENT_METHOD` =  S.`PAYMENT_METHOD`,
# MAGIC T.`CONTACT_EMAIL` =  S.`CONTACT_EMAIL`,
# MAGIC T.`SHIPTO_ID` =  S.`SHIPTO_ID`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` =  now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `ROWID`,
# MAGIC `ID`,
# MAGIC `NAME`,
# MAGIC `ADDR_1`,
# MAGIC `ADDR_2`,
# MAGIC `ADDR_3`,
# MAGIC `CITY`,
# MAGIC `STATE`,
# MAGIC `ZIPCODE`,
# MAGIC `COUNTRY`,
# MAGIC `CONTACT_FIRST_NAME`,
# MAGIC `CONTACT_LAST_NAME`,
# MAGIC `CONTACT_PHONE`,
# MAGIC `CONTACT_FAX`,
# MAGIC `REMIT_TO_NAME`,
# MAGIC `REMIT_TO_ADDR_1`,
# MAGIC `REMIT_TO_ADDR_2`,
# MAGIC `REMIT_TO_ADDR_3`,
# MAGIC `REMIT_TO_CITY`,
# MAGIC `REMIT_TO_STATE`,
# MAGIC `REMIT_TO_ZIPCODE`,
# MAGIC `REMIT_TO_COUNTRY`,
# MAGIC `FREE_ON_BOARD`,
# MAGIC `SHIP_VIA`,
# MAGIC `BUYER`,
# MAGIC `REPORT_1099_MISC`,
# MAGIC `TERMS_NET_TYPE`,
# MAGIC `TERMS_NET_DAYS`,
# MAGIC `TERMS_NET_DATE`,
# MAGIC `TERMS_DISC_TYPE`,
# MAGIC `TERMS_DISC_DAYS`,
# MAGIC `TERMS_DISC_DATE`,
# MAGIC `TERMS_DISC_PERCENT`,
# MAGIC `TERMS_DESCRIPTION`,
# MAGIC `CURRENCY_ID`,
# MAGIC `TAX_ID_NUMBER`,
# MAGIC `ENTITY_ID`,
# MAGIC `LAST_ORDER_DATE`,
# MAGIC `OPEN_DATE`,
# MAGIC `MODIFY_DATE`,
# MAGIC `COUNTRY_ID`,
# MAGIC `VAT_EXEMPT`,
# MAGIC `PAYMENT_METHOD`,
# MAGIC `CONTACT_EMAIL`,
# MAGIC `SHIPTO_ID`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC `ROWID`,
# MAGIC `ID`,
# MAGIC `NAME`,
# MAGIC `ADDR_1`,
# MAGIC `ADDR_2`,
# MAGIC `ADDR_3`,
# MAGIC `CITY`,
# MAGIC `STATE`,
# MAGIC `ZIPCODE`,
# MAGIC `COUNTRY`,
# MAGIC `CONTACT_FIRST_NAME`,
# MAGIC `CONTACT_LAST_NAME`,
# MAGIC `CONTACT_PHONE`,
# MAGIC `CONTACT_FAX`,
# MAGIC `REMIT_TO_NAME`,
# MAGIC `REMIT_TO_ADDR_1`,
# MAGIC `REMIT_TO_ADDR_2`,
# MAGIC `REMIT_TO_ADDR_3`,
# MAGIC `REMIT_TO_CITY`,
# MAGIC `REMIT_TO_STATE`,
# MAGIC `REMIT_TO_ZIPCODE`,
# MAGIC `REMIT_TO_COUNTRY`,
# MAGIC `FREE_ON_BOARD`,
# MAGIC `SHIP_VIA`,
# MAGIC `BUYER`,
# MAGIC `REPORT_1099_MISC`,
# MAGIC `TERMS_NET_TYPE`,
# MAGIC `TERMS_NET_DAYS`,
# MAGIC `TERMS_NET_DATE`,
# MAGIC `TERMS_DISC_TYPE`,
# MAGIC `TERMS_DISC_DAYS`,
# MAGIC `TERMS_DISC_DATE`,
# MAGIC `TERMS_DISC_PERCENT`,
# MAGIC `TERMS_DESCRIPTION`,
# MAGIC `CURRENCY_ID`,
# MAGIC `TAX_ID_NUMBER`,
# MAGIC `ENTITY_ID`,
# MAGIC `LAST_ORDER_DATE`,
# MAGIC `OPEN_DATE`,
# MAGIC `MODIFY_DATE`,
# MAGIC `COUNTRY_ID`,
# MAGIC `VAT_EXEMPT`,
# MAGIC `PAYMENT_METHOD`,
# MAGIC `CONTACT_EMAIL`,
# MAGIC `SHIPTO_ID`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC 'VE70',
# MAGIC now()
# MAGIC )
# MAGIC     

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


