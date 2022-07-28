# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'RECEIVER'
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
StructField('ID',StringType(),True),\
StructField('PURC_ORDER_ID',StringType(),True),\
StructField('RECEIVED_DATE',StringType(),True),\
StructField('CREATE_DATE',StringType(),True),\
StructField('USER_ID',StringType(),True),\
StructField('MARKED_FOR_PURGE',StringType(),True),\
StructField('SHIP_REASON_CD',StringType(),True),\
StructField('CARRIER_ID',StringType(),True),\
StructField('BOL_ID',StringType(),True),\
StructField('UCT_ASSOCIATED_SH',StringType(),True),\
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
                                        .withColumn("RECEIVED_DATE", to_timestamp(regexp_replace(df_add_column.RECEIVED_DATE,'\.','-'))) \
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
# MAGIC MERGE INTO VE70.RECEIVER as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY ID ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_RECEIVER)A where A.rn = 1 ) as S 
# MAGIC ON T.ID = S.ID
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`ROWID` =  S.`ROWID`,
# MAGIC T.`ID` =  S.`ID`,
# MAGIC T.`PURC_ORDER_ID` =  S.`PURC_ORDER_ID`,
# MAGIC T.`RECEIVED_DATE` =  S.`RECEIVED_DATE`,
# MAGIC T.`CREATE_DATE` =  S.`CREATE_DATE`,
# MAGIC T.`USER_ID` =  S.`USER_ID`,
# MAGIC T.`MARKED_FOR_PURGE` =  S.`MARKED_FOR_PURGE`,
# MAGIC T.`SHIP_REASON_CD` =  S.`SHIP_REASON_CD`,
# MAGIC T.`CARRIER_ID` =  S.`CARRIER_ID`,
# MAGIC T.`BOL_ID` =  S.`BOL_ID`,
# MAGIC T.`UCT_ASSOCIATED_SH` =  S.`UCT_ASSOCIATED_SH`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` =  now()
# MAGIC  WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `ROWID`,
# MAGIC `ID`,
# MAGIC `PURC_ORDER_ID`,
# MAGIC `RECEIVED_DATE`,
# MAGIC `CREATE_DATE`,
# MAGIC `USER_ID`,
# MAGIC `MARKED_FOR_PURGE`,
# MAGIC `SHIP_REASON_CD`,
# MAGIC `CARRIER_ID`,
# MAGIC `BOL_ID`,
# MAGIC `UCT_ASSOCIATED_SH`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC  DataSource,
# MAGIC   UpdatedOn
# MAGIC     )
# MAGIC   VALUES
# MAGIC (
# MAGIC S.`ROWID`,
# MAGIC S.`ID`,
# MAGIC S.`PURC_ORDER_ID`,
# MAGIC S.`RECEIVED_DATE`,
# MAGIC S.`CREATE_DATE`,
# MAGIC S.`USER_ID`,
# MAGIC S.`MARKED_FOR_PURGE`,
# MAGIC S.`SHIP_REASON_CD`,
# MAGIC S.`CARRIER_ID`,
# MAGIC S.`BOL_ID`,
# MAGIC S.`UCT_ASSOCIATED_SH`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'VE70',
# MAGIC now()
# MAGIC )
# MAGIC   
# MAGIC 
# MAGIC  

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


