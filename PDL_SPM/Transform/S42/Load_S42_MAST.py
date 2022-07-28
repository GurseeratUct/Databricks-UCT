# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'MAST'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'


read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.options(header='True', inferShema='True', delimiter='^').csv(read_path)

# COMMAND ----------

schema = StructType([ \
StructField('DI_SEQUENCE_NUMBER',StringType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',StringType(),True),\
StructField('MATNR',StringType(),True),\
StructField('WERKS',StringType(),True),\
StructField('STLAN',StringType(),True),\
StructField('STLNR',StringType(),True),\
StructField('STLAL',StringType(),True),\
StructField('LOSVN',StringType(),True),\
StructField('LOSBS',StringType(),True),\
StructField('ANDAT',StringType(),True),\
StructField('ANNAM',StringType(),True),\
StructField('AEDAT',StringType(),True),\
StructField('AENAM',StringType(),True),\
StructField('CSLTY',StringType(),True),\
StructField('MATERIAL_BOM_KEY',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
                    ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) \
.withColumn( "ANDAT",to_date(regexp_replace(df_add_column.ANDAT,'\.','-'))) \
.withColumn( "AEDAT",to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
.na.fill(0)

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.MAST as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY MANDT,MATNR,WERKS,STLAN,STLNR,STLAL ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_MAST)A where A.rn = 1 ) as S 
# MAGIC ON T.`MANDT`	 = S.`MANDT`
# MAGIC and T.`MATNR`	 = S.`MATNR`
# MAGIC and T.`WERKS`	 = S.`WERKS`
# MAGIC and T.`STLAN`	 = S.`STLAN`
# MAGIC and T.`STLNR`	 = S.`STLNR`
# MAGIC and T.`STLAL`	 = S.`STLAL`
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`MANDT`	 = S.`MANDT`,
# MAGIC T.`MATNR`	 = S.`MATNR`,
# MAGIC T.`WERKS`	 = S.`WERKS`,
# MAGIC T.`STLAN`	 = S.`STLAN`,
# MAGIC T.`STLNR`	 = S.`STLNR`,
# MAGIC T.`STLAL`	 = S.`STLAL`,
# MAGIC T.`LOSVN`	 = S.`LOSVN`,
# MAGIC T.`LOSBS`	 = S.`LOSBS`,
# MAGIC T.`ANDAT`	 = S.`ANDAT`,
# MAGIC T.`ANNAM`	 = S.`ANNAM`,
# MAGIC T.`AEDAT`	 = S.`AEDAT`,
# MAGIC T.`AENAM`	 = S.`AENAM`,
# MAGIC T.`CSLTY`	 = S.`CSLTY`,
# MAGIC T.`MATERIAL_BOM_KEY`	 = S.`MATERIAL_BOM_KEY`,
# MAGIC T.`ODQ_CHANGEMODE`	 = S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR`	 = S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp`	 = S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `MATNR`,
# MAGIC `WERKS`,
# MAGIC `STLAN`,
# MAGIC `STLNR`,
# MAGIC `STLAL`,
# MAGIC `LOSVN`,
# MAGIC `LOSBS`,
# MAGIC `ANDAT`,
# MAGIC `ANNAM`,
# MAGIC `AEDAT`,
# MAGIC `AENAM`,
# MAGIC `CSLTY`,
# MAGIC `MATERIAL_BOM_KEY`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC VALUES
# MAGIC (S.`MANDT`,
# MAGIC S.`MATNR`,
# MAGIC S.`WERKS`,
# MAGIC S.`STLAN`,
# MAGIC S.`STLNR`,
# MAGIC S.`STLAL`,
# MAGIC S.`LOSVN`,
# MAGIC S.`LOSBS`,
# MAGIC S.`ANDAT`,
# MAGIC S.`ANNAM`,
# MAGIC S.`AEDAT`,
# MAGIC S.`AENAM`,
# MAGIC S.`CSLTY`,
# MAGIC S.`MATERIAL_BOM_KEY`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


