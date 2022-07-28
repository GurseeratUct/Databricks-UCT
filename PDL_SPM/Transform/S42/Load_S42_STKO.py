# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'STKO'
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
StructField('STLTY',StringType(),True),\
StructField('STLNR',StringType(),True),\
StructField('STLAL',StringType(),True),\
StructField('STKOZ',StringType(),True),\
StructField('DATUV',StringType(),True),\
StructField('TECHV',StringType(),True),\
StructField('AENNR',StringType(),True),\
StructField('LKENZ',StringType(),True),\
StructField('LOEKZ',StringType(),True),\
StructField('VGKZL',StringType(),True),\
StructField('ANDAT',StringType(),True),\
StructField('ANNAM',StringType(),True),\
StructField('AEDAT',StringType(),True),\
StructField('AENAM',StringType(),True),\
StructField('BMEIN',StringType(),True),\
StructField('BMENG',StringType(),True),\
StructField('CADKZ',StringType(),True),\
StructField('LABOR',StringType(),True),\
StructField('LTXSP',StringType(),True),\
StructField('STKTX',StringType(),True),\
StructField('STLST',StringType(),True),\
StructField('WRKAN',StringType(),True),\
StructField('DVDAT',StringType(),True),\
StructField('DVNAM',StringType(),True),\
StructField('AEHLP',StringType(),True),\
StructField('ALEKZ',StringType(),True),\
StructField('GUIDX',StringType(),True),\
StructField('VALID_TO',StringType(),True),\
StructField('ECN_TO',StringType(),True),\
StructField('BOM_VERSN',StringType(),True),\
StructField('VERSNST',StringType(),True),\
StructField('VERSNLASTIND',StringType(),True),\
StructField('LASTCHANGEDATETIME',StringType(),True),\
StructField('BOM_AIN_IND',StringType(),True),\
StructField('BOM_PREV_VERSN',StringType(),True),\
StructField('DUMMY_STKO_INCL_EEW_PS',StringType(),True),\
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
.withColumn( "DATUV",to_date(regexp_replace(df_add_column.DATUV,'\.','-'))) \
.withColumn( "ANDAT",to_date(regexp_replace(df_add_column.ANDAT,'\.','-'))) \
.withColumn( "AEDAT",to_date(regexp_replace(df_add_column.AEDAT,'\.','-'))) \
.withColumn( "DVDAT",to_date(regexp_replace(df_add_column.DVDAT,'\.','-'))) \
.withColumn( "VALID_TO",to_date(regexp_replace(df_add_column.VALID_TO,'\.','-'))) \
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
# MAGIC MERGE INTO S42.STKO as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY MANDT,STLTY,STLNR,STLAL,STKOZ ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_STKO)A where A.rn = 1 ) as S 
# MAGIC ON T.`MANDT`	 = S.`MANDT`
# MAGIC and T.`STLTY`	 = S.`STLTY`
# MAGIC and T.`STLNR`	 = S.`STLNR`
# MAGIC and T.`STLAL`	 = S.`STLAL`
# MAGIC and T.`STKOZ`	 = S.`STKOZ`
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`MANDT`	 = S.`MANDT`,
# MAGIC T.`STLTY`	 = S.`STLTY`,
# MAGIC T.`STLNR`	 = S.`STLNR`,
# MAGIC T.`STLAL`	 = S.`STLAL`,
# MAGIC T.`STKOZ`	 = S.`STKOZ`,
# MAGIC T.`DATUV`	 = S.`DATUV`,
# MAGIC T.`TECHV`	 = S.`TECHV`,
# MAGIC T.`AENNR`	 = S.`AENNR`,
# MAGIC T.`LKENZ`	 = S.`LKENZ`,
# MAGIC T.`LOEKZ`	 = S.`LOEKZ`,
# MAGIC T.`VGKZL`	 = S.`VGKZL`,
# MAGIC T.`ANDAT`	 = S.`ANDAT`,
# MAGIC T.`ANNAM`	 = S.`ANNAM`,
# MAGIC T.`AEDAT`	 = S.`AEDAT`,
# MAGIC T.`AENAM`	 = S.`AENAM`,
# MAGIC T.`BMEIN`	 = S.`BMEIN`,
# MAGIC T.`BMENG`	 = S.`BMENG`,
# MAGIC T.`CADKZ`	 = S.`CADKZ`,
# MAGIC T.`LABOR`	 = S.`LABOR`,
# MAGIC T.`LTXSP`	 = S.`LTXSP`,
# MAGIC T.`STKTX`	 = S.`STKTX`,
# MAGIC T.`STLST`	 = S.`STLST`,
# MAGIC T.`WRKAN`	 = S.`WRKAN`,
# MAGIC T.`DVDAT`	 = S.`DVDAT`,
# MAGIC T.`DVNAM`	 = S.`DVNAM`,
# MAGIC T.`AEHLP`	 = S.`AEHLP`,
# MAGIC T.`ALEKZ`	 = S.`ALEKZ`,
# MAGIC T.`GUIDX`	 = S.`GUIDX`,
# MAGIC T.`VALID_TO`	 = S.`VALID_TO`,
# MAGIC T.`ECN_TO`	 = S.`ECN_TO`,
# MAGIC T.`BOM_VERSN`	 = S.`BOM_VERSN`,
# MAGIC T.`VERSNST`	 = S.`VERSNST`,
# MAGIC T.`VERSNLASTIND`	 = S.`VERSNLASTIND`,
# MAGIC T.`LASTCHANGEDATETIME`	 = S.`LASTCHANGEDATETIME`,
# MAGIC T.`BOM_AIN_IND`	 = S.`BOM_AIN_IND`,
# MAGIC T.`BOM_PREV_VERSN`	 = S.`BOM_PREV_VERSN`,
# MAGIC T.`DUMMY_STKO_INCL_EEW_PS`	 = S.`DUMMY_STKO_INCL_EEW_PS`,
# MAGIC T.`ODQ_CHANGEMODE`	 = S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR`	 = S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp`	 = S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `STLTY`,
# MAGIC `STLNR`,
# MAGIC `STLAL`,
# MAGIC `STKOZ`,
# MAGIC `DATUV`,
# MAGIC `TECHV`,
# MAGIC `AENNR`,
# MAGIC `LKENZ`,
# MAGIC `LOEKZ`,
# MAGIC `VGKZL`,
# MAGIC `ANDAT`,
# MAGIC `ANNAM`,
# MAGIC `AEDAT`,
# MAGIC `AENAM`,
# MAGIC `BMEIN`,
# MAGIC `BMENG`,
# MAGIC `CADKZ`,
# MAGIC `LABOR`,
# MAGIC `LTXSP`,
# MAGIC `STKTX`,
# MAGIC `STLST`,
# MAGIC `WRKAN`,
# MAGIC `DVDAT`,
# MAGIC `DVNAM`,
# MAGIC `AEHLP`,
# MAGIC `ALEKZ`,
# MAGIC `GUIDX`,
# MAGIC `VALID_TO`,
# MAGIC `ECN_TO`,
# MAGIC `BOM_VERSN`,
# MAGIC `VERSNST`,
# MAGIC `VERSNLASTIND`,
# MAGIC `LASTCHANGEDATETIME`,
# MAGIC `BOM_AIN_IND`,
# MAGIC `BOM_PREV_VERSN`,
# MAGIC `DUMMY_STKO_INCL_EEW_PS`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC VALUES
# MAGIC (S.`MANDT`,
# MAGIC S.`STLTY`,
# MAGIC S.`STLNR`,
# MAGIC S.`STLAL`,
# MAGIC S.`STKOZ`,
# MAGIC S.`DATUV`,
# MAGIC S.`TECHV`,
# MAGIC S.`AENNR`,
# MAGIC S.`LKENZ`,
# MAGIC S.`LOEKZ`,
# MAGIC S.`VGKZL`,
# MAGIC S.`ANDAT`,
# MAGIC S.`ANNAM`,
# MAGIC S.`AEDAT`,
# MAGIC S.`AENAM`,
# MAGIC S.`BMEIN`,
# MAGIC S.`BMENG`,
# MAGIC S.`CADKZ`,
# MAGIC S.`LABOR`,
# MAGIC S.`LTXSP`,
# MAGIC S.`STKTX`,
# MAGIC S.`STLST`,
# MAGIC S.`WRKAN`,
# MAGIC S.`DVDAT`,
# MAGIC S.`DVNAM`,
# MAGIC S.`AEHLP`,
# MAGIC S.`ALEKZ`,
# MAGIC S.`GUIDX`,
# MAGIC S.`VALID_TO`,
# MAGIC S.`ECN_TO`,
# MAGIC S.`BOM_VERSN`,
# MAGIC S.`VERSNST`,
# MAGIC S.`VERSNLASTIND`,
# MAGIC S.`LASTCHANGEDATETIME`,
# MAGIC S.`BOM_AIN_IND`,
# MAGIC S.`BOM_PREV_VERSN`,
# MAGIC S.`DUMMY_STKO_INCL_EEW_PS`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)
