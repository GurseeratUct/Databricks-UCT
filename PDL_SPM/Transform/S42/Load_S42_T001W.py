# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'T001W'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'


read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

#df = spark.read.options(header='True', inferSchema='True', delimiter='^').csv(read_path)

# COMMAND ----------

schema = StructType([ \
                     StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
StructField('DI_OPERATION_TYPE',StringType(),True),\
StructField('MANDT',IntegerType(),True),\
StructField('WERKS',StringType(),True),\
StructField('NAME1',StringType(),True),\
StructField('BWKEY',StringType(),True),\
StructField('KUNNR',StringType(),True),\
StructField('LIFNR',StringType(),True),\
StructField('FABKL',StringType(),True),\
StructField('NAME2',StringType(),True),\
StructField('STRAS',StringType(),True),\
StructField('PFACH',StringType(),True),\
StructField('PSTLZ',StringType(),True),\
StructField('ORT01',StringType(),True),\
StructField('EKORG',StringType(),True),\
StructField('VKORG',StringType(),True),\
StructField('CHAZV',StringType(),True),\
StructField('KKOWK',StringType(),True),\
StructField('KORDB',StringType(),True),\
StructField('BEDPL',StringType(),True),\
StructField('LAND1',StringType(),True),\
StructField('REGIO',StringType(),True),\
StructField('COUNC',StringType(),True),\
StructField('CITYC',StringType(),True),\
StructField('ADRNR',StringType(),True),\
StructField('IWERK',StringType(),True),\
StructField('TXJCD',StringType(),True),\
StructField('VTWEG',StringType(),True),\
StructField('SPART',StringType(),True),\
StructField('SPRAS',StringType(),True),\
StructField('WKSOP',StringType(),True),\
StructField('AWSLS',StringType(),True),\
StructField('CHAZV_OLD',StringType(),True),\
StructField('VLFKZ',StringType(),True),\
StructField('BZIRK',StringType(),True),\
StructField('ZONE1',StringType(),True),\
StructField('TAXIW',StringType(),True),\
StructField('BZQHL',StringType(),True),\
StructField('LET01',IntegerType(),True),\
StructField('LET02',IntegerType(),True),\
StructField('LET03',IntegerType(),True),\
StructField('TXNAM_MA1',StringType(),True),\
StructField('TXNAM_MA2',StringType(),True),\
StructField('TXNAM_MA3',StringType(),True),\
StructField('BETOL',IntegerType(),True),\
StructField('J_1BBRANCH',StringType(),True),\
StructField('VTBFI',StringType(),True),\
StructField('FPRFW',StringType(),True),\
StructField('ACHVM',StringType(),True),\
StructField('DVSART',StringType(),True),\
StructField('NODETYPE',StringType(),True),\
StructField('NSCHEMA',StringType(),True),\
StructField('PKOSA',StringType(),True),\
StructField('MISCH',StringType(),True),\
StructField('MGVUPD',StringType(),True),\
StructField('VSTEL',StringType(),True),\
StructField('MGVLAUPD',StringType(),True),\
StructField('MGVLAREVAL',StringType(),True),\
StructField('SOURCING',StringType(),True),\
StructField('NO_DEFAULT_BATCH_MANAGEMENT',StringType(),True),\
StructField('FSH_MG_ARUN_REQ',StringType(),True),\
StructField('FSH_SEAIM',StringType(),True),\
StructField('FSH_BOM_MAINTENANCE',StringType(),True),\
StructField('FSH_GROUP_PR',StringType(),True),\
StructField('ARUN_FIX_BATCH',StringType(),True),\
StructField('OILIVAL',StringType(),True),\
StructField('OIHVTYPE',StringType(),True),\
StructField('OIHCREDIPI',StringType(),True),\
StructField('STORETYPE',StringType(),True),\
StructField('DEP_STORE',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
StructField('LandingFileTimeStamp',StringType(),True)
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
# MAGIC MERGE INTO S42.T001W as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,WERKS ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T001W where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and 
# MAGIC T.WERKS = S.WERKS
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`DI_SEQUENCE_NUMBER` =  S.`DI_SEQUENCE_NUMBER`,
# MAGIC T.`DI_OPERATION_TYPE` =  S.`DI_OPERATION_TYPE`,
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`WERKS` =  S.`WERKS`,
# MAGIC T.`NAME1` =  S.`NAME1`,
# MAGIC T.`BWKEY` =  S.`BWKEY`,
# MAGIC T.`KUNNR` =  S.`KUNNR`,
# MAGIC T.`LIFNR` =  S.`LIFNR`,
# MAGIC T.`FABKL` =  S.`FABKL`,
# MAGIC T.`NAME2` =  S.`NAME2`,
# MAGIC T.`STRAS` =  S.`STRAS`,
# MAGIC T.`PFACH` =  S.`PFACH`,
# MAGIC T.`PSTLZ` =  S.`PSTLZ`,
# MAGIC T.`ORT01` =  S.`ORT01`,
# MAGIC T.`EKORG` =  S.`EKORG`,
# MAGIC T.`VKORG` =  S.`VKORG`,
# MAGIC T.`CHAZV` =  S.`CHAZV`,
# MAGIC T.`KKOWK` =  S.`KKOWK`,
# MAGIC T.`KORDB` =  S.`KORDB`,
# MAGIC T.`BEDPL` =  S.`BEDPL`,
# MAGIC T.`LAND1` =  S.`LAND1`,
# MAGIC T.`REGIO` =  S.`REGIO`,
# MAGIC T.`COUNC` =  S.`COUNC`,
# MAGIC T.`CITYC` =  S.`CITYC`,
# MAGIC T.`ADRNR` =  S.`ADRNR`,
# MAGIC T.`IWERK` =  S.`IWERK`,
# MAGIC T.`TXJCD` =  S.`TXJCD`,
# MAGIC T.`VTWEG` =  S.`VTWEG`,
# MAGIC T.`SPART` =  S.`SPART`,
# MAGIC T.`SPRAS` =  S.`SPRAS`,
# MAGIC T.`WKSOP` =  S.`WKSOP`,
# MAGIC T.`AWSLS` =  S.`AWSLS`,
# MAGIC T.`CHAZV_OLD` =  S.`CHAZV_OLD`,
# MAGIC T.`VLFKZ` =  S.`VLFKZ`,
# MAGIC T.`BZIRK` =  S.`BZIRK`,
# MAGIC T.`ZONE1` =  S.`ZONE1`,
# MAGIC T.`TAXIW` =  S.`TAXIW`,
# MAGIC T.`BZQHL` =  S.`BZQHL`,
# MAGIC T.`LET01` =  S.`LET01`,
# MAGIC T.`LET02` =  S.`LET02`,
# MAGIC T.`LET03` =  S.`LET03`,
# MAGIC T.`TXNAM_MA1` =  S.`TXNAM_MA1`,
# MAGIC T.`TXNAM_MA2` =  S.`TXNAM_MA2`,
# MAGIC T.`TXNAM_MA3` =  S.`TXNAM_MA3`,
# MAGIC T.`BETOL` =  S.`BETOL`,
# MAGIC T.`J_1BBRANCH` =  S.`J_1BBRANCH`,
# MAGIC T.`VTBFI` =  S.`VTBFI`,
# MAGIC T.`FPRFW` =  S.`FPRFW`,
# MAGIC T.`ACHVM` =  S.`ACHVM`,
# MAGIC T.`DVSART` =  S.`DVSART`,
# MAGIC T.`NODETYPE` =  S.`NODETYPE`,
# MAGIC T.`NSCHEMA` =  S.`NSCHEMA`,
# MAGIC T.`PKOSA` =  S.`PKOSA`,
# MAGIC T.`MISCH` =  S.`MISCH`,
# MAGIC T.`MGVUPD` =  S.`MGVUPD`,
# MAGIC T.`VSTEL` =  S.`VSTEL`,
# MAGIC T.`MGVLAUPD` =  S.`MGVLAUPD`,
# MAGIC T.`MGVLAREVAL` =  S.`MGVLAREVAL`,
# MAGIC T.`SOURCING` =  S.`SOURCING`,
# MAGIC T.`NO_DEFAULT_BATCH_MANAGEMENT` =  S.`NO_DEFAULT_BATCH_MANAGEMENT`,
# MAGIC T.`FSH_MG_ARUN_REQ` =  S.`FSH_MG_ARUN_REQ`,
# MAGIC T.`FSH_SEAIM` =  S.`FSH_SEAIM`,
# MAGIC T.`FSH_BOM_MAINTENANCE` =  S.`FSH_BOM_MAINTENANCE`,
# MAGIC T.`FSH_GROUP_PR` =  S.`FSH_GROUP_PR`,
# MAGIC T.`ARUN_FIX_BATCH` =  S.`ARUN_FIX_BATCH`,
# MAGIC T.`OILIVAL` =  S.`OILIVAL`,
# MAGIC T.`OIHVTYPE` =  S.`OIHVTYPE`,
# MAGIC T.`OIHCREDIPI` =  S.`OIHCREDIPI`,
# MAGIC T.`STORETYPE` =  S.`STORETYPE`,
# MAGIC T.`DEP_STORE` =  S.`DEP_STORE`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `DI_SEQUENCE_NUMBER`,
# MAGIC `DI_OPERATION_TYPE`,
# MAGIC `MANDT`,
# MAGIC `WERKS`,
# MAGIC `NAME1`,
# MAGIC `BWKEY`,
# MAGIC `KUNNR`,
# MAGIC `LIFNR`,
# MAGIC `FABKL`,
# MAGIC `NAME2`,
# MAGIC `STRAS`,
# MAGIC `PFACH`,
# MAGIC `PSTLZ`,
# MAGIC `ORT01`,
# MAGIC `EKORG`,
# MAGIC `VKORG`,
# MAGIC `CHAZV`,
# MAGIC `KKOWK`,
# MAGIC `KORDB`,
# MAGIC `BEDPL`,
# MAGIC `LAND1`,
# MAGIC `REGIO`,
# MAGIC `COUNC`,
# MAGIC `CITYC`,
# MAGIC `ADRNR`,
# MAGIC `IWERK`,
# MAGIC `TXJCD`,
# MAGIC `VTWEG`,
# MAGIC `SPART`,
# MAGIC `SPRAS`,
# MAGIC `WKSOP`,
# MAGIC `AWSLS`,
# MAGIC `CHAZV_OLD`,
# MAGIC `VLFKZ`,
# MAGIC `BZIRK`,
# MAGIC `ZONE1`,
# MAGIC `TAXIW`,
# MAGIC `BZQHL`,
# MAGIC `LET01`,
# MAGIC `LET02`,
# MAGIC `LET03`,
# MAGIC `TXNAM_MA1`,
# MAGIC `TXNAM_MA2`,
# MAGIC `TXNAM_MA3`,
# MAGIC `BETOL`,
# MAGIC `J_1BBRANCH`,
# MAGIC `VTBFI`,
# MAGIC `FPRFW`,
# MAGIC `ACHVM`,
# MAGIC `DVSART`,
# MAGIC `NODETYPE`,
# MAGIC `NSCHEMA`,
# MAGIC `PKOSA`,
# MAGIC `MISCH`,
# MAGIC `MGVUPD`,
# MAGIC `VSTEL`,
# MAGIC `MGVLAUPD`,
# MAGIC `MGVLAREVAL`,
# MAGIC `SOURCING`,
# MAGIC `NO_DEFAULT_BATCH_MANAGEMENT`,
# MAGIC `FSH_MG_ARUN_REQ`,
# MAGIC `FSH_SEAIM`,
# MAGIC `FSH_BOM_MAINTENANCE`,
# MAGIC `FSH_GROUP_PR`,
# MAGIC `ARUN_FIX_BATCH`,
# MAGIC `OILIVAL`,
# MAGIC `OIHVTYPE`,
# MAGIC `OIHCREDIPI`,
# MAGIC `STORETYPE`,
# MAGIC `DEP_STORE`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC ) VALUES 
# MAGIC (
# MAGIC S.`DI_SEQUENCE_NUMBER`,
# MAGIC S.`DI_OPERATION_TYPE`,
# MAGIC S.`MANDT`,
# MAGIC S.`WERKS`,
# MAGIC S.`NAME1`,
# MAGIC S.`BWKEY`,
# MAGIC S.`KUNNR`,
# MAGIC S.`LIFNR`,
# MAGIC S.`FABKL`,
# MAGIC S.`NAME2`,
# MAGIC S.`STRAS`,
# MAGIC S.`PFACH`,
# MAGIC S.`PSTLZ`,
# MAGIC S.`ORT01`,
# MAGIC S.`EKORG`,
# MAGIC S.`VKORG`,
# MAGIC S.`CHAZV`,
# MAGIC S.`KKOWK`,
# MAGIC S.`KORDB`,
# MAGIC S.`BEDPL`,
# MAGIC S.`LAND1`,
# MAGIC S.`REGIO`,
# MAGIC S.`COUNC`,
# MAGIC S.`CITYC`,
# MAGIC S.`ADRNR`,
# MAGIC S.`IWERK`,
# MAGIC S.`TXJCD`,
# MAGIC S.`VTWEG`,
# MAGIC S.`SPART`,
# MAGIC S.`SPRAS`,
# MAGIC S.`WKSOP`,
# MAGIC S.`AWSLS`,
# MAGIC S.`CHAZV_OLD`,
# MAGIC S.`VLFKZ`,
# MAGIC S.`BZIRK`,
# MAGIC S.`ZONE1`,
# MAGIC S.`TAXIW`,
# MAGIC S.`BZQHL`,
# MAGIC S.`LET01`,
# MAGIC S.`LET02`,
# MAGIC S.`LET03`,
# MAGIC S.`TXNAM_MA1`,
# MAGIC S.`TXNAM_MA2`,
# MAGIC S.`TXNAM_MA3`,
# MAGIC S.`BETOL`,
# MAGIC S.`J_1BBRANCH`,
# MAGIC S.`VTBFI`,
# MAGIC S.`FPRFW`,
# MAGIC S.`ACHVM`,
# MAGIC S.`DVSART`,
# MAGIC S.`NODETYPE`,
# MAGIC S.`NSCHEMA`,
# MAGIC S.`PKOSA`,
# MAGIC S.`MISCH`,
# MAGIC S.`MGVUPD`,
# MAGIC S.`VSTEL`,
# MAGIC S.`MGVLAUPD`,
# MAGIC S.`MGVLAREVAL`,
# MAGIC S.`SOURCING`,
# MAGIC S.`NO_DEFAULT_BATCH_MANAGEMENT`,
# MAGIC S.`FSH_MG_ARUN_REQ`,
# MAGIC S.`FSH_SEAIM`,
# MAGIC S.`FSH_BOM_MAINTENANCE`,
# MAGIC S.`FSH_GROUP_PR`,
# MAGIC S.`ARUN_FIX_BATCH`,
# MAGIC S.`OILIVAL`,
# MAGIC S.`OIHVTYPE`,
# MAGIC S.`OIHCREDIPI`,
# MAGIC S.`STORETYPE`,
# MAGIC S.`DEP_STORE`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'SAP',
# MAGIC now()
# MAGIC )
# MAGIC     

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


