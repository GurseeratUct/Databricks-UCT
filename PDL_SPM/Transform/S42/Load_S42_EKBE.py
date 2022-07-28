# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'EKBE'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

schema = StructType([ StructField('DI_SEQUENCE_NUMBER',IntegerType(),True),\
                     StructField('DI_OPERATION_TYPE',StringType(),True),\
                     StructField('MANDT',IntegerType(),True),\
StructField('EBELN',StringType(),True),\
StructField('EBELP',StringType(),True),\
StructField('ZEKKN',IntegerType(),True),\
StructField('VGABE',StringType(),True),\
StructField('GJAHR',IntegerType(),True),\
StructField('BELNR',LongType(),True),\
StructField('BUZEI',IntegerType(),True),\
StructField('BEWTP',StringType(),True),\
StructField('BWART',StringType(),True),\
StructField('BUDAT',StringType(),True),\
StructField('MENGE',DoubleType(),True),\
StructField('BPMNG',DoubleType(),True),\
StructField('DMBTR',DoubleType(),True),\
StructField('WRBTR',DoubleType(),True),\
StructField('WAERS',StringType(),True),\
StructField('AREWR',DoubleType(),True),\
StructField('WESBS',DoubleType(),True),\
StructField('BPWES',DoubleType(),True),\
StructField('SHKZG',StringType(),True),\
StructField('BWTAR',StringType(),True),\
StructField('ELIKZ',StringType(),True),\
StructField('XBLNR',StringType(),True),\
StructField('LFGJA',IntegerType(),True),\
StructField('LFBNR',StringType(),True),\
StructField('LFPOS',IntegerType(),True),\
StructField('GRUND',IntegerType(),True),\
StructField('CPUDT',StringType(),True),\
StructField('CPUTM',TimestampType(),True),\
StructField('REEWR',DoubleType(),True),\
StructField('EVERE',StringType(),True),\
StructField('REFWR',DoubleType(),True),\
StructField('MATNR',StringType(),True),\
StructField('WERKS',StringType(),True),\
StructField('XWSBR',StringType(),True),\
StructField('ETENS',IntegerType(),True),\
StructField('KNUMV',StringType(),True),\
StructField('MWSKZ',StringType(),True),\
StructField('TAX_COUNTRY',StringType(),True),\
StructField('LSMNG',DoubleType(),True),\
StructField('LSMEH',StringType(),True),\
StructField('EMATN',StringType(),True),\
StructField('AREWW',DoubleType(),True),\
StructField('HSWAE',StringType(),True),\
StructField('BAMNG',DoubleType(),True),\
StructField('CHARG',StringType(),True),\
StructField('BLDAT',StringType(),True),\
StructField('XWOFF',StringType(),True),\
StructField('XUNPL',StringType(),True),\
StructField('ERNAM',StringType(),True),\
StructField('SRVPOS',StringType(),True),\
StructField('PACKNO',IntegerType(),True),\
StructField('INTROW',IntegerType(),True),\
StructField('BEKKN',IntegerType(),True),\
StructField('LEMIN',StringType(),True),\
StructField('AREWB',DoubleType(),True),\
StructField('REWRB',DoubleType(),True),\
StructField('SAPRL',StringType(),True),\
StructField('MENGE_POP',DoubleType(),True),\
StructField('BPMNG_POP',DoubleType(),True),\
StructField('DMBTR_POP',DoubleType(),True),\
StructField('WRBTR_POP',DoubleType(),True),\
StructField('WESBB',DoubleType(),True),\
StructField('BPWEB',DoubleType(),True),\
StructField('WEORA',StringType(),True),\
StructField('AREWR_POP',DoubleType(),True),\
StructField('KUDIF',DoubleType(),True),\
StructField('RETAMT_FC',DoubleType(),True),\
StructField('RETAMT_LC',DoubleType(),True),\
StructField('RETAMTP_FC',DoubleType(),True),\
StructField('RETAMTP_LC',DoubleType(),True),\
StructField('XMACC',StringType(),True),\
StructField('WKURS',DoubleType(),True),\
StructField('INV_ITEM_ORIGIN',StringType(),True),\
StructField('VBELN_ST',StringType(),True),\
StructField('VBELP_ST',IntegerType(),True),\
StructField('SGT_SCAT',StringType(),True),\
StructField('_DATAAGING',StringType(),True),\
StructField('SESUOM',StringType(),True),\
StructField('LOGSY',StringType(),True),\
StructField('ET_UPD',StringType(),True),\
StructField('/CWM/BAMNG',DoubleType(),True),\
StructField('/CWM/WESBS',DoubleType(),True),\
StructField('/CWM/TY2TQ',StringType(),True),\
StructField('/CWM/WESBB',DoubleType(),True),\
StructField('J_SC_DIE_COMP_F',StringType(),True),\
StructField('FSH_SEASON_YEAR',StringType(),True),\
StructField('FSH_SEASON',StringType(),True),\
StructField('FSH_COLLECTION',StringType(),True),\
StructField('FSH_THEME',StringType(),True),\
StructField('QTY_DIFF',DoubleType(),True),\
StructField('WRF_CHARSTC1',StringType(),True),\
StructField('WRF_CHARSTC2',StringType(),True),\
StructField('WRF_CHARSTC3',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',IntegerType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
                    ])

# COMMAND ----------

#df = spark.read.format(read_format) \
#      .option("header", True) \
#      .option("delimiter",delimiter) \
#      .option("InferSchema",True) \
#      .load(read_path)

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
                            .withColumn("BUDAT", to_date(regexp_replace(df_add_column.BUDAT,'\.','-'))) \
                            .withColumn("CPUDT", to_date(regexp_replace(df_add_column.CPUDT,'\.','-'))) \
                            .withColumn("BLDAT", to_date(regexp_replace(df_add_column.BLDAT,'\.','-'))) \
                            .withColumn("_DATAAGING", to_date(regexp_replace(df_add_column._DATAAGING,'\.','-'))) \
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
# MAGIC MERGE INTO S42.EKBE as T
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY EBELN,EBELP,ZEKKN,VGABE,GJAHR,BELNR,BUZEI ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_EKBE where MANDT = '100')A where A.rn = 1 ) as S 
# MAGIC ON T.EBELN = S.EBELN and 
# MAGIC T.EBELP = S.EBELP and
# MAGIC T.ZEKKN = S.ZEKKN and
# MAGIC T.VGABE = S.VGABE and
# MAGIC T.GJAHR = S.GJAHR and
# MAGIC T.BELNR = S.BELNR and
# MAGIC T.BUZEI = S.BUZEI 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`MANDT` = S.`MANDT`,
# MAGIC T.`EBELN` = S.`EBELN`,
# MAGIC T.`EBELP` = S.`EBELP`,
# MAGIC T.`ZEKKN` = S.`ZEKKN`,
# MAGIC T.`VGABE` = S.`VGABE`,
# MAGIC T.`GJAHR` = S.`GJAHR`,
# MAGIC T.`BELNR` = S.`BELNR`,
# MAGIC T.`BUZEI` = S.`BUZEI`,
# MAGIC T.`BEWTP` = S.`BEWTP`,
# MAGIC T.`BWART` = S.`BWART`,
# MAGIC T.`BUDAT` = S.`BUDAT`,
# MAGIC T.`MENGE` = S.`MENGE`,
# MAGIC T.`BPMNG` = S.`BPMNG`,
# MAGIC T.`DMBTR` = S.`DMBTR`,
# MAGIC T.`WRBTR` = S.`WRBTR`,
# MAGIC T.`WAERS` = S.`WAERS`,
# MAGIC T.`AREWR` = S.`AREWR`,
# MAGIC T.`WESBS` = S.`WESBS`,
# MAGIC T.`BPWES` = S.`BPWES`,
# MAGIC T.`SHKZG` = S.`SHKZG`,
# MAGIC T.`BWTAR` = S.`BWTAR`,
# MAGIC T.`ELIKZ` = S.`ELIKZ`,
# MAGIC T.`XBLNR` = S.`XBLNR`,
# MAGIC T.`LFGJA` = S.`LFGJA`,
# MAGIC T.`LFBNR` = S.`LFBNR`,
# MAGIC T.`LFPOS` = S.`LFPOS`,
# MAGIC T.`GRUND` = S.`GRUND`,
# MAGIC T.`CPUDT` = S.`CPUDT`,
# MAGIC T.`CPUTM` = S.`CPUTM`,
# MAGIC T.`REEWR` = S.`REEWR`,
# MAGIC T.`EVERE` = S.`EVERE`,
# MAGIC T.`REFWR` = S.`REFWR`,
# MAGIC T.`MATNR` = S.`MATNR`,
# MAGIC T.`WERKS` = S.`WERKS`,
# MAGIC T.`XWSBR` = S.`XWSBR`,
# MAGIC T.`ETENS` = S.`ETENS`,
# MAGIC T.`KNUMV` = S.`KNUMV`,
# MAGIC T.`MWSKZ` = S.`MWSKZ`,
# MAGIC T.`TAX_COUNTRY` = S.`TAX_COUNTRY`,
# MAGIC T.`LSMNG` = S.`LSMNG`,
# MAGIC T.`LSMEH` = S.`LSMEH`,
# MAGIC T.`EMATN` = S.`EMATN`,
# MAGIC T.`AREWW` = S.`AREWW`,
# MAGIC T.`HSWAE` = S.`HSWAE`,
# MAGIC T.`BAMNG` = S.`BAMNG`,
# MAGIC T.`CHARG` = S.`CHARG`,
# MAGIC T.`BLDAT` = S.`BLDAT`,
# MAGIC T.`XWOFF` = S.`XWOFF`,
# MAGIC T.`XUNPL` = S.`XUNPL`,
# MAGIC T.`ERNAM` = S.`ERNAM`,
# MAGIC T.`SRVPOS` = S.`SRVPOS`,
# MAGIC T.`PACKNO` = S.`PACKNO`,
# MAGIC T.`INTROW` = S.`INTROW`,
# MAGIC T.`BEKKN` = S.`BEKKN`,
# MAGIC T.`LEMIN` = S.`LEMIN`,
# MAGIC T.`AREWB` = S.`AREWB`,
# MAGIC T.`REWRB` = S.`REWRB`,
# MAGIC T.`SAPRL` = S.`SAPRL`,
# MAGIC T.`MENGE_POP` = S.`MENGE_POP`,
# MAGIC T.`BPMNG_POP` = S.`BPMNG_POP`,
# MAGIC T.`DMBTR_POP` = S.`DMBTR_POP`,
# MAGIC T.`WRBTR_POP` = S.`WRBTR_POP`,
# MAGIC T.`WESBB` = S.`WESBB`,
# MAGIC T.`BPWEB` = S.`BPWEB`,
# MAGIC T.`WEORA` = S.`WEORA`,
# MAGIC T.`AREWR_POP` = S.`AREWR_POP`,
# MAGIC T.`KUDIF` = S.`KUDIF`,
# MAGIC T.`RETAMT_FC` = S.`RETAMT_FC`,
# MAGIC T.`RETAMT_LC` = S.`RETAMT_LC`,
# MAGIC T.`RETAMTP_FC` = S.`RETAMTP_FC`,
# MAGIC T.`RETAMTP_LC` = S.`RETAMTP_LC`,
# MAGIC T.`XMACC` = S.`XMACC`,
# MAGIC T.`WKURS` = S.`WKURS`,
# MAGIC T.`INV_ITEM_ORIGIN` = S.`INV_ITEM_ORIGIN`,
# MAGIC T.`VBELN_ST` = S.`VBELN_ST`,
# MAGIC T.`VBELP_ST` = S.`VBELP_ST`,
# MAGIC T.`SGT_SCAT` = S.`SGT_SCAT`,
# MAGIC T.`_DATAAGING` = S.`_DATAAGING`,
# MAGIC T.`SESUOM` = S.`SESUOM`,
# MAGIC T.`LOGSY` = S.`LOGSY`,
# MAGIC T.`ET_UPD` = S.`ET_UPD`,
# MAGIC T.`/CWM/BAMNG` = S.`/CWM/BAMNG`,
# MAGIC T.`/CWM/WESBS` = S.`/CWM/WESBS`,
# MAGIC T.`/CWM/TY2TQ` = S.`/CWM/TY2TQ`,
# MAGIC T.`/CWM/WESBB` = S.`/CWM/WESBB`,
# MAGIC T.`J_SC_DIE_COMP_F` = S.`J_SC_DIE_COMP_F`,
# MAGIC T.`FSH_SEASON_YEAR` = S.`FSH_SEASON_YEAR`,
# MAGIC T.`FSH_SEASON` = S.`FSH_SEASON`,
# MAGIC T.`FSH_COLLECTION` = S.`FSH_COLLECTION`,
# MAGIC T.`FSH_THEME` = S.`FSH_THEME`,
# MAGIC T.`QTY_DIFF` = S.`QTY_DIFF`,
# MAGIC T.`WRF_CHARSTC1` = S.`WRF_CHARSTC1`,
# MAGIC T.`WRF_CHARSTC2` = S.`WRF_CHARSTC2`,
# MAGIC T.`WRF_CHARSTC3` = S.`WRF_CHARSTC3`,
# MAGIC T.`ODQ_CHANGEMODE` = S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` = S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` = S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC `MANDT`,
# MAGIC `EBELN`,
# MAGIC `EBELP`,
# MAGIC `ZEKKN`,
# MAGIC `VGABE`,
# MAGIC `GJAHR`,
# MAGIC `BELNR`,
# MAGIC `BUZEI`,
# MAGIC `BEWTP`,
# MAGIC `BWART`,
# MAGIC `BUDAT`,
# MAGIC `MENGE`,
# MAGIC `BPMNG`,
# MAGIC `DMBTR`,
# MAGIC `WRBTR`,
# MAGIC `WAERS`,
# MAGIC `AREWR`,
# MAGIC `WESBS`,
# MAGIC `BPWES`,
# MAGIC `SHKZG`,
# MAGIC `BWTAR`,
# MAGIC `ELIKZ`,
# MAGIC `XBLNR`,
# MAGIC `LFGJA`,
# MAGIC `LFBNR`,
# MAGIC `LFPOS`,
# MAGIC `GRUND`,
# MAGIC `CPUDT`,
# MAGIC `CPUTM`,
# MAGIC `REEWR`,
# MAGIC `EVERE`,
# MAGIC `REFWR`,
# MAGIC `MATNR`,
# MAGIC `WERKS`,
# MAGIC `XWSBR`,
# MAGIC `ETENS`,
# MAGIC `KNUMV`,
# MAGIC `MWSKZ`,
# MAGIC `TAX_COUNTRY`,
# MAGIC `LSMNG`,
# MAGIC `LSMEH`,
# MAGIC `EMATN`,
# MAGIC `AREWW`,
# MAGIC `HSWAE`,
# MAGIC `BAMNG`,
# MAGIC `CHARG`,
# MAGIC `BLDAT`,
# MAGIC `XWOFF`,
# MAGIC `XUNPL`,
# MAGIC `ERNAM`,
# MAGIC `SRVPOS`,
# MAGIC `PACKNO`,
# MAGIC `INTROW`,
# MAGIC `BEKKN`,
# MAGIC `LEMIN`,
# MAGIC `AREWB`,
# MAGIC `REWRB`,
# MAGIC `SAPRL`,
# MAGIC `MENGE_POP`,
# MAGIC `BPMNG_POP`,
# MAGIC `DMBTR_POP`,
# MAGIC `WRBTR_POP`,
# MAGIC `WESBB`,
# MAGIC `BPWEB`,
# MAGIC `WEORA`,
# MAGIC `AREWR_POP`,
# MAGIC `KUDIF`,
# MAGIC `RETAMT_FC`,
# MAGIC `RETAMT_LC`,
# MAGIC `RETAMTP_FC`,
# MAGIC `RETAMTP_LC`,
# MAGIC `XMACC`,
# MAGIC `WKURS`,
# MAGIC `INV_ITEM_ORIGIN`,
# MAGIC `VBELN_ST`,
# MAGIC `VBELP_ST`,
# MAGIC `SGT_SCAT`,
# MAGIC `_DATAAGING`,
# MAGIC `SESUOM`,
# MAGIC `LOGSY`,
# MAGIC `ET_UPD`,
# MAGIC `/CWM/BAMNG`,
# MAGIC `/CWM/WESBS`,
# MAGIC `/CWM/TY2TQ`,
# MAGIC `/CWM/WESBB`,
# MAGIC `J_SC_DIE_COMP_F`,
# MAGIC `FSH_SEASON_YEAR`,
# MAGIC `FSH_SEASON`,
# MAGIC `FSH_COLLECTION`,
# MAGIC `FSH_THEME`,
# MAGIC `QTY_DIFF`,
# MAGIC `WRF_CHARSTC1`,
# MAGIC `WRF_CHARSTC2`,
# MAGIC `WRF_CHARSTC3`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn)
# MAGIC   VALUES 
# MAGIC (
# MAGIC S.`MANDT`,
# MAGIC S.`EBELN`,
# MAGIC S.`EBELP`,
# MAGIC S.`ZEKKN`,
# MAGIC S.`VGABE`,
# MAGIC S.`GJAHR`,
# MAGIC S.`BELNR`,
# MAGIC S.`BUZEI`,
# MAGIC S.`BEWTP`,
# MAGIC S.`BWART`,
# MAGIC S.`BUDAT`,
# MAGIC S.`MENGE`,
# MAGIC S.`BPMNG`,
# MAGIC S.`DMBTR`,
# MAGIC S.`WRBTR`,
# MAGIC S.`WAERS`,
# MAGIC S.`AREWR`,
# MAGIC S.`WESBS`,
# MAGIC S.`BPWES`,
# MAGIC S.`SHKZG`,
# MAGIC S.`BWTAR`,
# MAGIC S.`ELIKZ`,
# MAGIC S.`XBLNR`,
# MAGIC S.`LFGJA`,
# MAGIC S.`LFBNR`,
# MAGIC S.`LFPOS`,
# MAGIC S.`GRUND`,
# MAGIC S.`CPUDT`,
# MAGIC S.`CPUTM`,
# MAGIC S.`REEWR`,
# MAGIC S.`EVERE`,
# MAGIC S.`REFWR`,
# MAGIC S.`MATNR`,
# MAGIC S.`WERKS`,
# MAGIC S.`XWSBR`,
# MAGIC S.`ETENS`,
# MAGIC S.`KNUMV`,
# MAGIC S.`MWSKZ`,
# MAGIC S.`TAX_COUNTRY`,
# MAGIC S.`LSMNG`,
# MAGIC S.`LSMEH`,
# MAGIC S.`EMATN`,
# MAGIC S.`AREWW`,
# MAGIC S.`HSWAE`,
# MAGIC S.`BAMNG`,
# MAGIC S.`CHARG`,
# MAGIC S.`BLDAT`,
# MAGIC S.`XWOFF`,
# MAGIC S.`XUNPL`,
# MAGIC S.`ERNAM`,
# MAGIC S.`SRVPOS`,
# MAGIC S.`PACKNO`,
# MAGIC S.`INTROW`,
# MAGIC S.`BEKKN`,
# MAGIC S.`LEMIN`,
# MAGIC S.`AREWB`,
# MAGIC S.`REWRB`,
# MAGIC S.`SAPRL`,
# MAGIC S.`MENGE_POP`,
# MAGIC S.`BPMNG_POP`,
# MAGIC S.`DMBTR_POP`,
# MAGIC S.`WRBTR_POP`,
# MAGIC S.`WESBB`,
# MAGIC S.`BPWEB`,
# MAGIC S.`WEORA`,
# MAGIC S.`AREWR_POP`,
# MAGIC S.`KUDIF`,
# MAGIC S.`RETAMT_FC`,
# MAGIC S.`RETAMT_LC`,
# MAGIC S.`RETAMTP_FC`,
# MAGIC S.`RETAMTP_LC`,
# MAGIC S.`XMACC`,
# MAGIC S.`WKURS`,
# MAGIC S.`INV_ITEM_ORIGIN`,
# MAGIC S.`VBELN_ST`,
# MAGIC S.`VBELP_ST`,
# MAGIC S.`SGT_SCAT`,
# MAGIC S.`_DATAAGING`,
# MAGIC S.`SESUOM`,
# MAGIC S.`LOGSY`,
# MAGIC S.`ET_UPD`,
# MAGIC S.`/CWM/BAMNG`,
# MAGIC S.`/CWM/WESBS`,
# MAGIC S.`/CWM/TY2TQ`,
# MAGIC S.`/CWM/WESBB`,
# MAGIC S.`J_SC_DIE_COMP_F`,
# MAGIC S.`FSH_SEASON_YEAR`,
# MAGIC S.`FSH_SEASON`,
# MAGIC S.`FSH_COLLECTION`,
# MAGIC S.`FSH_THEME`,
# MAGIC S.`QTY_DIFF`,
# MAGIC S.`WRF_CHARSTC1`,
# MAGIC S.`WRF_CHARSTC2`,
# MAGIC S.`WRF_CHARSTC3`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'SAP',
# MAGIC now()
# MAGIC  )
# MAGIC  

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path,True)

# COMMAND ----------


