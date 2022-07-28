# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Unit_Of_Measure'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_marm = ["MATNR",
"MEINH",
"UMREZ",
"UMREN",
"LAENG",
"BREIT",
"HOEHE",
"VOLUM",
"BRGEW",
"GEWEI",
"ATINN",
"MESRT",
"NEST_FTR",
"MAX_STACK",
"TOP_LOAD_FULL",
"CAPAUSE",
"UMREZ",
"UMREN"]

# COMMAND ----------

col_names_part_units_conv = [
"PART_ID",
"FROM_UM",
"TO_UM",
"CONVERSION_FACTOR"
]

# COMMAND ----------

df = spark.sql("select * from VE70.PART_UNITS_CONV where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PART_UNITS_CONV' and TargetLayerTableName ='Unit_Of_Measure' and DatabaseName = 'VE70')")
df_ve70_part_units_conv = df.select(col_names_part_units_conv)
df_ve70_part_units_conv.createOrReplaceTempView("ve70_part_units_conv_tmp")
df_ts_ve70_part_units_conv = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_part_units_conv = df_ts_ve70_part_units_conv["max(UpdatedOn)"]
print(ts_ve70_part_units_conv)

# COMMAND ----------

df = spark.sql("select * from VE72.PART_UNITS_CONV where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PART_UNITS_CONV' and TargetLayerTableName ='Unit_Of_Measure' and DatabaseName = 'VE72')")
df_ve72_part_units_conv = df.select(col_names_part_units_conv)
df_ve72_part_units_conv.createOrReplaceTempView("ve72_part_units_conv_tmp")
df_ts_ve72_part_units_conv = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_part_units_conv = df_ts_ve72_part_units_conv["max(UpdatedOn)"]
print(ts_ve72_part_units_conv)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view part_units_conv_tmp
# MAGIC as
# MAGIC select *, 'VE70' as DataSource from ve70_part_units_conv_tmp
# MAGIC union 
# MAGIC select *, 'VE72' as DataSource  from ve72_part_units_conv_tmp

# COMMAND ----------

df = spark.sql("select * from S42.MARM where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'MARM' and TargetLayerTableName ='Unit_Of_Measure' and DatabaseName = 'S42')")
df_marm = df.select(col_names_marm)
df_marm.createOrReplaceTempView("marm_tmp")
df_ts_marm = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_marm = df_ts_marm["max(UpdatedOn)"]
print(ts_marm)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view unit_of_measure as
# MAGIC select 
# MAGIC mm.MATNR as MaterialNumber,
# MAGIC ma.MEINS as ToUOM,
# MAGIC mm.MEINH as FromUOM,
# MAGIC mm.UMREZ as NumeratorForConvToBaseUOM,
# MAGIC mm.UMREN as DenominatorForConvToBaseUOM,
# MAGIC mm.LAENG as Length,
# MAGIC mm.BREIT as Width,
# MAGIC mm.HOEHE as Height,
# MAGIC mm.VOLUM as Volume,
# MAGIC mm.BRGEW as GrossWeight,
# MAGIC mm.GEWEI as WeightUnit,
# MAGIC mm.ATINN as InternalCharacteristic,
# MAGIC mm.MESRT as UOMSortNumber,
# MAGIC mm.NEST_FTR as RemainingVolumeAfterNesting,
# MAGIC mm.MAX_STACK as MaxStackingFactor,
# MAGIC mm.TOP_LOAD_FULL as MaxTopLoadOnFullPackage,
# MAGIC mm.CAPAUSE as CapacityUsage,
# MAGIC mm.UMREZ/mm.UMREN as ConversionFactor,
# MAGIC 'SAP' as DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from marm_tmp as mm
# MAGIC  join S42.MARA as ma
# MAGIC on mm.MATNR = ma.MATNR
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC select
# MAGIC V.PART_ID as MaterialNumber,
# MAGIC V.TO_UM as ToUOM,
# MAGIC V.FROM_UM as FromUOM,
# MAGIC '' as NumeratorForConvToBaseUOM,
# MAGIC '' as DenominatorForConvToBaseUOM,
# MAGIC '' as Length,
# MAGIC '' as Width,
# MAGIC '' as Height,
# MAGIC '' as Volume,
# MAGIC '' as GrossWeight,
# MAGIC '' as WeightUnit,
# MAGIC '' as InternalCharacteristic,
# MAGIC '' as UOMSortNumber,
# MAGIC '' as RemainingVolumeAfterNesting,
# MAGIC '' as MaxStackingFactor,
# MAGIC '' as MaxTopLoadOnFullPackage,
# MAGIC '' as CapacityUsage,
# MAGIC V.CONVERSION_FACTOR as ConversionFactor,
# MAGIC V.DataSource as DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from part_units_conv_tmp as V

# COMMAND ----------

df_unit_of_measure = spark.sql("""select MaterialNumber,
ToUOM,
FromUOM,
NumeratorForConvToBaseUOM,
DenominatorForConvToBaseUOM,
Length,
Width,
Height,
Volume,
GrossWeight,
WeightUnit,
InternalCharacteristic,
UOMSortNumber,
RemainingVolumeAfterNesting,
MaxStackingFactor,
MaxTopLoadOnFullPackage,
CapacityUsage,
ConversionFactor,
DataSource,
UpdatedOn
 from (select ROW_NUMBER() OVER (PARTITION BY MaterialNumber,FromUOM,ToUOM ORDER BY DataSource ASC) as rn,* from unit_of_measure)A where A.rn = 1""")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_unit_of_measure.write.format(write_format).mode("overwrite").partitionBy("DataSource").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_unit_of_measure.createOrReplaceTempView('tmp_unit_of_measure')

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE into fedw.unit_of_measure as T
# MAGIC Using tmp_unit_of_measure as S
# MAGIC on T.MaterialNumber = S.MaterialNumber 
# MAGIC and T.ToUOM = S.ToUOM
# MAGIC and T.FromUOM = S.FromUOM
# MAGIC and 'SAP' = S.DataSource
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.MaterialNumber = S.MaterialNumber,
# MAGIC T.ToUOM = S.ToUOM,
# MAGIC T.FromUOM = S.FromUOM,
# MAGIC T.NumeratorForConvToBaseUOM = S.NumeratorForConvToBaseUOM,
# MAGIC T.DenominatorForConvToBaseUOM = S.DenominatorForConvToBaseUOM,
# MAGIC T.Length = S.Length,
# MAGIC T.Width = S.Width,
# MAGIC T.Height = S.Height,
# MAGIC T.Volume = S.Volume,
# MAGIC T.GrossWeight = S.GrossWeight,
# MAGIC T.WeightUnit = S.WeightUnit,
# MAGIC T.InternalCharacteristic = S.InternalCharacteristic,
# MAGIC T.UOMSortNumber = S.UOMSortNumber,
# MAGIC T.RemainingVolumeAfterNesting = S.RemainingVolumeAfterNesting,
# MAGIC T.MaxStackingFactor = S.MaxStackingFactor,
# MAGIC T.MaxTopLoadOnFullPackage = S.MaxTopLoadOnFullPackage,
# MAGIC T.CapacityUsage = S.CapacityUsage,
# MAGIC T.ConversionFactor = S.ConversionFactor,
# MAGIC T.DataSource = S.DataSource,
# MAGIC T.UpdatedOn = now()
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT
# MAGIC (
# MAGIC MaterialNumber,
# MAGIC ToUOM,
# MAGIC FromUOM,
# MAGIC NumeratorForConvToBaseUOM,
# MAGIC DenominatorForConvToBaseUOM,
# MAGIC Length,
# MAGIC Width,
# MAGIC Height,
# MAGIC Volume,
# MAGIC GrossWeight,
# MAGIC WeightUnit,
# MAGIC InternalCharacteristic,
# MAGIC UOMSortNumber,
# MAGIC RemainingVolumeAfterNesting,
# MAGIC MaxStackingFactor,
# MAGIC MaxTopLoadOnFullPackage,
# MAGIC CapacityUsage,
# MAGIC ConversionFactor,
# MAGIC DataSource,
# MAGIC UpdatedOn
# MAGIC )
# MAGIC VALUES 
# MAGIC (
# MAGIC S.MaterialNumber,
# MAGIC S.ToUOM,
# MAGIC S.FromUOM,
# MAGIC S.NumeratorForConvToBaseUOM,
# MAGIC S.DenominatorForConvToBaseUOM,
# MAGIC S.Length,
# MAGIC S.Width,
# MAGIC S.Height,
# MAGIC S.Volume,
# MAGIC S.GrossWeight,
# MAGIC S.WeightUnit,
# MAGIC S.InternalCharacteristic,
# MAGIC S.UOMSortNumber,
# MAGIC S.RemainingVolumeAfterNesting,
# MAGIC S.MaxStackingFactor,
# MAGIC S.MaxTopLoadOnFullPackage,
# MAGIC S.CapacityUsage,
# MAGIC S.ConversionFactor,
# MAGIC S.DataSource,
# MAGIC now()
# MAGIC )

# COMMAND ----------

if(ts_marm != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'MARM' and TargetLayerTableName ='Unit_Of_Measure' and DatabaseName = 'S42'".format(ts_marm))
    
if(ts_ve70_part_units_conv != None):    
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PART_UNITS_CONV' and TargetLayerTableName ='Unit_Of_Measure' and DatabaseName = 'VE70'".format(ts_ve70_part_units_conv))
    
if(ts_ve72_part_units_conv != None):    
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PART_UNITS_CONV' and TargetLayerTableName ='Unit_Of_Measure' and DatabaseName = 'VE72'".format(ts_ve72_part_units_conv))


# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Unit_Of_Measure where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Unit_Of_Measure' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_unit_of_measure = ts_sf["max(UpdatedOn)"]
print(ts_unit_of_measure)

# COMMAND ----------

sfUrl = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUrl'").first()[0]
sfUser = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUser'").first()[0]
sfPassword = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfPassword'").first()[0]
sfDatabase = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfDatabase'").first()[0]
sfSchema = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfSchema'").first()[0]
sfWarehouse = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfWarehouse'").first()[0]
sfRole = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfRole'").first()[0]
insecureMode = spark.sql("select variable_value from Config.config_constant where variable_name = 'insecureMode'").first()[0]

options = {
  "sfUrl": sfUrl,
  "sfUser": sfUser,
  "sfPassword": sfPassword,
  "sfDatabase": sfDatabase,
  "sfSchema": sfSchema,
  "sfWarehouse": sfWarehouse,
  "sfRole": sfRole,
  "insecureMode": insecureMode
}


# COMMAND ----------

df_sf.write \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("dbtable", "Unit_Of_Measure") \
  .mode("OVERWRITE") \
  .save()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql._
# MAGIC 
# MAGIC val sfUrl = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUrl'").first().getString(0)
# MAGIC val sfUser = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUser'").first().getString(0)
# MAGIC val sfPassword = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfPassword'").first().getString(0)
# MAGIC val sfDatabase = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfDatabase'").first().getString(0)
# MAGIC val sfSchema = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfSchema'").first().getString(0)
# MAGIC val sfWarehouse = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfWarehouse'").first().getString(0)
# MAGIC val sfRole = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfRole'").first().getString(0)
# MAGIC val insecureMode = spark.sql("select variable_value from Config.config_constant where variable_name = 'insecureMode'").first().getString(0)
# MAGIC 
# MAGIC 
# MAGIC val options = Map(
# MAGIC   "sfUrl" -> sfUrl,
# MAGIC   "sfUser" -> sfUser,
# MAGIC   "sfPassword" -> sfPassword,
# MAGIC   "sfDatabase" -> sfDatabase,
# MAGIC   "sfSchema" -> sfSchema,
# MAGIC   "sfWarehouse" -> sfWarehouse,
# MAGIC   "sfRole" -> sfRole,
# MAGIC    "insecureMode" -> insecureMode
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC  
# MAGIC Utils.runQuery(options, """call sp_load_unit_of_measure()""")

# COMMAND ----------

if(ts_unit_of_measure != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Unit_Of_Measure' and DatabaseName = 'FEDW'".format(ts_unit_of_measure))

# COMMAND ----------


