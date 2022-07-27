# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Material_Global'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_mara = ["MATNR",
"ERSDA",
"CREATED_AT_TIME",
"ERNAM",
"LAEDA",
"AENAM",
"VPSTA",
"PSTAT",
"MTART",
"MBRSH",
"MATKL",
"MEINS",
"BRGEW",
"NTGEW",
"GEWEI",
"VOLUM",
"VOLEH",
"RAUBE",
"STOFF",
"SPART",
"KUNNR",
"EAN11",
"NUMTP",
"LAENG",
"BREIT",
"HOEHE",
"MEABM",
"PRDHA",
"ATTYP",
"MSTAE",
"MSTDE",
"/STTPEC/PRDCAT",
"BRAND_ID","UpdatedOn"]

# COMMAND ----------

df = spark.sql("select * from S42.MARA where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'MARA' and DatabaseName = 'S42')")
df_mara = df.select(col_names_mara)
df_mara.createOrReplaceTempView("mara_tmp")
df_ts_mara = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_mara = df_ts_mara["max(UpdatedOn)"]
print(ts_mara)

# COMMAND ----------

df = spark.sql("select * from S42.MAKT where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'MAKT' and TargetLayerTableName = 'Material_Global' and DatabaseName = 'S42')")
df.createOrReplaceTempView("makt_tmp")
df_ts_makt = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_makt = df_ts_makt["max(UpdatedOn)"]
print(ts_makt)

# COMMAND ----------

df = spark.sql("select * from IT_TEMP.UCT_V_AGILE_ITEM_MV where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_V_AGILE_ITEM_MV' and DatabaseName = 'IT_TEMP')")
df.createOrReplaceTempView("UCT_V_AGILE_ITEM_MV_tmp")
df_ts_UCT_V_AGILE_ITEM_MV = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_UCT_V_AGILE_ITEM_MV = df_ts_UCT_V_AGILE_ITEM_MV["max(UpdatedOn)"]
print(ts_UCT_V_AGILE_ITEM_MV)

# COMMAND ----------

df_sap =  spark.sql("select mk.MAKTX,ma.* from mara_tmp ma inner join makt_tmp mk on ma.MATNR = mk.MATNR where mk.SPRAS = 'E' and ma.MATNR not in (select PART_NUMBER from UCT_V_AGILE_ITEM_MV_tmp)")
df_sap.createOrReplaceTempView("mara_makt_tmp")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_material_global_1 as 
# MAGIC select 
# MAGIC V.AGILE_ITEM_ID as Agileitemid,
# MAGIC V.PART_NUMBER  as MaterialNumber,
# MAGIC V.PART_CLASS as Partclass,
# MAGIC V.PART_TYPE as Parttype,
# MAGIC V.LIFECYCLE_PHASE as Lifecyclephase,
# MAGIC V.DESCRIPTION  as Description,
# MAGIC V.DEFAULT_CHANGE as Defaultchange,
# MAGIC V.DEFAULT_CHANGE_REL_DATE as Defaultchangereldate,
# MAGIC V.LATEST_RELEASED_ECO as Latestreleasedeco,
# MAGIC V.LATEST_RELEASED_ECO_REL_DATE as Latestreleasedecoreldate,
# MAGIC V.LATEST_RELEASED_ECO_REV_TYPE as Latestreleasedecorevtype,
# MAGIC V.COMMODITY_CODE as Commoditycode,
# MAGIC ltrim(V.COMMODITY_CODE_SAP) as Commoditycodesap,
# MAGIC V.UOM  as BASEUOMVISUAL,
# MAGIC V.CUSTOMER as Customer,
# MAGIC V.RESTRICTED_ACCESS_GROUP as Restrictedaccessgroup,
# MAGIC V.REV as Rev,
# MAGIC V.DEMAND_FACILITY as Demandfacility,
# MAGIC V.COPY_EXACT as Copyexact,
# MAGIC V.CP_CLASSIFICATION as Cpclassification,
# MAGIC V.CRITICAL_ASPECT as Criticalaspect,
# MAGIC V.CRITICAL_PART as Criticalpart,
# MAGIC V.SUPPLIER_LOCKED as Supplierlocked,
# MAGIC V.PRODUCT_CODE as Productcode,
# MAGIC V.MATERIAL_TYPE  as MaterialtypeVisual,
# MAGIC V.UMC as Umc,
# MAGIC V.ROUTING_TEMPLATE as Routingtemplate,
# MAGIC V.MATERIAL_SUBTYPE as Materialsubtype,
# MAGIC V.CBA as Cba,
# MAGIC V.CBR_NO as Cbrno,
# MAGIC V.CZBA as Czba,
# MAGIC V.CZBR_NO as Czbrno,
# MAGIC V.SBA as Sba,
# MAGIC V.SBR_NO as Sbrno,
# MAGIC V.IBA as Iba,
# MAGIC V.IBR_NO as Ibrno,
# MAGIC V.PHBA as Phba,
# MAGIC V.PHBR_NO as Phbrno,
# MAGIC V.MYBA as Myba,
# MAGIC V.MYBR_NO as Mybrno,
# MAGIC V.CUSTOMER_101 as Customer101,
# MAGIC V.CUSTOMER_104 as Customer104,
# MAGIC V.CUSTOMER_107 as Customer107,
# MAGIC V.CUSTOMER_111_AND_102 as Customer111And102,
# MAGIC V.CUSTOMER_113 as Customer113,
# MAGIC V.CUSTOMER_133 as Customer133,
# MAGIC V.CUSTOMER_115 as Customer115,
# MAGIC V.CUSTOMER_118 as Customer118,
# MAGIC V.CUSTOMER_123 as Customer123,
# MAGIC V.CUSTOMER_128 as Customer128,
# MAGIC V.CUSTOMER_129 as Customer129,
# MAGIC V.CUSTOMER_131 as Customer131,
# MAGIC V.CUSTOMER_134 as Customer134,
# MAGIC V.CUSTOMER_140 as Customer140,
# MAGIC V.CUSTOMER_141 as Customer141,
# MAGIC V.CUSTOMER_146 as Customer146,
# MAGIC V.CUSTOMER_150 as Customer150,
# MAGIC V.CUSTOMER_154 as Customer154,
# MAGIC V.CUSTOMER_156 as Customer156,
# MAGIC V.CUSTOMER_158 as Customer158,
# MAGIC V.CUSTOMER_164 as Customer164,
# MAGIC V.CUSTOMER_168 as Customer168,
# MAGIC V.CUSTOMER_169 as Customer169,
# MAGIC V.CUSTOMER_170 as Customer170,
# MAGIC V.CUSTOMER_179 as Customer179,
# MAGIC V.CUSTOMER_182 as Customer182,
# MAGIC V.CUSTOMER_183 as Customer183,
# MAGIC V.CUSTOMER_267 as Customer267,
# MAGIC V.MARCHI_PN as Marchipn,
# MAGIC V.LEGACY_PN as Legacypn,
# MAGIC V.LEGACY_SYSTEM as Legacysystem,
# MAGIC V.ECCN_NO as Eccnno,
# MAGIC V.HTS as Hts,
# MAGIC V.SCHEDULE_B as Scheduleb,
# MAGIC V.RELEASE_TYPE as Releasetype,
# MAGIC V.SUB_MPN_BOM as Submpnbom,
# MAGIC V.DISP_ONORDER as Disponorder,
# MAGIC V.DISP_WIP as Dispwip,
# MAGIC V.DISP_FG as Dispfg,
# MAGIC V.DISP_STOCKROOM as Dispstockroom,
# MAGIC V.DISP_FIELD as Dispfield,
# MAGIC V.CREATE_USER as Createuser,
# MAGIC V.CREATE_DATE as Createdate,
# MAGIC V.CPDS_FILE_COUNT as Cpdsfilecount,
# MAGIC V.LandingFileTimeStamp as Landingfiletimestamp,
# MAGIC 'IT_TEMP' as DataSource
# MAGIC from UCT_V_AGILE_ITEM_MV_tmp as V
# MAGIC 
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC select 
# MAGIC '' as Agileitemid,
# MAGIC S.`MATNR` as  MaterialNumber,
# MAGIC '' as Partclass,
# MAGIC '' as Parttype,
# MAGIC '' as Lifecyclephase,
# MAGIC S.`MAKTX` as Description,
# MAGIC '' as Defaultchange,
# MAGIC '' as Defaultchangereldate,
# MAGIC '' as Latestreleasedeco,
# MAGIC '' as Latestreleasedecoreldate,
# MAGIC '' as Latestreleasedecorevtype,
# MAGIC '' as Commoditycode,
# MAGIC '' as Commoditycodesap,
# MAGIC S.`MEINS` as BaseUOMVISUAL,
# MAGIC '' as Customer,
# MAGIC '' as Restrictedaccessgroup,
# MAGIC '' as Rev,
# MAGIC '' as Demandfacility,
# MAGIC '' as Copyexact,
# MAGIC '' as Cpclassification,
# MAGIC '' as Criticalaspect,
# MAGIC '' as Criticalpart,
# MAGIC '' as Supplierlocked,
# MAGIC '' as Productcode,
# MAGIC '' as MaterialtypeVisual,
# MAGIC '' as Umc,
# MAGIC '' as Routingtemplate,
# MAGIC '' as Materialsubtype,
# MAGIC '' as Cba,
# MAGIC '' as Cbrno,
# MAGIC '' as Czba,
# MAGIC '' as Czbrno,
# MAGIC '' as Sba,
# MAGIC '' as Sbrno,
# MAGIC '' as Iba,
# MAGIC '' as Ibrno,
# MAGIC '' as Phba,
# MAGIC '' as Phbrno,
# MAGIC '' as Myba,
# MAGIC '' as Mybrno,
# MAGIC '' as Customer101,
# MAGIC '' as Customer104,
# MAGIC '' as Customer107,
# MAGIC '' as Customer111And102,
# MAGIC '' as Customer113,
# MAGIC '' as Customer133,
# MAGIC '' as Customer115,
# MAGIC '' as Customer118,
# MAGIC '' as Customer123,
# MAGIC '' as Customer128,
# MAGIC '' as Customer129,
# MAGIC '' as Customer131,
# MAGIC '' as Customer134,
# MAGIC '' as Customer140,
# MAGIC '' as Customer141,
# MAGIC '' as Customer146,
# MAGIC '' as Customer150,
# MAGIC '' as Customer154,
# MAGIC '' as Customer156,
# MAGIC '' as Customer158,
# MAGIC '' as Customer164,
# MAGIC '' as Customer168,
# MAGIC '' as Customer169,
# MAGIC '' as Customer170,
# MAGIC '' as Customer179,
# MAGIC '' as Customer182,
# MAGIC '' as Customer183,
# MAGIC '' as Customer267,
# MAGIC '' as Marchipn,
# MAGIC '' as Legacypn,
# MAGIC '' as Legacysystem,
# MAGIC '' as Eccnno,
# MAGIC '' as Hts,
# MAGIC '' as Scheduleb,
# MAGIC '' as Releasetype,
# MAGIC '' as Submpnbom,
# MAGIC '' as Disponorder,
# MAGIC '' as Dispwip,
# MAGIC '' as Dispfg,
# MAGIC '' as Dispstockroom,
# MAGIC '' as Dispfield,
# MAGIC S.`ERNAM` as Createuser,
# MAGIC S.`ERSDA` as Createdate,
# MAGIC '' as Cpdsfilecount,
# MAGIC '' as Landingfiletimestamp,
# MAGIC 'SAP' as DataSource
# MAGIC from mara_makt_tmp as S

# COMMAND ----------

df_sap1 =  spark.sql("select * from merge_material_global_1")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_material_global as 
# MAGIC select 
# MAGIC V.Agileitemid,
# MAGIC V.MaterialNumber,
# MAGIC V.Partclass,
# MAGIC V.Parttype,
# MAGIC V.Lifecyclephase,
# MAGIC V.Description,
# MAGIC V.Defaultchange,
# MAGIC V.Defaultchangereldate,
# MAGIC V.Latestreleasedeco,
# MAGIC V.Latestreleasedecoreldate,
# MAGIC V.Latestreleasedecorevtype,
# MAGIC V.Commoditycode,
# MAGIC V.Commoditycodesap,
# MAGIC V.BaseUOMVISUAL,
# MAGIC S.`MEINS` as BaseUOMSAP,
# MAGIC V.Customer,
# MAGIC V.Restrictedaccessgroup,
# MAGIC V.Rev,
# MAGIC V.Demandfacility,
# MAGIC V.Copyexact,
# MAGIC V.Cpclassification,
# MAGIC V.Criticalaspect,
# MAGIC V.Criticalpart,
# MAGIC V.Supplierlocked,
# MAGIC V.Productcode,
# MAGIC V.MaterialtypeVisual,
# MAGIC S.`MTART` as MaterialtypeSAP,
# MAGIC V.Umc,
# MAGIC V.Routingtemplate,
# MAGIC V.Materialsubtype,
# MAGIC V.Cba,
# MAGIC V.Cbrno,
# MAGIC V.Czba,
# MAGIC V.Czbrno,
# MAGIC V.Sba,
# MAGIC V.Sbrno,
# MAGIC V.Iba,
# MAGIC V.Ibrno,
# MAGIC V.Phba,
# MAGIC V.Phbrno,
# MAGIC V.Myba,
# MAGIC V.Mybrno,
# MAGIC V.Customer101,
# MAGIC V.Customer104,
# MAGIC V.Customer107,
# MAGIC V.Customer111And102,
# MAGIC V.Customer113,
# MAGIC V.Customer133,
# MAGIC V.Customer115,
# MAGIC V.Customer118,
# MAGIC V.Customer123,
# MAGIC V.Customer128,
# MAGIC V.Customer129,
# MAGIC V.Customer131,
# MAGIC V.Customer134,
# MAGIC V.Customer140,
# MAGIC V.Customer141,
# MAGIC V.Customer146,
# MAGIC V.Customer150,
# MAGIC V.Customer154,
# MAGIC V.Customer156,
# MAGIC V.Customer158,
# MAGIC V.Customer164,
# MAGIC V.Customer168,
# MAGIC V.Customer169,
# MAGIC V.Customer170,
# MAGIC V.Customer179,
# MAGIC V.Customer182,
# MAGIC V.Customer183,
# MAGIC V.Customer267,
# MAGIC V.Marchipn,
# MAGIC V.Legacypn,
# MAGIC V.Legacysystem,
# MAGIC V.Eccnno,
# MAGIC V.Hts,
# MAGIC V.Scheduleb,
# MAGIC V.Releasetype,
# MAGIC V.Submpnbom,
# MAGIC V.Disponorder,
# MAGIC V.Dispwip,
# MAGIC V.Dispfg,
# MAGIC V.Dispstockroom,
# MAGIC V.Dispfield,
# MAGIC V.Createuser,
# MAGIC V.Createdate,
# MAGIC V.Cpdsfilecount,
# MAGIC V.Landingfiletimestamp,
# MAGIC S.`CREATED_AT_TIME` as CreatedAtTime,
# MAGIC S.`LAEDA` as LastChange,
# MAGIC S.`AENAM` as ChangedBy,
# MAGIC S.`VPSTA` as CompleteStatus,
# MAGIC S.`PSTAT` as MaintenanceStatus,
# MAGIC S.`MBRSH` as IndustrySector,
# MAGIC S.`MATKL` as MaterialGroup,
# MAGIC S.`BRGEW` as GrossWeight,
# MAGIC S.`NTGEW` as NetWeight,
# MAGIC S.`GEWEI` as WeightUnit,
# MAGIC S.`VOLUM` as Volume,
# MAGIC S.`VOLEH` as VolumeUnit,
# MAGIC S.`RAUBE` as StorageConditions,
# MAGIC S.`STOFF` as HazardousMaterialNumber,
# MAGIC S.`SPART` as Division,
# MAGIC S.`KUNNR` as Competitor,
# MAGIC S.`EAN11` as InternationalArticleNum,
# MAGIC S.`NUMTP` as InternationalArticleNumCategory,
# MAGIC S.`LAENG` as Length,
# MAGIC S.`BREIT` as Width,
# MAGIC S.`HOEHE` as Height,
# MAGIC S.`MEABM` as UnitOfDim,
# MAGIC S.`PRDHA` as ProductHierarchy,
# MAGIC S.`ATTYP` as MaterialCategory,
# MAGIC S.`MSTAE` as CrossPlantMatStatus,
# MAGIC S.`MSTDE` as CrossPlantMatStatusValid,
# MAGIC S.`/STTPEC/PRDCAT` as ProductCategory,
# MAGIC S.`BRAND_ID` as Brand,
# MAGIC now() as UpdatedOn,
# MAGIC V.datasource
# MAGIC from merge_material_global_1 as V
# MAGIC left join mara_tmp as S
# MAGIC on V.MaterialNumber = S.MATNR 

# COMMAND ----------

df_merge = spark.sql("select * from merge_material_global")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_material_global')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.material_global as T 
# MAGIC USING stg_material_global as S 
# MAGIC ON T.MaterialNumber = S.MaterialNumber
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Agileitemid = S.Agileitemid,
# MAGIC T.MaterialNumber = S.MaterialNumber,
# MAGIC T.Partclass = S.Partclass,
# MAGIC T.Parttype = S.Parttype,
# MAGIC T.Lifecyclephase = S.Lifecyclephase,
# MAGIC T.Description = S.Description,
# MAGIC T.Defaultchange = S.Defaultchange,
# MAGIC T.Defaultchangereldate = S.Defaultchangereldate,
# MAGIC T.Latestreleasedeco = S.Latestreleasedeco,
# MAGIC T.Latestreleasedecoreldate = S.Latestreleasedecoreldate,
# MAGIC T.Latestreleasedecorevtype = S.Latestreleasedecorevtype,
# MAGIC T.Commoditycode = S.Commoditycode,
# MAGIC T.Commoditycodesap = S.Commoditycodesap,
# MAGIC T.BaseUOMVISUAL = S.BaseUOMVISUAL,
# MAGIC T.BaseUOMSAP = S.BaseUOMSAP,
# MAGIC T.Customer = S.Customer,
# MAGIC T.Restrictedaccessgroup = S.Restrictedaccessgroup,
# MAGIC T.Rev = S.Rev,
# MAGIC T.Demandfacility = S.Demandfacility,
# MAGIC T.Copyexact = S.Copyexact,
# MAGIC T.Cpclassification = S.Cpclassification,
# MAGIC T.Criticalaspect = S.Criticalaspect,
# MAGIC T.Criticalpart = S.Criticalpart,
# MAGIC T.Supplierlocked = S.Supplierlocked,
# MAGIC T.Productcode = S.Productcode,
# MAGIC T.MaterialtypeVisual = S.MaterialtypeVisual,
# MAGIC T.MaterialtypeSAP = S.MaterialtypeSAP,
# MAGIC T.Umc = S.Umc,
# MAGIC T.Routingtemplate = S.Routingtemplate,
# MAGIC T.Materialsubtype = S.Materialsubtype,
# MAGIC T.Cba = S.Cba,
# MAGIC T.Cbrno = S.Cbrno,
# MAGIC T.Czba = S.Czba,
# MAGIC T.Czbrno = S.Czbrno,
# MAGIC T.Sba = S.Sba,
# MAGIC T.Sbrno = S.Sbrno,
# MAGIC T.Iba = S.Iba,
# MAGIC T.Ibrno = S.Ibrno,
# MAGIC T.Phba = S.Phba,
# MAGIC T.Phbrno = S.Phbrno,
# MAGIC T.Myba = S.Myba,
# MAGIC T.Mybrno = S.Mybrno,
# MAGIC T.Customer101 = S.Customer101,
# MAGIC T.Customer104 = S.Customer104,
# MAGIC T.Customer107 = S.Customer107,
# MAGIC T.Customer111And102 = S.Customer111And102,
# MAGIC T.Customer113 = S.Customer113,
# MAGIC T.Customer133 = S.Customer133,
# MAGIC T.Customer115 = S.Customer115,
# MAGIC T.Customer118 = S.Customer118,
# MAGIC T.Customer123 = S.Customer123,
# MAGIC T.Customer128 = S.Customer128,
# MAGIC T.Customer129 = S.Customer129,
# MAGIC T.Customer131 = S.Customer131,
# MAGIC T.Customer134 = S.Customer134,
# MAGIC T.Customer140 = S.Customer140,
# MAGIC T.Customer141 = S.Customer141,
# MAGIC T.Customer146 = S.Customer146,
# MAGIC T.Customer150 = S.Customer150,
# MAGIC T.Customer154 = S.Customer154,
# MAGIC T.Customer156 = S.Customer156,
# MAGIC T.Customer158 = S.Customer158,
# MAGIC T.Customer164 = S.Customer164,
# MAGIC T.Customer168 = S.Customer168,
# MAGIC T.Customer169 = S.Customer169,
# MAGIC T.Customer170 = S.Customer170,
# MAGIC T.Customer179 = S.Customer179,
# MAGIC T.Customer182 = S.Customer182,
# MAGIC T.Customer183 = S.Customer183,
# MAGIC T.Customer267 = S.Customer267,
# MAGIC T.Marchipn = S.Marchipn,
# MAGIC T.Legacypn = S.Legacypn,
# MAGIC T.Legacysystem = S.Legacysystem,
# MAGIC T.Eccnno = S.Eccnno,
# MAGIC T.Hts = S.Hts,
# MAGIC T.Scheduleb = S.Scheduleb,
# MAGIC T.Releasetype = S.Releasetype,
# MAGIC T.Submpnbom = S.Submpnbom,
# MAGIC T.Disponorder = S.Disponorder,
# MAGIC T.Dispwip = S.Dispwip,
# MAGIC T.Dispfg = S.Dispfg,
# MAGIC T.Dispstockroom = S.Dispstockroom,
# MAGIC T.Dispfield = S.Dispfield,
# MAGIC T.Createuser = S.Createuser,
# MAGIC T.Createdate = S.Createdate,
# MAGIC T.Cpdsfilecount = S.Cpdsfilecount,
# MAGIC T.Landingfiletimestamp = S.Landingfiletimestamp,
# MAGIC T.CreatedAtTime = S.CreatedAtTime,
# MAGIC T.LastChange = S.LastChange,
# MAGIC T.ChangedBy = S.ChangedBy,
# MAGIC T.CompleteStatus = S.CompleteStatus,
# MAGIC T.MaintenanceStatus = S.MaintenanceStatus,
# MAGIC T.IndustrySector = S.IndustrySector,
# MAGIC T.MaterialGroup = S.MaterialGroup,
# MAGIC T.GrossWeight = S.GrossWeight,
# MAGIC T.NetWeight = S.NetWeight,
# MAGIC T.WeightUnit = S.WeightUnit,
# MAGIC T.Volume = S.Volume,
# MAGIC T.VolumeUnit = S.VolumeUnit,
# MAGIC T.StorageConditions = S.StorageConditions,
# MAGIC T.HazardousMaterialNumber = S.HazardousMaterialNumber,
# MAGIC T.Division = S.Division,
# MAGIC T.Competitor = S.Competitor,
# MAGIC T.InternationalArticleNum = S.InternationalArticleNum,
# MAGIC T.InternationalArticleNumCategory = S.InternationalArticleNumCategory,
# MAGIC T.Length = S.Length,
# MAGIC T.Width = S.Width,
# MAGIC T.Height = S.Height,
# MAGIC T.UnitOfDim = S.UnitOfDim,
# MAGIC T.ProductHierarchy = S.ProductHierarchy,
# MAGIC T.MaterialCategory = S.MaterialCategory,
# MAGIC T.CrossPlantMatStatus = S.CrossPlantMatStatus,
# MAGIC T.CrossPlantMatStatusValid = S.CrossPlantMatStatusValid,
# MAGIC T.ProductCategory = S.ProductCategory,
# MAGIC T.Brand = S.Brand,
# MAGIC T.UpdatedOn = now(),
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Agileitemid,
# MAGIC MaterialNumber,
# MAGIC Partclass,
# MAGIC Parttype,
# MAGIC Lifecyclephase,
# MAGIC Description,
# MAGIC Defaultchange,
# MAGIC Defaultchangereldate,
# MAGIC Latestreleasedeco,
# MAGIC Latestreleasedecoreldate,
# MAGIC Latestreleasedecorevtype,
# MAGIC Commoditycode,
# MAGIC Commoditycodesap,
# MAGIC BaseUOMVISUAL,
# MAGIC BaseUOMSAP,
# MAGIC Customer,
# MAGIC Restrictedaccessgroup,
# MAGIC Rev,
# MAGIC Demandfacility,
# MAGIC Copyexact,
# MAGIC Cpclassification,
# MAGIC Criticalaspect,
# MAGIC Criticalpart,
# MAGIC Supplierlocked,
# MAGIC Productcode,
# MAGIC MaterialtypeVisual,
# MAGIC MaterialtypeSAP,
# MAGIC Umc,
# MAGIC Routingtemplate,
# MAGIC Materialsubtype,
# MAGIC Cba,
# MAGIC Cbrno,
# MAGIC Czba,
# MAGIC Czbrno,
# MAGIC Sba,
# MAGIC Sbrno,
# MAGIC Iba,
# MAGIC Ibrno,
# MAGIC Phba,
# MAGIC Phbrno,
# MAGIC Myba,
# MAGIC Mybrno,
# MAGIC Customer101,
# MAGIC Customer104,
# MAGIC Customer107,
# MAGIC Customer111And102,
# MAGIC Customer113,
# MAGIC Customer133,
# MAGIC Customer115,
# MAGIC Customer118,
# MAGIC Customer123,
# MAGIC Customer128,
# MAGIC Customer129,
# MAGIC Customer131,
# MAGIC Customer134,
# MAGIC Customer140,
# MAGIC Customer141,
# MAGIC Customer146,
# MAGIC Customer150,
# MAGIC Customer154,
# MAGIC Customer156,
# MAGIC Customer158,
# MAGIC Customer164,
# MAGIC Customer168,
# MAGIC Customer169,
# MAGIC Customer170,
# MAGIC Customer179,
# MAGIC Customer182,
# MAGIC Customer183,
# MAGIC Customer267,
# MAGIC Marchipn,
# MAGIC Legacypn,
# MAGIC Legacysystem,
# MAGIC Eccnno,
# MAGIC Hts,
# MAGIC Scheduleb,
# MAGIC Releasetype,
# MAGIC Submpnbom,
# MAGIC Disponorder,
# MAGIC Dispwip,
# MAGIC Dispfg,
# MAGIC Dispstockroom,
# MAGIC Dispfield,
# MAGIC Createuser,
# MAGIC Createdate,
# MAGIC Cpdsfilecount,
# MAGIC Landingfiletimestamp,
# MAGIC CreatedAtTime,
# MAGIC LastChange,
# MAGIC ChangedBy,
# MAGIC CompleteStatus,
# MAGIC MaintenanceStatus,
# MAGIC IndustrySector,
# MAGIC MaterialGroup,
# MAGIC GrossWeight,
# MAGIC NetWeight,
# MAGIC WeightUnit,
# MAGIC Volume,
# MAGIC VolumeUnit,
# MAGIC StorageConditions,
# MAGIC HazardousMaterialNumber,
# MAGIC Division,
# MAGIC Competitor,
# MAGIC InternationalArticleNum,
# MAGIC InternationalArticleNumCategory,
# MAGIC Length,
# MAGIC Width,
# MAGIC Height,
# MAGIC UnitOfDim,
# MAGIC ProductHierarchy,
# MAGIC MaterialCategory,
# MAGIC CrossPlantMatStatus,
# MAGIC CrossPlantMatStatusValid,
# MAGIC ProductCategory,
# MAGIC Brand,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC Values (
# MAGIC S.Agileitemid,
# MAGIC S.MaterialNumber,
# MAGIC S.Partclass,
# MAGIC S.Parttype,
# MAGIC S.Lifecyclephase,
# MAGIC S.Description,
# MAGIC S.Defaultchange,
# MAGIC S.Defaultchangereldate,
# MAGIC S.Latestreleasedeco,
# MAGIC S.Latestreleasedecoreldate,
# MAGIC S.Latestreleasedecorevtype,
# MAGIC S.Commoditycode,
# MAGIC S.Commoditycodesap,
# MAGIC S.BaseUOMVISUAL,
# MAGIC S.BaseUOMSAP,
# MAGIC S.Customer,
# MAGIC S.Restrictedaccessgroup,
# MAGIC S.Rev,
# MAGIC S.Demandfacility,
# MAGIC S.Copyexact,
# MAGIC S.Cpclassification,
# MAGIC S.Criticalaspect,
# MAGIC S.Criticalpart,
# MAGIC S.Supplierlocked,
# MAGIC S.Productcode,
# MAGIC S.MaterialtypeVisual,
# MAGIC S.MaterialtypeSAP,
# MAGIC S.Umc,
# MAGIC S.Routingtemplate,
# MAGIC S.Materialsubtype,
# MAGIC S.Cba,
# MAGIC S.Cbrno,
# MAGIC S.Czba,
# MAGIC S.Czbrno,
# MAGIC S.Sba,
# MAGIC S.Sbrno,
# MAGIC S.Iba,
# MAGIC S.Ibrno,
# MAGIC S.Phba,
# MAGIC S.Phbrno,
# MAGIC S.Myba,
# MAGIC S.Mybrno,
# MAGIC S.Customer101,
# MAGIC S.Customer104,
# MAGIC S.Customer107,
# MAGIC S.Customer111And102,
# MAGIC S.Customer113,
# MAGIC S.Customer133,
# MAGIC S.Customer115,
# MAGIC S.Customer118,
# MAGIC S.Customer123,
# MAGIC S.Customer128,
# MAGIC S.Customer129,
# MAGIC S.Customer131,
# MAGIC S.Customer134,
# MAGIC S.Customer140,
# MAGIC S.Customer141,
# MAGIC S.Customer146,
# MAGIC S.Customer150,
# MAGIC S.Customer154,
# MAGIC S.Customer156,
# MAGIC S.Customer158,
# MAGIC S.Customer164,
# MAGIC S.Customer168,
# MAGIC S.Customer169,
# MAGIC S.Customer170,
# MAGIC S.Customer179,
# MAGIC S.Customer182,
# MAGIC S.Customer183,
# MAGIC S.Customer267,
# MAGIC S.Marchipn,
# MAGIC S.Legacypn,
# MAGIC S.Legacysystem,
# MAGIC S.Eccnno,
# MAGIC S.Hts,
# MAGIC S.Scheduleb,
# MAGIC S.Releasetype,
# MAGIC S.Submpnbom,
# MAGIC S.Disponorder,
# MAGIC S.Dispwip,
# MAGIC S.Dispfg,
# MAGIC S.Dispstockroom,
# MAGIC S.Dispfield,
# MAGIC S.Createuser,
# MAGIC S.Createdate,
# MAGIC S.Cpdsfilecount,
# MAGIC S.Landingfiletimestamp,
# MAGIC S.CreatedAtTime,
# MAGIC S.LastChange,
# MAGIC S.ChangedBy,
# MAGIC S.CompleteStatus,
# MAGIC S.MaintenanceStatus,
# MAGIC S.IndustrySector,
# MAGIC S.MaterialGroup,
# MAGIC S.GrossWeight,
# MAGIC S.NetWeight,
# MAGIC S.WeightUnit,
# MAGIC S.Volume,
# MAGIC S.VolumeUnit,
# MAGIC S.StorageConditions,
# MAGIC S.HazardousMaterialNumber,
# MAGIC S.Division,
# MAGIC S.Competitor,
# MAGIC S.InternationalArticleNum,
# MAGIC S.InternationalArticleNumCategory,
# MAGIC S.Length,
# MAGIC S.Width,
# MAGIC S.Height,
# MAGIC S.UnitOfDim,
# MAGIC S.ProductHierarchy,
# MAGIC S.MaterialCategory,
# MAGIC S.CrossPlantMatStatus,
# MAGIC S.CrossPlantMatStatusValid,
# MAGIC S.ProductCategory,
# MAGIC S.Brand,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_mara != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'MARA' and DatabaseName = 'S42'".format(ts_mara))

if(ts_makt != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'MAKT' and DatabaseName = 'S42'".format(ts_makt))

if(ts_UCT_V_AGILE_ITEM_MV != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_V_AGILE_ITEM_MV' and DatabaseName = 'IT_TEMP'".format(ts_UCT_V_AGILE_ITEM_MV))


# COMMAND ----------

df_sf = spark.sql("select * from FEDW.material_global where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Global' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_material_global = ts_sf["max(UpdatedOn)"]
print(ts_material_global)

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
  .option("dbtable", "Material_Global") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_GLOBALMATERIAL();""")

# COMMAND ----------

if(ts_material_global != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Global' and DatabaseName = 'FEDW'".format(ts_material_global))

# COMMAND ----------


