# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'UCT_V_AGILE_ITEM_MV'
read_format = 'csv'
write_format = 'delta'
database_name = 'IT_TEMP'
delimiter = '^'
read_path = '/mnt/uct-landing-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/VISUAL/'+database_name+'/'+table_name+'/'
stage_view = 'stg_'+table_name

# COMMAND ----------

schema = StructType([ \
StructField('AGILE_ITEM_ID',StringType(),True),\
StructField('PART_NUMBER',StringType(),True),\
StructField('PART_CLASS',StringType(),True),\
StructField('PART_TYPE',StringType(),True),\
StructField('LIFECYCLE_PHASE',StringType(),True),\
StructField('DESCRIPTION',StringType(),True),\
StructField('DEFAULT_CHANGE',StringType(),True),\
StructField('DEFAULT_CHANGE_REL_DATE',StringType(),True),\
StructField('LATEST_RELEASED_ECO',StringType(),True),\
StructField('LATEST_RELEASED_ECO_REL_DATE',StringType(),True),\
StructField('LATEST_RELEASED_ECO_REV_TYPE',StringType(),True),\
StructField('COMMODITY_CODE',StringType(),True),\
StructField('COMMODITY_CODE_SAP',StringType(),True),\
StructField('UOM',StringType(),True),\
StructField('CUSTOMER',StringType(),True),\
StructField('RESTRICTED_ACCESS_GROUP',StringType(),True),\
StructField('REV',StringType(),True),\
StructField('DEMAND_FACILITY',StringType(),True),\
StructField('COPY_EXACT',StringType(),True),\
StructField('CP_CLASSIFICATION',StringType(),True),\
StructField('CRITICAL_ASPECT',StringType(),True),\
StructField('CRITICAL_PART',StringType(),True),\
StructField('SUPPLIER_LOCKED',StringType(),True),\
StructField('PRODUCT_CODE',StringType(),True),\
StructField('MATERIAL_TYPE',StringType(),True),\
StructField('UMC',StringType(),True),\
StructField('ROUTING_TEMPLATE',StringType(),True),\
StructField('MATERIAL_SUBTYPE',StringType(),True),\
StructField('CBA',StringType(),True),\
StructField('CBR_NO',StringType(),True),\
StructField('CZBA',StringType(),True),\
StructField('CZBR_NO',StringType(),True),\
StructField('SBA',StringType(),True),\
StructField('SBR_NO',StringType(),True),\
StructField('IBA',StringType(),True),\
StructField('IBR_NO',StringType(),True),\
StructField('PHBA',StringType(),True),\
StructField('PHBR_NO',StringType(),True),\
StructField('MYBA',StringType(),True),\
StructField('MYBR_NO',StringType(),True),\
StructField('CUSTOMER_101',StringType(),True),\
StructField('CUSTOMER_104',StringType(),True),\
StructField('CUSTOMER_107',StringType(),True),\
StructField('CUSTOMER_111_AND_102',StringType(),True),\
StructField('CUSTOMER_113',StringType(),True),\
StructField('CUSTOMER_133',StringType(),True),\
StructField('CUSTOMER_115',StringType(),True),\
StructField('CUSTOMER_118',StringType(),True),\
StructField('CUSTOMER_123',StringType(),True),\
StructField('CUSTOMER_128',StringType(),True),\
StructField('CUSTOMER_129',StringType(),True),\
StructField('CUSTOMER_131',StringType(),True),\
StructField('CUSTOMER_134',StringType(),True),\
StructField('CUSTOMER_140',StringType(),True),\
StructField('CUSTOMER_141',StringType(),True),\
StructField('CUSTOMER_146',StringType(),True),\
StructField('CUSTOMER_150',StringType(),True),\
StructField('CUSTOMER_154',StringType(),True),\
StructField('CUSTOMER_156',StringType(),True),\
StructField('CUSTOMER_158',StringType(),True),\
StructField('CUSTOMER_164',StringType(),True),\
StructField('CUSTOMER_168',StringType(),True),\
StructField('CUSTOMER_169',StringType(),True),\
StructField('CUSTOMER_170',StringType(),True),\
StructField('CUSTOMER_179',StringType(),True),\
StructField('CUSTOMER_182',StringType(),True),\
StructField('CUSTOMER_183',StringType(),True),\
StructField('CUSTOMER_267',StringType(),True),\
StructField('MARCHI_PN',StringType(),True),\
StructField('LEGACY_PN',StringType(),True),\
StructField('LEGACY_SYSTEM',StringType(),True),\
StructField('ECCN_NO',StringType(),True),\
StructField('HTS',StringType(),True),\
StructField('SCHEDULE_B',StringType(),True),\
StructField('RELEASE_TYPE',StringType(),True),\
StructField('SUB_MPN_BOM',StringType(),True),\
StructField('DISP_ONORDER',StringType(),True),\
StructField('DISP_WIP',StringType(),True),\
StructField('DISP_FG',StringType(),True),\
StructField('DISP_STOCKROOM',StringType(),True),\
StructField('DISP_FIELD',StringType(),True),\
StructField('CREATE_USER',StringType(),True),\
StructField('CREATE_DATE',StringType(),True),\
StructField('CPDS_FILE_COUNT',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
                      ])


# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('IT_TEMP'))

# COMMAND ----------

df_transform = df_add_column.withColumn("DEFAULT_CHANGE_REL_DATE", to_timestamp(regexp_replace(df_add_column.DEFAULT_CHANGE_REL_DATE,'\.','-'))) \
                            .withColumn("LATEST_RELEASED_ECO_REL_DATE", to_timestamp(regexp_replace(df_add_column.LATEST_RELEASED_ECO_REL_DATE,'\.','-'))) \
                            .withColumn("CREATE_DATE", to_timestamp(regexp_replace(df_add_column.CREATE_DATE,'\.','-'))) \
                            .withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO IT_TEMP.UCT_V_AGILE_ITEM_MV as T
# MAGIC USING (select * from (select ROW_NUMBER() OVER (PARTITION BY PART_NUMBER ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_UCT_V_AGILE_ITEM_MV)A where A.rn = 1 ) as S 
# MAGIC ON 
# MAGIC T.PART_NUMBER = S.PART_NUMBER
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`AGILE_ITEM_ID` = S.`AGILE_ITEM_ID`,
# MAGIC T.`PART_NUMBER` = S.`PART_NUMBER`,
# MAGIC T.`PART_CLASS` = S.`PART_CLASS`,
# MAGIC T.`PART_TYPE` = S.`PART_TYPE`,
# MAGIC T.`LIFECYCLE_PHASE` = S.`LIFECYCLE_PHASE`,
# MAGIC T.`DESCRIPTION` = S.`DESCRIPTION`,
# MAGIC T.`DEFAULT_CHANGE` = S.`DEFAULT_CHANGE`,
# MAGIC T.`DEFAULT_CHANGE_REL_DATE` = S.`DEFAULT_CHANGE_REL_DATE`,
# MAGIC T.`LATEST_RELEASED_ECO` = S.`LATEST_RELEASED_ECO`,
# MAGIC T.`LATEST_RELEASED_ECO_REL_DATE` = S.`LATEST_RELEASED_ECO_REL_DATE`,
# MAGIC T.`LATEST_RELEASED_ECO_REV_TYPE` = S.`LATEST_RELEASED_ECO_REV_TYPE`,
# MAGIC T.`COMMODITY_CODE` = S.`COMMODITY_CODE`,
# MAGIC T.`COMMODITY_CODE_SAP` = S.`COMMODITY_CODE_SAP`,
# MAGIC T.`UOM` = S.`UOM`,
# MAGIC T.`CUSTOMER` = S.`CUSTOMER`,
# MAGIC T.`RESTRICTED_ACCESS_GROUP` = S.`RESTRICTED_ACCESS_GROUP`,
# MAGIC T.`REV` = S.`REV`,
# MAGIC T.`DEMAND_FACILITY` = S.`DEMAND_FACILITY`,
# MAGIC T.`COPY_EXACT` = S.`COPY_EXACT`,
# MAGIC T.`CP_CLASSIFICATION` = S.`CP_CLASSIFICATION`,
# MAGIC T.`CRITICAL_ASPECT` = S.`CRITICAL_ASPECT`,
# MAGIC T.`CRITICAL_PART` = S.`CRITICAL_PART`,
# MAGIC T.`SUPPLIER_LOCKED` = S.`SUPPLIER_LOCKED`,
# MAGIC T.`PRODUCT_CODE` = S.`PRODUCT_CODE`,
# MAGIC T.`MATERIAL_TYPE` = S.`MATERIAL_TYPE`,
# MAGIC T.`UMC` = S.`UMC`,
# MAGIC T.`ROUTING_TEMPLATE` = S.`ROUTING_TEMPLATE`,
# MAGIC T.`MATERIAL_SUBTYPE` = S.`MATERIAL_SUBTYPE`,
# MAGIC T.`CBA` = S.`CBA`,
# MAGIC T.`CBR_NO` = S.`CBR_NO`,
# MAGIC T.`CZBA` = S.`CZBA`,
# MAGIC T.`CZBR_NO` = S.`CZBR_NO`,
# MAGIC T.`SBA` = S.`SBA`,
# MAGIC T.`SBR_NO` = S.`SBR_NO`,
# MAGIC T.`IBA` = S.`IBA`,
# MAGIC T.`IBR_NO` = S.`IBR_NO`,
# MAGIC T.`PHBA` = S.`PHBA`,
# MAGIC T.`PHBR_NO` = S.`PHBR_NO`,
# MAGIC T.`MYBA` = S.`MYBA`,
# MAGIC T.`MYBR_NO` = S.`MYBR_NO`,
# MAGIC T.`CUSTOMER_101` = S.`CUSTOMER_101`,
# MAGIC T.`CUSTOMER_104` = S.`CUSTOMER_104`,
# MAGIC T.`CUSTOMER_107` = S.`CUSTOMER_107`,
# MAGIC T.`CUSTOMER_111_AND_102` = S.`CUSTOMER_111_AND_102`,
# MAGIC T.`CUSTOMER_113` = S.`CUSTOMER_113`,
# MAGIC T.`CUSTOMER_133` = S.`CUSTOMER_133`,
# MAGIC T.`CUSTOMER_115` = S.`CUSTOMER_115`,
# MAGIC T.`CUSTOMER_118` = S.`CUSTOMER_118`,
# MAGIC T.`CUSTOMER_123` = S.`CUSTOMER_123`,
# MAGIC T.`CUSTOMER_128` = S.`CUSTOMER_128`,
# MAGIC T.`CUSTOMER_129` = S.`CUSTOMER_129`,
# MAGIC T.`CUSTOMER_131` = S.`CUSTOMER_131`,
# MAGIC T.`CUSTOMER_134` = S.`CUSTOMER_134`,
# MAGIC T.`CUSTOMER_140` = S.`CUSTOMER_140`,
# MAGIC T.`CUSTOMER_141` = S.`CUSTOMER_141`,
# MAGIC T.`CUSTOMER_146` = S.`CUSTOMER_146`,
# MAGIC T.`CUSTOMER_150` = S.`CUSTOMER_150`,
# MAGIC T.`CUSTOMER_154` = S.`CUSTOMER_154`,
# MAGIC T.`CUSTOMER_156` = S.`CUSTOMER_156`,
# MAGIC T.`CUSTOMER_158` = S.`CUSTOMER_158`,
# MAGIC T.`CUSTOMER_164` = S.`CUSTOMER_164`,
# MAGIC T.`CUSTOMER_168` = S.`CUSTOMER_168`,
# MAGIC T.`CUSTOMER_169` = S.`CUSTOMER_169`,
# MAGIC T.`CUSTOMER_170` = S.`CUSTOMER_170`,
# MAGIC T.`CUSTOMER_179` = S.`CUSTOMER_179`,
# MAGIC T.`CUSTOMER_182` = S.`CUSTOMER_182`,
# MAGIC T.`CUSTOMER_183` = S.`CUSTOMER_183`,
# MAGIC T.`CUSTOMER_267` = S.`CUSTOMER_267`,
# MAGIC T.`MARCHI_PN` = S.`MARCHI_PN`,
# MAGIC T.`LEGACY_PN` = S.`LEGACY_PN`,
# MAGIC T.`LEGACY_SYSTEM` = S.`LEGACY_SYSTEM`,
# MAGIC T.`ECCN_NO` = S.`ECCN_NO`,
# MAGIC T.`HTS` = S.`HTS`,
# MAGIC T.`SCHEDULE_B` = S.`SCHEDULE_B`,
# MAGIC T.`RELEASE_TYPE` = S.`RELEASE_TYPE`,
# MAGIC T.`SUB_MPN_BOM` = S.`SUB_MPN_BOM`,
# MAGIC T.`DISP_ONORDER` = S.`DISP_ONORDER`,
# MAGIC T.`DISP_WIP` = S.`DISP_WIP`,
# MAGIC T.`DISP_FG` = S.`DISP_FG`,
# MAGIC T.`DISP_STOCKROOM` = S.`DISP_STOCKROOM`,
# MAGIC T.`DISP_FIELD` = S.`DISP_FIELD`,
# MAGIC T.`CREATE_USER` = S.`CREATE_USER`,
# MAGIC T.`CREATE_DATE` = S.`CREATE_DATE`,
# MAGIC T.`CPDS_FILE_COUNT` = S.`CPDS_FILE_COUNT`,
# MAGIC T.`LandingFileTimeStamp` = S.`LandingFileTimeStamp`,
# MAGIC T.UpdatedOn = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `AGILE_ITEM_ID`,
# MAGIC `PART_NUMBER`,
# MAGIC `PART_CLASS`,
# MAGIC `PART_TYPE`,
# MAGIC `LIFECYCLE_PHASE`,
# MAGIC `DESCRIPTION`,
# MAGIC `DEFAULT_CHANGE`,
# MAGIC `DEFAULT_CHANGE_REL_DATE`,
# MAGIC `LATEST_RELEASED_ECO`,
# MAGIC `LATEST_RELEASED_ECO_REL_DATE`,
# MAGIC `LATEST_RELEASED_ECO_REV_TYPE`,
# MAGIC `COMMODITY_CODE`,
# MAGIC `COMMODITY_CODE_SAP`,
# MAGIC `UOM`,
# MAGIC `CUSTOMER`,
# MAGIC `RESTRICTED_ACCESS_GROUP`,
# MAGIC `REV`,
# MAGIC `DEMAND_FACILITY`,
# MAGIC `COPY_EXACT`,
# MAGIC `CP_CLASSIFICATION`,
# MAGIC `CRITICAL_ASPECT`,
# MAGIC `CRITICAL_PART`,
# MAGIC `SUPPLIER_LOCKED`,
# MAGIC `PRODUCT_CODE`,
# MAGIC `MATERIAL_TYPE`,
# MAGIC `UMC`,
# MAGIC `ROUTING_TEMPLATE`,
# MAGIC `MATERIAL_SUBTYPE`,
# MAGIC `CBA`,
# MAGIC `CBR_NO`,
# MAGIC `CZBA`,
# MAGIC `CZBR_NO`,
# MAGIC `SBA`,
# MAGIC `SBR_NO`,
# MAGIC `IBA`,
# MAGIC `IBR_NO`,
# MAGIC `PHBA`,
# MAGIC `PHBR_NO`,
# MAGIC `MYBA`,
# MAGIC `MYBR_NO`,
# MAGIC `CUSTOMER_101`,
# MAGIC `CUSTOMER_104`,
# MAGIC `CUSTOMER_107`,
# MAGIC `CUSTOMER_111_AND_102`,
# MAGIC `CUSTOMER_113`,
# MAGIC `CUSTOMER_133`,
# MAGIC `CUSTOMER_115`,
# MAGIC `CUSTOMER_118`,
# MAGIC `CUSTOMER_123`,
# MAGIC `CUSTOMER_128`,
# MAGIC `CUSTOMER_129`,
# MAGIC `CUSTOMER_131`,
# MAGIC `CUSTOMER_134`,
# MAGIC `CUSTOMER_140`,
# MAGIC `CUSTOMER_141`,
# MAGIC `CUSTOMER_146`,
# MAGIC `CUSTOMER_150`,
# MAGIC `CUSTOMER_154`,
# MAGIC `CUSTOMER_156`,
# MAGIC `CUSTOMER_158`,
# MAGIC `CUSTOMER_164`,
# MAGIC `CUSTOMER_168`,
# MAGIC `CUSTOMER_169`,
# MAGIC `CUSTOMER_170`,
# MAGIC `CUSTOMER_179`,
# MAGIC `CUSTOMER_182`,
# MAGIC `CUSTOMER_183`,
# MAGIC `CUSTOMER_267`,
# MAGIC `MARCHI_PN`,
# MAGIC `LEGACY_PN`,
# MAGIC `LEGACY_SYSTEM`,
# MAGIC `ECCN_NO`,
# MAGIC `HTS`,
# MAGIC `SCHEDULE_B`,
# MAGIC `RELEASE_TYPE`,
# MAGIC `SUB_MPN_BOM`,
# MAGIC `DISP_ONORDER`,
# MAGIC `DISP_WIP`,
# MAGIC `DISP_FG`,
# MAGIC `DISP_STOCKROOM`,
# MAGIC `DISP_FIELD`,
# MAGIC `CREATE_USER`,
# MAGIC `CREATE_DATE`,
# MAGIC `CPDS_FILE_COUNT`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC ) values
# MAGIC (
# MAGIC S.`AGILE_ITEM_ID`,
# MAGIC S.`PART_NUMBER`,
# MAGIC S.`PART_CLASS`,
# MAGIC S.`PART_TYPE`,
# MAGIC S.`LIFECYCLE_PHASE`,
# MAGIC S.`DESCRIPTION`,
# MAGIC S.`DEFAULT_CHANGE`,
# MAGIC S.`DEFAULT_CHANGE_REL_DATE`,
# MAGIC S.`LATEST_RELEASED_ECO`,
# MAGIC S.`LATEST_RELEASED_ECO_REL_DATE`,
# MAGIC S.`LATEST_RELEASED_ECO_REV_TYPE`,
# MAGIC S.`COMMODITY_CODE`,
# MAGIC S.`COMMODITY_CODE_SAP`,
# MAGIC S.`UOM`,
# MAGIC S.`CUSTOMER`,
# MAGIC S.`RESTRICTED_ACCESS_GROUP`,
# MAGIC S.`REV`,
# MAGIC S.`DEMAND_FACILITY`,
# MAGIC S.`COPY_EXACT`,
# MAGIC S.`CP_CLASSIFICATION`,
# MAGIC S.`CRITICAL_ASPECT`,
# MAGIC S.`CRITICAL_PART`,
# MAGIC S.`SUPPLIER_LOCKED`,
# MAGIC S.`PRODUCT_CODE`,
# MAGIC S.`MATERIAL_TYPE`,
# MAGIC S.`UMC`,
# MAGIC S.`ROUTING_TEMPLATE`,
# MAGIC S.`MATERIAL_SUBTYPE`,
# MAGIC S.`CBA`,
# MAGIC S.`CBR_NO`,
# MAGIC S.`CZBA`,
# MAGIC S.`CZBR_NO`,
# MAGIC S.`SBA`,
# MAGIC S.`SBR_NO`,
# MAGIC S.`IBA`,
# MAGIC S.`IBR_NO`,
# MAGIC S.`PHBA`,
# MAGIC S.`PHBR_NO`,
# MAGIC S.`MYBA`,
# MAGIC S.`MYBR_NO`,
# MAGIC S.`CUSTOMER_101`,
# MAGIC S.`CUSTOMER_104`,
# MAGIC S.`CUSTOMER_107`,
# MAGIC S.`CUSTOMER_111_AND_102`,
# MAGIC S.`CUSTOMER_113`,
# MAGIC S.`CUSTOMER_133`,
# MAGIC S.`CUSTOMER_115`,
# MAGIC S.`CUSTOMER_118`,
# MAGIC S.`CUSTOMER_123`,
# MAGIC S.`CUSTOMER_128`,
# MAGIC S.`CUSTOMER_129`,
# MAGIC S.`CUSTOMER_131`,
# MAGIC S.`CUSTOMER_134`,
# MAGIC S.`CUSTOMER_140`,
# MAGIC S.`CUSTOMER_141`,
# MAGIC S.`CUSTOMER_146`,
# MAGIC S.`CUSTOMER_150`,
# MAGIC S.`CUSTOMER_154`,
# MAGIC S.`CUSTOMER_156`,
# MAGIC S.`CUSTOMER_158`,
# MAGIC S.`CUSTOMER_164`,
# MAGIC S.`CUSTOMER_168`,
# MAGIC S.`CUSTOMER_169`,
# MAGIC S.`CUSTOMER_170`,
# MAGIC S.`CUSTOMER_179`,
# MAGIC S.`CUSTOMER_182`,
# MAGIC S.`CUSTOMER_183`,
# MAGIC S.`CUSTOMER_267`,
# MAGIC S.`MARCHI_PN`,
# MAGIC S.`LEGACY_PN`,
# MAGIC S.`LEGACY_SYSTEM`,
# MAGIC S.`ECCN_NO`,
# MAGIC S.`HTS`,
# MAGIC S.`SCHEDULE_B`,
# MAGIC S.`RELEASE_TYPE`,
# MAGIC S.`SUB_MPN_BOM`,
# MAGIC S.`DISP_ONORDER`,
# MAGIC S.`DISP_WIP`,
# MAGIC S.`DISP_FG`,
# MAGIC S.`DISP_STOCKROOM`,
# MAGIC S.`DISP_FIELD`,
# MAGIC S.`CREATE_USER`,
# MAGIC S.`CREATE_DATE`,
# MAGIC S.`CPDS_FILE_COUNT`,
# MAGIC now(),
# MAGIC 'IT_TEMP',
# MAGIC now()
# MAGIC )
# MAGIC 
# MAGIC  

# COMMAND ----------


