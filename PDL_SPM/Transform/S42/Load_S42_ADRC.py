# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

table_name = 'ADRC'
read_format = 'csv'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/SAP/'+database_name+'/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name


# COMMAND ----------

#df = spark.read.format(read_format) \
#      .option("header", True) \
#      .option("delimiter","^") \
#      .option("inferschema",True) \
#      .load(read_path)

# COMMAND ----------

schema = StructType([ \
                     StructField('CLIENT',StringType(),True),\
StructField('ADDRNUMBER',StringType(),True),\
StructField('DATE_FROM',StringType(),True),\
StructField('NATION',StringType(),True),\
StructField('DATE_TO',StringType(),True),\
StructField('TITLE',StringType(),True),\
StructField('NAME1',StringType(),True),\
StructField('NAME2',StringType(),True),\
StructField('NAME3',StringType(),True),\
StructField('NAME4',StringType(),True),\
StructField('NAME_TEXT',StringType(),True),\
StructField('NAME_CO',StringType(),True),\
StructField('CITY1',StringType(),True),\
StructField('CITY2',StringType(),True),\
StructField('CITY_CODE',StringType(),True),\
StructField('CITYP_CODE',StringType(),True),\
StructField('HOME_CITY',StringType(),True),\
StructField('CITYH_CODE',StringType(),True),\
StructField('CHCKSTATUS',StringType(),True),\
StructField('REGIOGROUP',StringType(),True),\
StructField('POST_CODE1',StringType(),True),\
StructField('POST_CODE2',StringType(),True),\
StructField('POST_CODE3',StringType(),True),\
StructField('PCODE1_EXT',StringType(),True),\
StructField('PCODE2_EXT',StringType(),True),\
StructField('PCODE3_EXT',StringType(),True),\
StructField('PO_BOX',StringType(),True),\
StructField('DONT_USE_P',StringType(),True),\
StructField('PO_BOX_NUM',StringType(),True),\
StructField('PO_BOX_LOC',StringType(),True),\
StructField('CITY_CODE2',StringType(),True),\
StructField('PO_BOX_REG',StringType(),True),\
StructField('PO_BOX_CTY',StringType(),True),\
StructField('POSTALAREA',StringType(),True),\
StructField('TRANSPZONE',StringType(),True),\
StructField('STREET',StringType(),True),\
StructField('DONT_USE_S',StringType(),True),\
StructField('STREETCODE',StringType(),True),\
StructField('STREETABBR',StringType(),True),\
StructField('HOUSE_NUM1',StringType(),True),\
StructField('HOUSE_NUM2',StringType(),True),\
StructField('HOUSE_NUM3',StringType(),True),\
StructField('STR_SUPPL1',StringType(),True),\
StructField('STR_SUPPL2',StringType(),True),\
StructField('STR_SUPPL3',StringType(),True),\
StructField('LOCATION',StringType(),True),\
StructField('BUILDING',StringType(),True),\
StructField('FLOOR',StringType(),True),\
StructField('ROOMNUMBER',StringType(),True),\
StructField('COUNTRY',StringType(),True),\
StructField('LANGU',StringType(),True),\
StructField('REGION',StringType(),True),\
StructField('ADDR_GROUP',StringType(),True),\
StructField('FLAGGROUPS',StringType(),True),\
StructField('PERS_ADDR',StringType(),True),\
StructField('SORT1',StringType(),True),\
StructField('SORT2',StringType(),True),\
StructField('SORT_PHN',StringType(),True),\
StructField('DEFLT_COMM',StringType(),True),\
StructField('TEL_NUMBER',StringType(),True),\
StructField('TEL_EXTENS',StringType(),True),\
StructField('FAX_NUMBER',StringType(),True),\
StructField('FAX_EXTENS',StringType(),True),\
StructField('FLAGCOMM2',StringType(),True),\
StructField('FLAGCOMM3',StringType(),True),\
StructField('FLAGCOMM4',StringType(),True),\
StructField('FLAGCOMM5',StringType(),True),\
StructField('FLAGCOMM6',StringType(),True),\
StructField('FLAGCOMM7',StringType(),True),\
StructField('FLAGCOMM8',StringType(),True),\
StructField('FLAGCOMM9',StringType(),True),\
StructField('FLAGCOMM10',StringType(),True),\
StructField('FLAGCOMM11',StringType(),True),\
StructField('FLAGCOMM12',StringType(),True),\
StructField('FLAGCOMM13',StringType(),True),\
StructField('ADDRORIGIN',StringType(),True),\
StructField('MC_NAME1',StringType(),True),\
StructField('MC_CITY1',StringType(),True),\
StructField('MC_STREET',StringType(),True),\
StructField('EXTENSION1',StringType(),True),\
StructField('EXTENSION2',StringType(),True),\
StructField('TIME_ZONE',StringType(),True),\
StructField('TAXJURCODE',StringType(),True),\
StructField('ADDRESS_ID',StringType(),True),\
StructField('LANGU_CREA',StringType(),True),\
StructField('ADRC_UUID',StringType(),True),\
StructField('UUID_BELATED',StringType(),True),\
StructField('ID_CATEGORY',StringType(),True),\
StructField('ADRC_ERR_STATUS',StringType(),True),\
StructField('PO_BOX_LOBBY',StringType(),True),\
StructField('DELI_SERV_TYPE',StringType(),True),\
StructField('DELI_SERV_NUMBER',StringType(),True),\
StructField('COUNTY_CODE',StringType(),True),\
StructField('COUNTY',StringType(),True),\
StructField('TOWNSHIP_CODE',StringType(),True),\
StructField('TOWNSHIP',StringType(),True),\
StructField('MC_COUNTY',StringType(),True),\
StructField('MC_TOWNSHIP',StringType(),True),\
StructField('XPCPT',StringType(),True),\
StructField('_DATAAGING',StringType(),True),\
StructField('DUNS',StringType(),True),\
StructField('DUNSP4',StringType(),True),\
StructField('ODQ_CHANGEMODE',StringType(),True),\
StructField('ODQ_ENTITYCNTR',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True),\
                    ])

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter","^") \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP')) #updatedOn

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp, '-','')) \
                            .withColumn("DATE_FROM", to_date(regexp_replace(df_add_column.DATE_FROM,'\.','-'))) \
                            .withColumn("DATE_TO", to_date(regexp_replace(df_add_column.DATE_TO,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.ADRC as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY CLIENT,ADDRNUMBER,DATE_FROM,NATION ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_ADRC where CLIENT = 100  )A where A.rn = 1 ) as S 
# MAGIC ON 
# MAGIC T.CLIENT = S.CLIENT and 
# MAGIC T.ADDRNUMBER = S.ADDRNUMBER and
# MAGIC T.DATE_FROM = S.DATE_FROM and
# MAGIC T.NATION = S.NATION 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC  T.`CLIENT` =  S.`CLIENT`,
# MAGIC T.`ADDRNUMBER` =  S.`ADDRNUMBER`,
# MAGIC T.`DATE_FROM` =  S.`DATE_FROM`,
# MAGIC T.`NATION` =  S.`NATION`,
# MAGIC T.`DATE_TO` =  S.`DATE_TO`,
# MAGIC T.`TITLE` =  S.`TITLE`,
# MAGIC T.`NAME1` =  S.`NAME1`,
# MAGIC T.`NAME2` =  S.`NAME2`,
# MAGIC T.`NAME3` =  S.`NAME3`,
# MAGIC T.`NAME4` =  S.`NAME4`,
# MAGIC T.`NAME_TEXT` =  S.`NAME_TEXT`,
# MAGIC T.`NAME_CO` =  S.`NAME_CO`,
# MAGIC T.`CITY1` =  S.`CITY1`,
# MAGIC T.`CITY2` =  S.`CITY2`,
# MAGIC T.`CITY_CODE` =  S.`CITY_CODE`,
# MAGIC T.`CITYP_CODE` =  S.`CITYP_CODE`,
# MAGIC T.`HOME_CITY` =  S.`HOME_CITY`,
# MAGIC T.`CITYH_CODE` =  S.`CITYH_CODE`,
# MAGIC T.`CHCKSTATUS` =  S.`CHCKSTATUS`,
# MAGIC T.`REGIOGROUP` =  S.`REGIOGROUP`,
# MAGIC T.`POST_CODE1` =  S.`POST_CODE1`,
# MAGIC T.`POST_CODE2` =  S.`POST_CODE2`,
# MAGIC T.`POST_CODE3` =  S.`POST_CODE3`,
# MAGIC T.`PCODE1_EXT` =  S.`PCODE1_EXT`,
# MAGIC T.`PCODE2_EXT` =  S.`PCODE2_EXT`,
# MAGIC T.`PCODE3_EXT` =  S.`PCODE3_EXT`,
# MAGIC T.`PO_BOX` =  S.`PO_BOX`,
# MAGIC T.`DONT_USE_P` =  S.`DONT_USE_P`,
# MAGIC T.`PO_BOX_NUM` =  S.`PO_BOX_NUM`,
# MAGIC T.`PO_BOX_LOC` =  S.`PO_BOX_LOC`,
# MAGIC T.`CITY_CODE2` =  S.`CITY_CODE2`,
# MAGIC T.`PO_BOX_REG` =  S.`PO_BOX_REG`,
# MAGIC T.`PO_BOX_CTY` =  S.`PO_BOX_CTY`,
# MAGIC T.`POSTALAREA` =  S.`POSTALAREA`,
# MAGIC T.`TRANSPZONE` =  S.`TRANSPZONE`,
# MAGIC T.`STREET` =  S.`STREET`,
# MAGIC T.`DONT_USE_S` =  S.`DONT_USE_S`,
# MAGIC T.`STREETCODE` =  S.`STREETCODE`,
# MAGIC T.`STREETABBR` =  S.`STREETABBR`,
# MAGIC T.`HOUSE_NUM1` =  S.`HOUSE_NUM1`,
# MAGIC T.`HOUSE_NUM2` =  S.`HOUSE_NUM2`,
# MAGIC T.`HOUSE_NUM3` =  S.`HOUSE_NUM3`,
# MAGIC T.`STR_SUPPL1` =  S.`STR_SUPPL1`,
# MAGIC T.`STR_SUPPL2` =  S.`STR_SUPPL2`,
# MAGIC T.`STR_SUPPL3` =  S.`STR_SUPPL3`,
# MAGIC T.`LOCATION` =  S.`LOCATION`,
# MAGIC T.`BUILDING` =  S.`BUILDING`,
# MAGIC T.`FLOOR` =  S.`FLOOR`,
# MAGIC T.`ROOMNUMBER` =  S.`ROOMNUMBER`,
# MAGIC T.`COUNTRY` =  S.`COUNTRY`,
# MAGIC T.`LANGU` =  S.`LANGU`,
# MAGIC T.`REGION` =  S.`REGION`,
# MAGIC T.`ADDR_GROUP` =  S.`ADDR_GROUP`,
# MAGIC T.`FLAGGROUPS` =  S.`FLAGGROUPS`,
# MAGIC T.`PERS_ADDR` =  S.`PERS_ADDR`,
# MAGIC T.`SORT1` =  S.`SORT1`,
# MAGIC T.`SORT2` =  S.`SORT2`,
# MAGIC T.`SORT_PHN` =  S.`SORT_PHN`,
# MAGIC T.`DEFLT_COMM` =  S.`DEFLT_COMM`,
# MAGIC T.`TEL_NUMBER` =  S.`TEL_NUMBER`,
# MAGIC T.`TEL_EXTENS` =  S.`TEL_EXTENS`,
# MAGIC T.`FAX_NUMBER` =  S.`FAX_NUMBER`,
# MAGIC T.`FAX_EXTENS` =  S.`FAX_EXTENS`,
# MAGIC T.`FLAGCOMM2` =  S.`FLAGCOMM2`,
# MAGIC T.`FLAGCOMM3` =  S.`FLAGCOMM3`,
# MAGIC T.`FLAGCOMM4` =  S.`FLAGCOMM4`,
# MAGIC T.`FLAGCOMM5` =  S.`FLAGCOMM5`,
# MAGIC T.`FLAGCOMM6` =  S.`FLAGCOMM6`,
# MAGIC T.`FLAGCOMM7` =  S.`FLAGCOMM7`,
# MAGIC T.`FLAGCOMM8` =  S.`FLAGCOMM8`,
# MAGIC T.`FLAGCOMM9` =  S.`FLAGCOMM9`,
# MAGIC T.`FLAGCOMM10` =  S.`FLAGCOMM10`,
# MAGIC T.`FLAGCOMM11` =  S.`FLAGCOMM11`,
# MAGIC T.`FLAGCOMM12` =  S.`FLAGCOMM12`,
# MAGIC T.`FLAGCOMM13` =  S.`FLAGCOMM13`,
# MAGIC T.`ADDRORIGIN` =  S.`ADDRORIGIN`,
# MAGIC T.`MC_NAME1` =  S.`MC_NAME1`,
# MAGIC T.`MC_CITY1` =  S.`MC_CITY1`,
# MAGIC T.`MC_STREET` =  S.`MC_STREET`,
# MAGIC T.`EXTENSION1` =  S.`EXTENSION1`,
# MAGIC T.`EXTENSION2` =  S.`EXTENSION2`,
# MAGIC T.`TIME_ZONE` =  S.`TIME_ZONE`,
# MAGIC T.`TAXJURCODE` =  S.`TAXJURCODE`,
# MAGIC T.`ADDRESS_ID` =  S.`ADDRESS_ID`,
# MAGIC T.`LANGU_CREA` =  S.`LANGU_CREA`,
# MAGIC T.`ADRC_UUID` =  S.`ADRC_UUID`,
# MAGIC T.`UUID_BELATED` =  S.`UUID_BELATED`,
# MAGIC T.`ID_CATEGORY` =  S.`ID_CATEGORY`,
# MAGIC T.`ADRC_ERR_STATUS` =  S.`ADRC_ERR_STATUS`,
# MAGIC T.`PO_BOX_LOBBY` =  S.`PO_BOX_LOBBY`,
# MAGIC T.`DELI_SERV_TYPE` =  S.`DELI_SERV_TYPE`,
# MAGIC T.`DELI_SERV_NUMBER` =  S.`DELI_SERV_NUMBER`,
# MAGIC T.`COUNTY_CODE` =  S.`COUNTY_CODE`,
# MAGIC T.`COUNTY` =  S.`COUNTY`,
# MAGIC T.`TOWNSHIP_CODE` =  S.`TOWNSHIP_CODE`,
# MAGIC T.`TOWNSHIP` =  S.`TOWNSHIP`,
# MAGIC T.`MC_COUNTY` =  S.`MC_COUNTY`,
# MAGIC T.`MC_TOWNSHIP` =  S.`MC_TOWNSHIP`,
# MAGIC T.`XPCPT` =  S.`XPCPT`,
# MAGIC T.`_DATAAGING` =  S.`_DATAAGING`,
# MAGIC T.`DUNS` =  S.`DUNS`,
# MAGIC T.`DUNSP4` =  S.`DUNSP4`,
# MAGIC T.`ODQ_CHANGEMODE` =  S.`ODQ_CHANGEMODE`,
# MAGIC T.`ODQ_ENTITYCNTR` =  S.`ODQ_ENTITYCNTR`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `CLIENT`,
# MAGIC `ADDRNUMBER`,
# MAGIC `DATE_FROM`,
# MAGIC `NATION`,
# MAGIC `DATE_TO`,
# MAGIC `TITLE`,
# MAGIC `NAME1`,
# MAGIC `NAME2`,
# MAGIC `NAME3`,
# MAGIC `NAME4`,
# MAGIC `NAME_TEXT`,
# MAGIC `NAME_CO`,
# MAGIC `CITY1`,
# MAGIC `CITY2`,
# MAGIC `CITY_CODE`,
# MAGIC `CITYP_CODE`,
# MAGIC `HOME_CITY`,
# MAGIC `CITYH_CODE`,
# MAGIC `CHCKSTATUS`,
# MAGIC `REGIOGROUP`,
# MAGIC `POST_CODE1`,
# MAGIC `POST_CODE2`,
# MAGIC `POST_CODE3`,
# MAGIC `PCODE1_EXT`,
# MAGIC `PCODE2_EXT`,
# MAGIC `PCODE3_EXT`,
# MAGIC `PO_BOX`,
# MAGIC `DONT_USE_P`,
# MAGIC `PO_BOX_NUM`,
# MAGIC `PO_BOX_LOC`,
# MAGIC `CITY_CODE2`,
# MAGIC `PO_BOX_REG`,
# MAGIC `PO_BOX_CTY`,
# MAGIC `POSTALAREA`,
# MAGIC `TRANSPZONE`,
# MAGIC `STREET`,
# MAGIC `DONT_USE_S`,
# MAGIC `STREETCODE`,
# MAGIC `STREETABBR`,
# MAGIC `HOUSE_NUM1`,
# MAGIC `HOUSE_NUM2`,
# MAGIC `HOUSE_NUM3`,
# MAGIC `STR_SUPPL1`,
# MAGIC `STR_SUPPL2`,
# MAGIC `STR_SUPPL3`,
# MAGIC `LOCATION`,
# MAGIC `BUILDING`,
# MAGIC `FLOOR`,
# MAGIC `ROOMNUMBER`,
# MAGIC `COUNTRY`,
# MAGIC `LANGU`,
# MAGIC `REGION`,
# MAGIC `ADDR_GROUP`,
# MAGIC `FLAGGROUPS`,
# MAGIC `PERS_ADDR`,
# MAGIC `SORT1`,
# MAGIC `SORT2`,
# MAGIC `SORT_PHN`,
# MAGIC `DEFLT_COMM`,
# MAGIC `TEL_NUMBER`,
# MAGIC `TEL_EXTENS`,
# MAGIC `FAX_NUMBER`,
# MAGIC `FAX_EXTENS`,
# MAGIC `FLAGCOMM2`,
# MAGIC `FLAGCOMM3`,
# MAGIC `FLAGCOMM4`,
# MAGIC `FLAGCOMM5`,
# MAGIC `FLAGCOMM6`,
# MAGIC `FLAGCOMM7`,
# MAGIC `FLAGCOMM8`,
# MAGIC `FLAGCOMM9`,
# MAGIC `FLAGCOMM10`,
# MAGIC `FLAGCOMM11`,
# MAGIC `FLAGCOMM12`,
# MAGIC `FLAGCOMM13`,
# MAGIC `ADDRORIGIN`,
# MAGIC `MC_NAME1`,
# MAGIC `MC_CITY1`,
# MAGIC `MC_STREET`,
# MAGIC `EXTENSION1`,
# MAGIC `EXTENSION2`,
# MAGIC `TIME_ZONE`,
# MAGIC `TAXJURCODE`,
# MAGIC `ADDRESS_ID`,
# MAGIC `LANGU_CREA`,
# MAGIC `ADRC_UUID`,
# MAGIC `UUID_BELATED`,
# MAGIC `ID_CATEGORY`,
# MAGIC `ADRC_ERR_STATUS`,
# MAGIC `PO_BOX_LOBBY`,
# MAGIC `DELI_SERV_TYPE`,
# MAGIC `DELI_SERV_NUMBER`,
# MAGIC `COUNTY_CODE`,
# MAGIC `COUNTY`,
# MAGIC `TOWNSHIP_CODE`,
# MAGIC `TOWNSHIP`,
# MAGIC `MC_COUNTY`,
# MAGIC `MC_TOWNSHIP`,
# MAGIC `XPCPT`,
# MAGIC `_DATAAGING`,
# MAGIC `DUNS`,
# MAGIC `DUNSP4`,
# MAGIC `ODQ_CHANGEMODE`,
# MAGIC `ODQ_ENTITYCNTR`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC   VALUES (
# MAGIC   S.`CLIENT`,
# MAGIC S.`ADDRNUMBER`,
# MAGIC S.`DATE_FROM`,
# MAGIC S.`NATION`,
# MAGIC S.`DATE_TO`,
# MAGIC S.`TITLE`,
# MAGIC S.`NAME1`,
# MAGIC S.`NAME2`,
# MAGIC S.`NAME3`,
# MAGIC S.`NAME4`,
# MAGIC S.`NAME_TEXT`,
# MAGIC S.`NAME_CO`,
# MAGIC S.`CITY1`,
# MAGIC S.`CITY2`,
# MAGIC S.`CITY_CODE`,
# MAGIC S.`CITYP_CODE`,
# MAGIC S.`HOME_CITY`,
# MAGIC S.`CITYH_CODE`,
# MAGIC S.`CHCKSTATUS`,
# MAGIC S.`REGIOGROUP`,
# MAGIC S.`POST_CODE1`,
# MAGIC S.`POST_CODE2`,
# MAGIC S.`POST_CODE3`,
# MAGIC S.`PCODE1_EXT`,
# MAGIC S.`PCODE2_EXT`,
# MAGIC S.`PCODE3_EXT`,
# MAGIC S.`PO_BOX`,
# MAGIC S.`DONT_USE_P`,
# MAGIC S.`PO_BOX_NUM`,
# MAGIC S.`PO_BOX_LOC`,
# MAGIC S.`CITY_CODE2`,
# MAGIC S.`PO_BOX_REG`,
# MAGIC S.`PO_BOX_CTY`,
# MAGIC S.`POSTALAREA`,
# MAGIC S.`TRANSPZONE`,
# MAGIC S.`STREET`,
# MAGIC S.`DONT_USE_S`,
# MAGIC S.`STREETCODE`,
# MAGIC S.`STREETABBR`,
# MAGIC S.`HOUSE_NUM1`,
# MAGIC S.`HOUSE_NUM2`,
# MAGIC S.`HOUSE_NUM3`,
# MAGIC S.`STR_SUPPL1`,
# MAGIC S.`STR_SUPPL2`,
# MAGIC S.`STR_SUPPL3`,
# MAGIC S.`LOCATION`,
# MAGIC S.`BUILDING`,
# MAGIC S.`FLOOR`,
# MAGIC S.`ROOMNUMBER`,
# MAGIC S.`COUNTRY`,
# MAGIC S.`LANGU`,
# MAGIC S.`REGION`,
# MAGIC S.`ADDR_GROUP`,
# MAGIC S.`FLAGGROUPS`,
# MAGIC S.`PERS_ADDR`,
# MAGIC S.`SORT1`,
# MAGIC S.`SORT2`,
# MAGIC S.`SORT_PHN`,
# MAGIC S.`DEFLT_COMM`,
# MAGIC S.`TEL_NUMBER`,
# MAGIC S.`TEL_EXTENS`,
# MAGIC S.`FAX_NUMBER`,
# MAGIC S.`FAX_EXTENS`,
# MAGIC S.`FLAGCOMM2`,
# MAGIC S.`FLAGCOMM3`,
# MAGIC S.`FLAGCOMM4`,
# MAGIC S.`FLAGCOMM5`,
# MAGIC S.`FLAGCOMM6`,
# MAGIC S.`FLAGCOMM7`,
# MAGIC S.`FLAGCOMM8`,
# MAGIC S.`FLAGCOMM9`,
# MAGIC S.`FLAGCOMM10`,
# MAGIC S.`FLAGCOMM11`,
# MAGIC S.`FLAGCOMM12`,
# MAGIC S.`FLAGCOMM13`,
# MAGIC S.`ADDRORIGIN`,
# MAGIC S.`MC_NAME1`,
# MAGIC S.`MC_CITY1`,
# MAGIC S.`MC_STREET`,
# MAGIC S.`EXTENSION1`,
# MAGIC S.`EXTENSION2`,
# MAGIC S.`TIME_ZONE`,
# MAGIC S.`TAXJURCODE`,
# MAGIC S.`ADDRESS_ID`,
# MAGIC S.`LANGU_CREA`,
# MAGIC S.`ADRC_UUID`,
# MAGIC S.`UUID_BELATED`,
# MAGIC S.`ID_CATEGORY`,
# MAGIC S.`ADRC_ERR_STATUS`,
# MAGIC S.`PO_BOX_LOBBY`,
# MAGIC S.`DELI_SERV_TYPE`,
# MAGIC S.`DELI_SERV_NUMBER`,
# MAGIC S.`COUNTY_CODE`,
# MAGIC S.`COUNTY`,
# MAGIC S.`TOWNSHIP_CODE`,
# MAGIC S.`TOWNSHIP`,
# MAGIC S.`MC_COUNTY`,
# MAGIC S.`MC_TOWNSHIP`,
# MAGIC S.`XPCPT`,
# MAGIC S.`_DATAAGING`,
# MAGIC S.`DUNS`,
# MAGIC S.`DUNSP4`,
# MAGIC S.`ODQ_CHANGEMODE`,
# MAGIC S.`ODQ_ENTITYCNTR`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


