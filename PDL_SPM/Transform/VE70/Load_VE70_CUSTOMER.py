# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

table_name = 'CUSTOMER'
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
StructField('CONTACT_INITIAL',StringType(),True),\
StructField('CONTACT_POSITION',StringType(),True),\
StructField('CONTACT_HONORIFIC',StringType(),True),\
StructField('CONTACT_SALUTATION',StringType(),True),\
StructField('CONTACT_PHONE',StringType(),True),\
StructField('CONTACT_FAX',StringType(),True),\
StructField('BILL_TO_NAME',StringType(),True),\
StructField('BILL_TO_ADDR_1',StringType(),True),\
StructField('BILL_TO_ADDR_2',StringType(),True),\
StructField('BILL_TO_ADDR_3',StringType(),True),\
StructField('BILL_TO_CITY',StringType(),True),\
StructField('BILL_TO_STATE',StringType(),True),\
StructField('BILL_TO_ZIPCODE',StringType(),True),\
StructField('BILL_TO_COUNTRY',StringType(),True),\
StructField('DISCOUNT_CODE',StringType(),True),\
StructField('FREE_ON_BOARD',StringType(),True),\
StructField('SHIP_VIA',StringType(),True),\
StructField('SALESREP_ID',StringType(),True),\
StructField('TERRITORY',StringType(),True),\
StructField('CURRENCY_ID',StringType(),True),\
StructField('DEF_SLS_TAX_GRP_ID',StringType(),True),\
StructField('ENTITY_ID',StringType(),True),\
StructField('SIC_CODE',StringType(),True),\
StructField('IND_CODE',StringType(),True),\
StructField('CREDIT_STATUS',StringType(),True),\
StructField('CREDIT_LIMIT_CTL',StringType(),True),\
StructField('CREDIT_LIMIT',DoubleType(),True),\
StructField('RECV_AGE_LIMIT',IntegerType(),True),\
StructField('FINANCE_CHARGE',StringType(),True),\
StructField('TAX_EXEMPT',StringType(),True),\
StructField('TAX_ID_NUMBER',StringType(),True),\
StructField('BACKORDER_FLAG',StringType(),True),\
StructField('TERMS_NET_TYPE',StringType(),True),\
StructField('TERMS_NET_DAYS',IntegerType(),True),\
StructField('TERMS_NET_DATE',StringType(),True),\
StructField('TERMS_DISC_TYPE',StringType(),True),\
StructField('TERMS_DISC_DAYS',IntegerType(),True),\
StructField('TERMS_DISC_DATE',StringType(),True),\
StructField('TERMS_DISC_PERCENT',DoubleType(),True),\
StructField('TERMS_DESCRIPTION',StringType(),True),\
StructField('FREIGHT_TERMS',StringType(),True),\
StructField('DUNNING_LETTERS',StringType(),True),\
StructField('LAST_ORDER_DATE',StringType(),True),\
StructField('OPEN_DATE',StringType(),True),\
StructField('MODIFY_DATE',StringType(),True),\
StructField('TOTAL_OPEN_ORDERS',DoubleType(),True),\
StructField('TOTAL_OPEN_RECV',DoubleType(),True),\
StructField('OPEN_ORDER_COUNT',IntegerType(),True),\
StructField('OPEN_RECV_COUNT',IntegerType(),True),\
StructField('USER_1',StringType(),True),\
StructField('USER_2',StringType(),True),\
StructField('USER_3',StringType(),True),\
StructField('USER_4',StringType(),True),\
StructField('USER_5',StringType(),True),\
StructField('USER_6',StringType(),True),\
StructField('USER_7',StringType(),True),\
StructField('USER_8',StringType(),True),\
StructField('USER_9',StringType(),True),\
StructField('USER_10',StringType(),True),\
StructField('SUPPLIER_ID',StringType(),True),\
StructField('CARRIER_ID',StringType(),True),\
StructField('TAX_ON_WHOLESALE',StringType(),True),\
StructField('DEF_RECV_ACCT_ID',StringType(),True),\
StructField('UDF_LAYOUT_ID',StringType(),True),\
StructField('ARRIVAL_CODE',StringType(),True),\
StructField('TRANS_CODE',StringType(),True),\
StructField('COUNTRY_ID',StringType(),True),\
StructField('NATURE_OF_TRANS',StringType(),True),\
StructField('MODE_OF_TRANSPORT',StringType(),True),\
StructField('SIRET_NUMBER',StringType(),True),\
StructField('VAT_REGISTRATION',StringType(),True),\
StructField('VAT_BOOK_CODE_I',StringType(),True),\
StructField('VAT_BOOK_CODE_M',StringType(),True),\
StructField('VAT_EXEMPT',StringType(),True),\
StructField('SHIPTO_ID',IntegerType(),True),\
StructField('RMA_REQUIRED',StringType(),True),\
StructField('PRIORITY_CODE',StringType(),True),\
StructField('FILL_RATE_TYPE',StringType(),True),\
StructField('WAREHOUSE_ID',StringType(),True),\
StructField('COMPLIANCE_LABEL',StringType(),True),\
StructField('CONSOLIDATE_ORDERS',StringType(),True),\
StructField('ORDER_FILL_RATE',DoubleType(),True),\
StructField('CONTACT_MOBILE',StringType(),True),\
StructField('CONTACT_EMAIL',StringType(),True),\
StructField('DEF_TRANS_CURRENCY',StringType(),True),\
StructField('DEF_LBL_FORMAT_ID',StringType(),True),\
StructField('CUSTOMER_TYPE',StringType(),True),\
StructField('ACCEPT_EARLY',StringType(),True),\
StructField('DAYS_EARLY',IntegerType(),True),\
StructField('VAT_DISCOUNTED',StringType(),True),\
StructField('WEB_USER_ID',StringType(),True),\
StructField('WEB_PASSWORD',StringType(),True),\
StructField('REALLOCATE',StringType(),True),\
StructField('ALLOCATION_FENCE',StringType(),True),\
StructField('PRIORITY',StringType(),True),\
StructField('LANGUAGE_ID',StringType(),True),\
StructField('BILL_LANGUAGE_ID',StringType(),True),\
StructField('DEF_ACK_ID',StringType(),True),\
StructField('PRIMARILY_EDI',StringType(),True),\
StructField('VAT_ALWAYS_DISC',StringType(),True),\
StructField('VAT_CODE',StringType(),True),\
StructField('VAT_OVERRIDE_SEQ',StringType(),True),\
StructField('RETURN_TRANS',StringType(),True),\
StructField('AUTO_ALLOCATE',StringType(),True),\
StructField('CO_ALLOC_LEVEL',StringType(),True),\
StructField('CASH_PERCENT_VAR',StringType(),True),\
StructField('CASH_MIN_VAR',StringType(),True),\
StructField('CASH_MAX_VAR',StringType(),True),\
StructField('TAKE_DISC_DAYS',DoubleType(),True),\
StructField('MARKET_ID',StringType(),True),\
StructField('CREDIT_CARD_ID',StringType(),True),\
StructField('WEB_URL',StringType(),True),\
StructField('UPS_ACCOUNT_ID',StringType(),True),\
StructField('GENERATE_ASN',StringType(),True),\
StructField('HOLD_TRANSFER_ASN',StringType(),True),\
StructField('CUSTOMS_DOC_PRINT',StringType(),True),\
StructField('SUPPRESS_AR_PRINT',StringType(),True),\
StructField('ACCEPT_830',StringType(),True),\
StructField('ACCEPT_862',StringType(),True),\
StructField('POOL_CODE',StringType(),True),\
StructField('INTER_CONSIGNEE',StringType(),True),\
StructField('CONSOL_SHIP_LINE',StringType(),True),\
StructField('PALLET_DETAILS_REQ',StringType(),True),\
StructField('MATCH_INV_TO_PACK',StringType(),True),\
StructField('ALLOW_OVER_PAYMNT',StringType(),True),\
StructField('ALLOW_ADJ_INVOICE',StringType(),True),\
StructField('TOTAL_OPEN_SHIPPED',DoubleType(),True),\
StructField('DEF_GL_ACCT_ID',StringType(),True),\
StructField('LIQ_GL_ACCT_ID',StringType(),True),\
StructField('PBC_GL_ACCT_ID',StringType(),True),\
StructField('GENERATE_WSA',StringType(),True),\
StructField('HOLD_TRANSFER_WSA',StringType(),True),\
StructField('ACTIVE_FLAG',StringType(),True),\
StructField('UCT_WELDMENT_LABEL_FORMAT_ID',StringType(),True),\
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
                                       .withColumn("OPEN_DATE", to_timestamp(regexp_replace(df_add_column.OPEN_DATE,'\.','-'))) \
                                        .withColumn("MODIFY_DATE", to_timestamp(regexp_replace(df_add_column.MODIFY_DATE,'\.','-'))) 

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_transform.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")
        spark.sql("truncate table "+database_name+"."+ table_name + "")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO VE70.CUSTOMER as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY ID ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_CUSTOMER)A where A.rn = 1) as S 
# MAGIC ON T.ID = S.ID 
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET
# MAGIC T.`ROWID`	 = S.`ROWID`,
# MAGIC T.`ID`	 = S.`ID`,
# MAGIC T.`NAME`	 = S.`NAME`,
# MAGIC T.`ADDR_1`	 = S.`ADDR_1`,
# MAGIC T.`ADDR_2`	 = S.`ADDR_2`,
# MAGIC T.`ADDR_3`	 = S.`ADDR_3`,
# MAGIC T.`CITY`	 = S.`CITY`,
# MAGIC T.`STATE`	 = S.`STATE`,
# MAGIC T.`ZIPCODE`	 = S.`ZIPCODE`,
# MAGIC T.`COUNTRY`	 = S.`COUNTRY`,
# MAGIC T.`CONTACT_FIRST_NAME`	 = S.`CONTACT_FIRST_NAME`,
# MAGIC T.`CONTACT_LAST_NAME`	 = S.`CONTACT_LAST_NAME`,
# MAGIC T.`CONTACT_INITIAL`	 = S.`CONTACT_INITIAL`,
# MAGIC T.`CONTACT_POSITION`	 = S.`CONTACT_POSITION`,
# MAGIC T.`CONTACT_HONORIFIC`	 = S.`CONTACT_HONORIFIC`,
# MAGIC T.`CONTACT_SALUTATION`	 = S.`CONTACT_SALUTATION`,
# MAGIC T.`CONTACT_PHONE`	 = S.`CONTACT_PHONE`,
# MAGIC T.`CONTACT_FAX`	 = S.`CONTACT_FAX`,
# MAGIC T.`BILL_TO_NAME`	 = S.`BILL_TO_NAME`,
# MAGIC T.`BILL_TO_ADDR_1`	 = S.`BILL_TO_ADDR_1`,
# MAGIC T.`BILL_TO_ADDR_2`	 = S.`BILL_TO_ADDR_2`,
# MAGIC T.`BILL_TO_ADDR_3`	 = S.`BILL_TO_ADDR_3`,
# MAGIC T.`BILL_TO_CITY`	 = S.`BILL_TO_CITY`,
# MAGIC T.`BILL_TO_STATE`	 = S.`BILL_TO_STATE`,
# MAGIC T.`BILL_TO_ZIPCODE`	 = S.`BILL_TO_ZIPCODE`,
# MAGIC T.`BILL_TO_COUNTRY`	 = S.`BILL_TO_COUNTRY`,
# MAGIC T.`DISCOUNT_CODE`	 = S.`DISCOUNT_CODE`,
# MAGIC T.`FREE_ON_BOARD`	 = S.`FREE_ON_BOARD`,
# MAGIC T.`SHIP_VIA`	 = S.`SHIP_VIA`,
# MAGIC T.`SALESREP_ID`	 = S.`SALESREP_ID`,
# MAGIC T.`TERRITORY`	 = S.`TERRITORY`,
# MAGIC T.`CURRENCY_ID`	 = S.`CURRENCY_ID`,
# MAGIC T.`DEF_SLS_TAX_GRP_ID`	 = S.`DEF_SLS_TAX_GRP_ID`,
# MAGIC T.`ENTITY_ID`	 = S.`ENTITY_ID`,
# MAGIC T.`SIC_CODE`	 = S.`SIC_CODE`,
# MAGIC T.`IND_CODE`	 = S.`IND_CODE`,
# MAGIC T.`CREDIT_STATUS`	 = S.`CREDIT_STATUS`,
# MAGIC T.`CREDIT_LIMIT_CTL`	 = S.`CREDIT_LIMIT_CTL`,
# MAGIC T.`CREDIT_LIMIT`	 = S.`CREDIT_LIMIT`,
# MAGIC T.`RECV_AGE_LIMIT`	 = S.`RECV_AGE_LIMIT`,
# MAGIC T.`FINANCE_CHARGE`	 = S.`FINANCE_CHARGE`,
# MAGIC T.`TAX_EXEMPT`	 = S.`TAX_EXEMPT`,
# MAGIC T.`TAX_ID_NUMBER`	 = S.`TAX_ID_NUMBER`,
# MAGIC T.`BACKORDER_FLAG`	 = S.`BACKORDER_FLAG`,
# MAGIC T.`TERMS_NET_TYPE`	 = S.`TERMS_NET_TYPE`,
# MAGIC T.`TERMS_NET_DAYS`	 = S.`TERMS_NET_DAYS`,
# MAGIC T.`TERMS_NET_DATE`	 = S.`TERMS_NET_DATE`,
# MAGIC T.`TERMS_DISC_TYPE`	 = S.`TERMS_DISC_TYPE`,
# MAGIC T.`TERMS_DISC_DAYS`	 = S.`TERMS_DISC_DAYS`,
# MAGIC T.`TERMS_DISC_DATE`	 = S.`TERMS_DISC_DATE`,
# MAGIC T.`TERMS_DISC_PERCENT`	 = S.`TERMS_DISC_PERCENT`,
# MAGIC T.`TERMS_DESCRIPTION`	 = S.`TERMS_DESCRIPTION`,
# MAGIC T.`FREIGHT_TERMS`	 = S.`FREIGHT_TERMS`,
# MAGIC T.`DUNNING_LETTERS`	 = S.`DUNNING_LETTERS`,
# MAGIC T.`LAST_ORDER_DATE`	 = S.`LAST_ORDER_DATE`,
# MAGIC T.`OPEN_DATE`	 = S.`OPEN_DATE`,
# MAGIC T.`MODIFY_DATE`	 = S.`MODIFY_DATE`,
# MAGIC T.`TOTAL_OPEN_ORDERS`	 = S.`TOTAL_OPEN_ORDERS`,
# MAGIC T.`TOTAL_OPEN_RECV`	 = S.`TOTAL_OPEN_RECV`,
# MAGIC T.`OPEN_ORDER_COUNT`	 = S.`OPEN_ORDER_COUNT`,
# MAGIC T.`OPEN_RECV_COUNT`	 = S.`OPEN_RECV_COUNT`,
# MAGIC T.`USER_1`	 = S.`USER_1`,
# MAGIC T.`USER_2`	 = S.`USER_2`,
# MAGIC T.`USER_3`	 = S.`USER_3`,
# MAGIC T.`USER_4`	 = S.`USER_4`,
# MAGIC T.`USER_5`	 = S.`USER_5`,
# MAGIC T.`USER_6`	 = S.`USER_6`,
# MAGIC T.`USER_7`	 = S.`USER_7`,
# MAGIC T.`USER_8`	 = S.`USER_8`,
# MAGIC T.`USER_9`	 = S.`USER_9`,
# MAGIC T.`USER_10`	 = S.`USER_10`,
# MAGIC T.`SUPPLIER_ID`	 = S.`SUPPLIER_ID`,
# MAGIC T.`CARRIER_ID`	 = S.`CARRIER_ID`,
# MAGIC T.`TAX_ON_WHOLESALE`	 = S.`TAX_ON_WHOLESALE`,
# MAGIC T.`DEF_RECV_ACCT_ID`	 = S.`DEF_RECV_ACCT_ID`,
# MAGIC T.`UDF_LAYOUT_ID`	 = S.`UDF_LAYOUT_ID`,
# MAGIC T.`ARRIVAL_CODE`	 = S.`ARRIVAL_CODE`,
# MAGIC T.`TRANS_CODE`	 = S.`TRANS_CODE`,
# MAGIC T.`COUNTRY_ID`	 = S.`COUNTRY_ID`,
# MAGIC T.`NATURE_OF_TRANS`	 = S.`NATURE_OF_TRANS`,
# MAGIC T.`MODE_OF_TRANSPORT`	 = S.`MODE_OF_TRANSPORT`,
# MAGIC T.`SIRET_NUMBER`	 = S.`SIRET_NUMBER`,
# MAGIC T.`VAT_REGISTRATION`	 = S.`VAT_REGISTRATION`,
# MAGIC T.`VAT_BOOK_CODE_I`	 = S.`VAT_BOOK_CODE_I`,
# MAGIC T.`VAT_BOOK_CODE_M`	 = S.`VAT_BOOK_CODE_M`,
# MAGIC T.`VAT_EXEMPT`	 = S.`VAT_EXEMPT`,
# MAGIC T.`SHIPTO_ID`	 = S.`SHIPTO_ID`,
# MAGIC T.`RMA_REQUIRED`	 = S.`RMA_REQUIRED`,
# MAGIC T.`PRIORITY_CODE`	 = S.`PRIORITY_CODE`,
# MAGIC T.`FILL_RATE_TYPE`	 = S.`FILL_RATE_TYPE`,
# MAGIC T.`WAREHOUSE_ID`	 = S.`WAREHOUSE_ID`,
# MAGIC T.`COMPLIANCE_LABEL`	 = S.`COMPLIANCE_LABEL`,
# MAGIC T.`CONSOLIDATE_ORDERS`	 = S.`CONSOLIDATE_ORDERS`,
# MAGIC T.`ORDER_FILL_RATE`	 = S.`ORDER_FILL_RATE`,
# MAGIC T.`CONTACT_MOBILE`	 = S.`CONTACT_MOBILE`,
# MAGIC T.`CONTACT_EMAIL`	 = S.`CONTACT_EMAIL`,
# MAGIC T.`DEF_TRANS_CURRENCY`	 = S.`DEF_TRANS_CURRENCY`,
# MAGIC T.`DEF_LBL_FORMAT_ID`	 = S.`DEF_LBL_FORMAT_ID`,
# MAGIC T.`CUSTOMER_TYPE`	 = S.`CUSTOMER_TYPE`,
# MAGIC T.`ACCEPT_EARLY`	 = S.`ACCEPT_EARLY`,
# MAGIC T.`DAYS_EARLY`	 = S.`DAYS_EARLY`,
# MAGIC T.`VAT_DISCOUNTED`	 = S.`VAT_DISCOUNTED`,
# MAGIC T.`WEB_USER_ID`	 = S.`WEB_USER_ID`,
# MAGIC T.`WEB_PASSWORD`	 = S.`WEB_PASSWORD`,
# MAGIC T.`REALLOCATE`	 = S.`REALLOCATE`,
# MAGIC T.`ALLOCATION_FENCE`	 = S.`ALLOCATION_FENCE`,
# MAGIC T.`PRIORITY`	 = S.`PRIORITY`,
# MAGIC T.`LANGUAGE_ID`	 = S.`LANGUAGE_ID`,
# MAGIC T.`BILL_LANGUAGE_ID`	 = S.`BILL_LANGUAGE_ID`,
# MAGIC T.`DEF_ACK_ID`	 = S.`DEF_ACK_ID`,
# MAGIC T.`PRIMARILY_EDI`	 = S.`PRIMARILY_EDI`,
# MAGIC T.`VAT_ALWAYS_DISC`	 = S.`VAT_ALWAYS_DISC`,
# MAGIC T.`VAT_CODE`	 = S.`VAT_CODE`,
# MAGIC T.`VAT_OVERRIDE_SEQ`	 = S.`VAT_OVERRIDE_SEQ`,
# MAGIC T.`RETURN_TRANS`	 = S.`RETURN_TRANS`,
# MAGIC T.`AUTO_ALLOCATE`	 = S.`AUTO_ALLOCATE`,
# MAGIC T.`CO_ALLOC_LEVEL`	 = S.`CO_ALLOC_LEVEL`,
# MAGIC T.`CASH_PERCENT_VAR`	 = S.`CASH_PERCENT_VAR`,
# MAGIC T.`CASH_MIN_VAR`	 = S.`CASH_MIN_VAR`,
# MAGIC T.`CASH_MAX_VAR`	 = S.`CASH_MAX_VAR`,
# MAGIC T.`TAKE_DISC_DAYS`	 = S.`TAKE_DISC_DAYS`,
# MAGIC T.`MARKET_ID`	 = S.`MARKET_ID`,
# MAGIC T.`CREDIT_CARD_ID`	 = S.`CREDIT_CARD_ID`,
# MAGIC T.`WEB_URL`	 = S.`WEB_URL`,
# MAGIC T.`UPS_ACCOUNT_ID`	 = S.`UPS_ACCOUNT_ID`,
# MAGIC T.`GENERATE_ASN`	 = S.`GENERATE_ASN`,
# MAGIC T.`HOLD_TRANSFER_ASN`	 = S.`HOLD_TRANSFER_ASN`,
# MAGIC T.`CUSTOMS_DOC_PRINT`	 = S.`CUSTOMS_DOC_PRINT`,
# MAGIC T.`SUPPRESS_AR_PRINT`	 = S.`SUPPRESS_AR_PRINT`,
# MAGIC T.`ACCEPT_830`	 = S.`ACCEPT_830`,
# MAGIC T.`ACCEPT_862`	 = S.`ACCEPT_862`,
# MAGIC T.`POOL_CODE`	 = S.`POOL_CODE`,
# MAGIC T.`INTER_CONSIGNEE`	 = S.`INTER_CONSIGNEE`,
# MAGIC T.`CONSOL_SHIP_LINE`	 = S.`CONSOL_SHIP_LINE`,
# MAGIC T.`PALLET_DETAILS_REQ`	 = S.`PALLET_DETAILS_REQ`,
# MAGIC T.`MATCH_INV_TO_PACK`	 = S.`MATCH_INV_TO_PACK`,
# MAGIC T.`ALLOW_OVER_PAYMNT`	 = S.`ALLOW_OVER_PAYMNT`,
# MAGIC T.`ALLOW_ADJ_INVOICE`	 = S.`ALLOW_ADJ_INVOICE`,
# MAGIC T.`TOTAL_OPEN_SHIPPED`	 = S.`TOTAL_OPEN_SHIPPED`,
# MAGIC T.`DEF_GL_ACCT_ID`	 = S.`DEF_GL_ACCT_ID`,
# MAGIC T.`LIQ_GL_ACCT_ID`	 = S.`LIQ_GL_ACCT_ID`,
# MAGIC T.`PBC_GL_ACCT_ID`	 = S.`PBC_GL_ACCT_ID`,
# MAGIC T.`GENERATE_WSA`	 = S.`GENERATE_WSA`,
# MAGIC T.`HOLD_TRANSFER_WSA`	 = S.`HOLD_TRANSFER_WSA`,
# MAGIC T.`ACTIVE_FLAG`	 = S.`ACTIVE_FLAG`,
# MAGIC T.`UCT_WELDMENT_LABEL_FORMAT_ID`	 = S.`UCT_WELDMENT_LABEL_FORMAT_ID`,
# MAGIC T.`LandingFileTimeStamp`	 = S.`LandingFileTimeStamp`,
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
# MAGIC `CONTACT_INITIAL`,
# MAGIC `CONTACT_POSITION`,
# MAGIC `CONTACT_HONORIFIC`,
# MAGIC `CONTACT_SALUTATION`,
# MAGIC `CONTACT_PHONE`,
# MAGIC `CONTACT_FAX`,
# MAGIC `BILL_TO_NAME`,
# MAGIC `BILL_TO_ADDR_1`,
# MAGIC `BILL_TO_ADDR_2`,
# MAGIC `BILL_TO_ADDR_3`,
# MAGIC `BILL_TO_CITY`,
# MAGIC `BILL_TO_STATE`,
# MAGIC `BILL_TO_ZIPCODE`,
# MAGIC `BILL_TO_COUNTRY`,
# MAGIC `DISCOUNT_CODE`,
# MAGIC `FREE_ON_BOARD`,
# MAGIC `SHIP_VIA`,
# MAGIC `SALESREP_ID`,
# MAGIC `TERRITORY`,
# MAGIC `CURRENCY_ID`,
# MAGIC `DEF_SLS_TAX_GRP_ID`,
# MAGIC `ENTITY_ID`,
# MAGIC `SIC_CODE`,
# MAGIC `IND_CODE`,
# MAGIC `CREDIT_STATUS`,
# MAGIC `CREDIT_LIMIT_CTL`,
# MAGIC `CREDIT_LIMIT`,
# MAGIC `RECV_AGE_LIMIT`,
# MAGIC `FINANCE_CHARGE`,
# MAGIC `TAX_EXEMPT`,
# MAGIC `TAX_ID_NUMBER`,
# MAGIC `BACKORDER_FLAG`,
# MAGIC `TERMS_NET_TYPE`,
# MAGIC `TERMS_NET_DAYS`,
# MAGIC `TERMS_NET_DATE`,
# MAGIC `TERMS_DISC_TYPE`,
# MAGIC `TERMS_DISC_DAYS`,
# MAGIC `TERMS_DISC_DATE`,
# MAGIC `TERMS_DISC_PERCENT`,
# MAGIC `TERMS_DESCRIPTION`,
# MAGIC `FREIGHT_TERMS`,
# MAGIC `DUNNING_LETTERS`,
# MAGIC `LAST_ORDER_DATE`,
# MAGIC `OPEN_DATE`,
# MAGIC `MODIFY_DATE`,
# MAGIC `TOTAL_OPEN_ORDERS`,
# MAGIC `TOTAL_OPEN_RECV`,
# MAGIC `OPEN_ORDER_COUNT`,
# MAGIC `OPEN_RECV_COUNT`,
# MAGIC `USER_1`,
# MAGIC `USER_2`,
# MAGIC `USER_3`,
# MAGIC `USER_4`,
# MAGIC `USER_5`,
# MAGIC `USER_6`,
# MAGIC `USER_7`,
# MAGIC `USER_8`,
# MAGIC `USER_9`,
# MAGIC `USER_10`,
# MAGIC `SUPPLIER_ID`,
# MAGIC `CARRIER_ID`,
# MAGIC `TAX_ON_WHOLESALE`,
# MAGIC `DEF_RECV_ACCT_ID`,
# MAGIC `UDF_LAYOUT_ID`,
# MAGIC `ARRIVAL_CODE`,
# MAGIC `TRANS_CODE`,
# MAGIC `COUNTRY_ID`,
# MAGIC `NATURE_OF_TRANS`,
# MAGIC `MODE_OF_TRANSPORT`,
# MAGIC `SIRET_NUMBER`,
# MAGIC `VAT_REGISTRATION`,
# MAGIC `VAT_BOOK_CODE_I`,
# MAGIC `VAT_BOOK_CODE_M`,
# MAGIC `VAT_EXEMPT`,
# MAGIC `SHIPTO_ID`,
# MAGIC `RMA_REQUIRED`,
# MAGIC `PRIORITY_CODE`,
# MAGIC `FILL_RATE_TYPE`,
# MAGIC `WAREHOUSE_ID`,
# MAGIC `COMPLIANCE_LABEL`,
# MAGIC `CONSOLIDATE_ORDERS`,
# MAGIC `ORDER_FILL_RATE`,
# MAGIC `CONTACT_MOBILE`,
# MAGIC `CONTACT_EMAIL`,
# MAGIC `DEF_TRANS_CURRENCY`,
# MAGIC `DEF_LBL_FORMAT_ID`,
# MAGIC `CUSTOMER_TYPE`,
# MAGIC `ACCEPT_EARLY`,
# MAGIC `DAYS_EARLY`,
# MAGIC `VAT_DISCOUNTED`,
# MAGIC `WEB_USER_ID`,
# MAGIC `WEB_PASSWORD`,
# MAGIC `REALLOCATE`,
# MAGIC `ALLOCATION_FENCE`,
# MAGIC `PRIORITY`,
# MAGIC `LANGUAGE_ID`,
# MAGIC `BILL_LANGUAGE_ID`,
# MAGIC `DEF_ACK_ID`,
# MAGIC `PRIMARILY_EDI`,
# MAGIC `VAT_ALWAYS_DISC`,
# MAGIC `VAT_CODE`,
# MAGIC `VAT_OVERRIDE_SEQ`,
# MAGIC `RETURN_TRANS`,
# MAGIC `AUTO_ALLOCATE`,
# MAGIC `CO_ALLOC_LEVEL`,
# MAGIC `CASH_PERCENT_VAR`,
# MAGIC `CASH_MIN_VAR`,
# MAGIC `CASH_MAX_VAR`,
# MAGIC `TAKE_DISC_DAYS`,
# MAGIC `MARKET_ID`,
# MAGIC `CREDIT_CARD_ID`,
# MAGIC `WEB_URL`,
# MAGIC `UPS_ACCOUNT_ID`,
# MAGIC `GENERATE_ASN`,
# MAGIC `HOLD_TRANSFER_ASN`,
# MAGIC `CUSTOMS_DOC_PRINT`,
# MAGIC `SUPPRESS_AR_PRINT`,
# MAGIC `ACCEPT_830`,
# MAGIC `ACCEPT_862`,
# MAGIC `POOL_CODE`,
# MAGIC `INTER_CONSIGNEE`,
# MAGIC `CONSOL_SHIP_LINE`,
# MAGIC `PALLET_DETAILS_REQ`,
# MAGIC `MATCH_INV_TO_PACK`,
# MAGIC `ALLOW_OVER_PAYMNT`,
# MAGIC `ALLOW_ADJ_INVOICE`,
# MAGIC `TOTAL_OPEN_SHIPPED`,
# MAGIC `DEF_GL_ACCT_ID`,
# MAGIC `LIQ_GL_ACCT_ID`,
# MAGIC `PBC_GL_ACCT_ID`,
# MAGIC `GENERATE_WSA`,
# MAGIC `HOLD_TRANSFER_WSA`,
# MAGIC `ACTIVE_FLAG`,
# MAGIC `UCT_WELDMENT_LABEL_FORMAT_ID`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC   DataSource,
# MAGIC   UpdatedOn
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`ROWID`,
# MAGIC S.`ID`,
# MAGIC S.`NAME`,
# MAGIC S.`ADDR_1`,
# MAGIC S.`ADDR_2`,
# MAGIC S.`ADDR_3`,
# MAGIC S.`CITY`,
# MAGIC S.`STATE`,
# MAGIC S.`ZIPCODE`,
# MAGIC S.`COUNTRY`,
# MAGIC S.`CONTACT_FIRST_NAME`,
# MAGIC S.`CONTACT_LAST_NAME`,
# MAGIC S.`CONTACT_INITIAL`,
# MAGIC S.`CONTACT_POSITION`,
# MAGIC S.`CONTACT_HONORIFIC`,
# MAGIC S.`CONTACT_SALUTATION`,
# MAGIC S.`CONTACT_PHONE`,
# MAGIC S.`CONTACT_FAX`,
# MAGIC S.`BILL_TO_NAME`,
# MAGIC S.`BILL_TO_ADDR_1`,
# MAGIC S.`BILL_TO_ADDR_2`,
# MAGIC S.`BILL_TO_ADDR_3`,
# MAGIC S.`BILL_TO_CITY`,
# MAGIC S.`BILL_TO_STATE`,
# MAGIC S.`BILL_TO_ZIPCODE`,
# MAGIC S.`BILL_TO_COUNTRY`,
# MAGIC S.`DISCOUNT_CODE`,
# MAGIC S.`FREE_ON_BOARD`,
# MAGIC S.`SHIP_VIA`,
# MAGIC S.`SALESREP_ID`,
# MAGIC S.`TERRITORY`,
# MAGIC S.`CURRENCY_ID`,
# MAGIC S.`DEF_SLS_TAX_GRP_ID`,
# MAGIC S.`ENTITY_ID`,
# MAGIC S.`SIC_CODE`,
# MAGIC S.`IND_CODE`,
# MAGIC S.`CREDIT_STATUS`,
# MAGIC S.`CREDIT_LIMIT_CTL`,
# MAGIC S.`CREDIT_LIMIT`,
# MAGIC S.`RECV_AGE_LIMIT`,
# MAGIC S.`FINANCE_CHARGE`,
# MAGIC S.`TAX_EXEMPT`,
# MAGIC S.`TAX_ID_NUMBER`,
# MAGIC S.`BACKORDER_FLAG`,
# MAGIC S.`TERMS_NET_TYPE`,
# MAGIC S.`TERMS_NET_DAYS`,
# MAGIC S.`TERMS_NET_DATE`,
# MAGIC S.`TERMS_DISC_TYPE`,
# MAGIC S.`TERMS_DISC_DAYS`,
# MAGIC S.`TERMS_DISC_DATE`,
# MAGIC S.`TERMS_DISC_PERCENT`,
# MAGIC S.`TERMS_DESCRIPTION`,
# MAGIC S.`FREIGHT_TERMS`,
# MAGIC S.`DUNNING_LETTERS`,
# MAGIC S.`LAST_ORDER_DATE`,
# MAGIC S.`OPEN_DATE`,
# MAGIC S.`MODIFY_DATE`,
# MAGIC S.`TOTAL_OPEN_ORDERS`,
# MAGIC S.`TOTAL_OPEN_RECV`,
# MAGIC S.`OPEN_ORDER_COUNT`,
# MAGIC S.`OPEN_RECV_COUNT`,
# MAGIC S.`USER_1`,
# MAGIC S.`USER_2`,
# MAGIC S.`USER_3`,
# MAGIC S.`USER_4`,
# MAGIC S.`USER_5`,
# MAGIC S.`USER_6`,
# MAGIC S.`USER_7`,
# MAGIC S.`USER_8`,
# MAGIC S.`USER_9`,
# MAGIC S.`USER_10`,
# MAGIC S.`SUPPLIER_ID`,
# MAGIC S.`CARRIER_ID`,
# MAGIC S.`TAX_ON_WHOLESALE`,
# MAGIC S.`DEF_RECV_ACCT_ID`,
# MAGIC S.`UDF_LAYOUT_ID`,
# MAGIC S.`ARRIVAL_CODE`,
# MAGIC S.`TRANS_CODE`,
# MAGIC S.`COUNTRY_ID`,
# MAGIC S.`NATURE_OF_TRANS`,
# MAGIC S.`MODE_OF_TRANSPORT`,
# MAGIC S.`SIRET_NUMBER`,
# MAGIC S.`VAT_REGISTRATION`,
# MAGIC S.`VAT_BOOK_CODE_I`,
# MAGIC S.`VAT_BOOK_CODE_M`,
# MAGIC S.`VAT_EXEMPT`,
# MAGIC S.`SHIPTO_ID`,
# MAGIC S.`RMA_REQUIRED`,
# MAGIC S.`PRIORITY_CODE`,
# MAGIC S.`FILL_RATE_TYPE`,
# MAGIC S.`WAREHOUSE_ID`,
# MAGIC S.`COMPLIANCE_LABEL`,
# MAGIC S.`CONSOLIDATE_ORDERS`,
# MAGIC S.`ORDER_FILL_RATE`,
# MAGIC S.`CONTACT_MOBILE`,
# MAGIC S.`CONTACT_EMAIL`,
# MAGIC S.`DEF_TRANS_CURRENCY`,
# MAGIC S.`DEF_LBL_FORMAT_ID`,
# MAGIC S.`CUSTOMER_TYPE`,
# MAGIC S.`ACCEPT_EARLY`,
# MAGIC S.`DAYS_EARLY`,
# MAGIC S.`VAT_DISCOUNTED`,
# MAGIC S.`WEB_USER_ID`,
# MAGIC S.`WEB_PASSWORD`,
# MAGIC S.`REALLOCATE`,
# MAGIC S.`ALLOCATION_FENCE`,
# MAGIC S.`PRIORITY`,
# MAGIC S.`LANGUAGE_ID`,
# MAGIC S.`BILL_LANGUAGE_ID`,
# MAGIC S.`DEF_ACK_ID`,
# MAGIC S.`PRIMARILY_EDI`,
# MAGIC S.`VAT_ALWAYS_DISC`,
# MAGIC S.`VAT_CODE`,
# MAGIC S.`VAT_OVERRIDE_SEQ`,
# MAGIC S.`RETURN_TRANS`,
# MAGIC S.`AUTO_ALLOCATE`,
# MAGIC S.`CO_ALLOC_LEVEL`,
# MAGIC S.`CASH_PERCENT_VAR`,
# MAGIC S.`CASH_MIN_VAR`,
# MAGIC S.`CASH_MAX_VAR`,
# MAGIC S.`TAKE_DISC_DAYS`,
# MAGIC S.`MARKET_ID`,
# MAGIC S.`CREDIT_CARD_ID`,
# MAGIC S.`WEB_URL`,
# MAGIC S.`UPS_ACCOUNT_ID`,
# MAGIC S.`GENERATE_ASN`,
# MAGIC S.`HOLD_TRANSFER_ASN`,
# MAGIC S.`CUSTOMS_DOC_PRINT`,
# MAGIC S.`SUPPRESS_AR_PRINT`,
# MAGIC S.`ACCEPT_830`,
# MAGIC S.`ACCEPT_862`,
# MAGIC S.`POOL_CODE`,
# MAGIC S.`INTER_CONSIGNEE`,
# MAGIC S.`CONSOL_SHIP_LINE`,
# MAGIC S.`PALLET_DETAILS_REQ`,
# MAGIC S.`MATCH_INV_TO_PACK`,
# MAGIC S.`ALLOW_OVER_PAYMNT`,
# MAGIC S.`ALLOW_ADJ_INVOICE`,
# MAGIC S.`TOTAL_OPEN_SHIPPED`,
# MAGIC S.`DEF_GL_ACCT_ID`,
# MAGIC S.`LIQ_GL_ACCT_ID`,
# MAGIC S.`PBC_GL_ACCT_ID`,
# MAGIC S.`GENERATE_WSA`,
# MAGIC S.`HOLD_TRANSFER_WSA`,
# MAGIC S.`ACTIVE_FLAG`,
# MAGIC S.`UCT_WELDMENT_LABEL_FORMAT_ID`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC 'VE70',
# MAGIC now()
# MAGIC )
# MAGIC     

# COMMAND ----------

dbutils.fs.mv(read_path,archive_path, True)

# COMMAND ----------


