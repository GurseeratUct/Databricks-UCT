# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'PO_Detail'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Fact/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_ekko = [
"EBELN",
"AEDAT",
"BUKRS",
"KONNR",
"BSTYP",
"KUNNR",
"STAFO",
"INCO1",
"INCO2",
"INCO2_L",
"BSART",
"BSAKZ",
"STATU",
"ERNAM",
"LASTCHANGEDATETIME",
"PINCR",
"LPONR",
"LIFNR",
"SPRAS",
"ZTERM",
"ZBD1P",
"ZBD2P",
"EKORG",
"EKGRP",
"WAERS",
"WKURS",
"BEDAT",
"KTWRT",
"KNUMV",
"KALSM",
"UPINC",
"LANDS",
"STCEG_L",
"ABSGR",
"PROCSTAT",
"RLWRT",
"RETPC",
"DPPCT",
"DPAMT",
"SHIPCOND",
"GRWCU",
"EXT_REV_TMSTMP",
"FORCE_CNT",
"FSH_ITEM_GROUP",
"FSH_VAS_LAST_ITEM",
"ZAPCGK",
"APCGK_EXTEND",
"Z_DEV",
"PROCE",
"PFM_CONTRACT",
"KEY_ID",
"OTB_VALUE",
"OTB_RES_VALUE",
"OTB_SPEC_VALUE"
]

# COMMAND ----------

col_names_ekpo = [
"EBELN",
"EBELP",
"TXZ01",
"MATNR",
"WERKS",
"LGORT",
"MATKL",
"INFNR",
"KTMNG",
"MENGE",
"MEINS",
"BPRME",
"BPUMZ",
"BPUMN",
"UMREZ",
"UMREN",
"NETPR",
"PEINH",
"NETWR",
"WEBAZ",
"MWSKZ",
"SPINF",
"BWTAR",
"BWTTY",
"ELIKZ",
"PSTYP",
"WEPOS",
"KONNR",
"KTPNR",
"LMEIN",
"PRDAT",
"BSTYP",
"XOBLR",
"KUNNR",
"STAFO",
"PLIFZ",
"NTGEW",
"GEWEI",
"TXJCD",
"SSQSS",
"BSTAE",
"REVLV",
"KO_PRCTR",
"BRGEW",
"VOLUM",
"ATTYP",
"DRUHR",
"DRUNR",
"BANFN",
"BNFPO",
"MTART",
"LFRET",
"AFNAM",
"TZONRC",
 "knttp", 
"BERID",
"CREATIONTIME",
"ZZPPV",
"ZZREAS_CODE",
"ZZHOTQTY",
"ZZPQV",
"ZZEBELP",
"ZZMENGE",
"ZZPOSNR_TOLL",
"ZZCOITEM",
"AFPNR",
"SERNP",
"RETPO",
"LOEKZ"
]

# COMMAND ----------

col_names_ekkn = [
                   'EBELN','EBELP','ZEKKN','SAKTO','KOSTL'
                    ]

# COMMAND ----------

col_names_purchaseorder = [
 "ID",
"CREATE_DATE",
"VENDOR_ID",
"CURRENCY_ID",
"ORDER_DATE",
"STATUS",
"DESIRED_RECV_DATE",
"SHIP_VIA",
"SELL_RATE",
"BUY_RATE",
"TOTAL_AMT_ORDERED",
"SHIPTO_ID",
"TERMS_NET_TYPE",
"TERMS_NET_DAYS",
"TERMS_DISC_TYPE",
"TERMS_DISC_DAYS",
"TERMS_DISC_PERCENT",
"TERMS_DESCRIPTION",
"SHIPFROM_ID"
]

# COMMAND ----------

col_names_purchaseorderline = [
"PURC_ORDER_ID",
"LINE_NO",
"PART_ID",
"ORDER_QTY",
"PURCHASE_UM",
"UNIT_PRICE",
"SERVICE_ID",
"DESIRED_RECV_DATE",
"TOTAL_AMT_ORDERED",
"TOTAL_AMT_RECVD",
"VENDOR_PART_ID",
"USER_ORDER_QTY",
"LINE_STATUS",
"TOTAL_USR_RECD_QTY",
"TOTAL_RECEIVED_QTY",
"MFG_PART_ID",
"VAT_CODE",
"VAT_AMOUNT",
"VAT_RCV_AMOUNT",
"ORIG_STAGE_REVISION_ID"
]

# COMMAND ----------

df = spark.sql("select * from S42.EKKO where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EKKO' and DatabaseName = 'S42')")
df_ekko = df.select(col_names_ekko)
df_ekko.count()
df_ekko.createOrReplaceTempView("ekko_tmp")
df_ts_ekko = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ekko = df_ts_ekko["max(UpdatedOn)"]
print(ts_ekko)

# COMMAND ----------

df = spark.sql("select * from S42.EKPO where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EKPO' and DatabaseName = 'S42')")
df_ekpo = df.select(col_names_ekpo)
df_ekpo.createOrReplaceTempView("ekpo_tmp")
df_ts_ekpo = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ekpo = df_ts_ekpo["max(UpdatedOn)"]
print(ts_ekpo)

# COMMAND ----------

df = spark.sql("select * from S42.EKKN where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EKKN' and DatabaseName = 'S42')")
df_ekkn = df.select(col_names_ekkn)
df_ekkn.createOrReplaceTempView("ekkn_tmp")
df_ts_ekkn = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ekkn = df_ts_ekkn["max(UpdatedOn)"]
print(ts_ekkn)

# COMMAND ----------

plant_code_singapore = spark.sql("select variable_value from Config.config_constant where variable_name = 'plant_code_singapore'")
plant_code_singapore = plant_code_singapore.first()[0]
print(plant_code_singapore)

plant_code_shangai = spark.sql("select variable_value from Config.config_constant where variable_name = 'plant_code_shangai'")
plant_code_shangai = plant_code_shangai.first()[0]
print(plant_code_shangai)

company_code_shangai = spark.sql("select variable_value from Config.config_constant where variable_name = 'company_code_shangai'")
company_code_shangai = company_code_shangai.first()[0]
print(company_code_shangai)

company_code_singapore = spark.sql("select variable_value from Config.config_constant where variable_name = 'company_code_singapore'")
company_code_singapore = company_code_singapore.first()[0]
print(company_code_singapore)




# COMMAND ----------

df = spark.sql("select * from VE70.PURCHASE_ORDER where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PURCHASE_ORDER' and DatabaseName = 'VE70')")
df_ve70_purchase_order = df.select(col_names_purchaseorder).withColumn('CompanyCode',lit(company_code_shangai)).withColumn('Plant',lit(plant_code_shangai)).withColumn('DataSource',lit('VE70'))
df_ve70_purchase_order.createOrReplaceTempView("ve70_purchase_order_tmp")
df_ts_ve70_purchase_order = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_purchase_order = df_ts_ve70_purchase_order["max(UpdatedOn)"]
print(ts_ve70_purchase_order)

# COMMAND ----------

df = spark.sql("select * from VE72.PURCHASE_ORDER where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PURCHASE_ORDER' and DatabaseName = 'VE72')")
df_ve72_purchase_order = df.select(col_names_purchaseorder).withColumn('CompanyCode',lit(company_code_singapore)).withColumn('Plant',lit(plant_code_singapore)).withColumn('DataSource',lit('VE72'))
df_ve72_purchase_order.createOrReplaceTempView("ve72_purchase_order_tmp")
df_ts_ve72_purchase_order = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_purchase_order = df_ts_ve72_purchase_order["max(UpdatedOn)"]
print(ts_ve72_purchase_order)

# COMMAND ----------

df = spark.sql("select * from VE70.PURC_ORDER_LINE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PURC_ORDER_LINE' and DatabaseName = 'VE70')")
df_ve70_purc_order_line = df.select(col_names_purchaseorderline).withColumn('CompanyCode',lit(company_code_shangai)).withColumn('Plant',lit(plant_code_shangai)).withColumn('ValuationType',lit(plant_code_shangai)).withColumn('DataSource',lit('VE70')).withColumn('PurchasingOrganization',lit('0070'))
df_ve70_purc_order_line.createOrReplaceTempView("ve70_purc_order_line_tmp")
df_ts_ve70_purc_order_line = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_purc_order_line = df_ts_ve70_purc_order_line["max(UpdatedOn)"]
print(ts_ve70_purc_order_line)

# COMMAND ----------

df = spark.sql("select * from VE72.PURC_ORDER_LINE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PURC_ORDER_LINE' and DatabaseName = 'VE72')")
df_ve72_purc_order_line = df.select(col_names_purchaseorderline).withColumn('CompanyCode',lit(company_code_singapore)).withColumn('Plant',lit(plant_code_singapore)).withColumn('ValuationType',lit(company_code_singapore)).withColumn('DataSource',lit('VE72')).withColumn('PurchasingOrganization',lit('0072'))
df_ve72_purc_order_line.createOrReplaceTempView("ve72_purc_order_line_tmp")
df_ts_ve72_purc_order_line = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_purc_order_line = df_ts_ve72_purc_order_line["max(UpdatedOn)"]
print(ts_ve72_purc_order_line)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view tmp_ekko_ekpo_ekkn 
# MAGIC as
# MAGIC select
# MAGIC ep.EBELN,
# MAGIC ep.EBELP,
# MAGIC ek.AEDAT,
# MAGIC ep.TXZ01,
# MAGIC ep.MATNR,
# MAGIC ek.BUKRS,
# MAGIC ep.WERKS,
# MAGIC ep.LGORT,
# MAGIC ep.MATKL,
# MAGIC ep.INFNR,
# MAGIC ep.KTMNG,
# MAGIC ep.MENGE,
# MAGIC ep.MEINS,
# MAGIC ep.BPRME,
# MAGIC ep.BPUMZ,
# MAGIC ep.BPUMN,
# MAGIC ep.UMREZ,
# MAGIC ep.UMREN,
# MAGIC ep.NETPR,
# MAGIC ep.PEINH,
# MAGIC ep.NETWR,
# MAGIC ep.WEBAZ,
# MAGIC ep.MWSKZ,
# MAGIC ep.SPINF,
# MAGIC ep.BWTAR,
# MAGIC ep.BWTTY,
# MAGIC ep.ELIKZ,
# MAGIC ep.PSTYP,
# MAGIC ep.WEPOS,
# MAGIC ep.KONNR,
# MAGIC ep.KTPNR,
# MAGIC ep.LMEIN,
# MAGIC ep.PRDAT,
# MAGIC ep.BSTYP,
# MAGIC ep.XOBLR,
# MAGIC ep.KUNNR,
# MAGIC ep.STAFO,
# MAGIC ep.PLIFZ,
# MAGIC ep.NTGEW,
# MAGIC ep.GEWEI,
# MAGIC ep.TXJCD,
# MAGIC ep.SSQSS,
# MAGIC ep.BSTAE,
# MAGIC ep.REVLV,
# MAGIC ep.KO_PRCTR,
# MAGIC ep.BRGEW,
# MAGIC ep.knttp,
# MAGIC ep.VOLUM,
# MAGIC ep.RETPO,
# MAGIC ek.INCO1,
# MAGIC ek.INCO2,
# MAGIC ep.ATTYP,
# MAGIC ep.DRUHR,
# MAGIC ep.DRUNR,
# MAGIC ep.BANFN,
# MAGIC ep.BNFPO,
# MAGIC ep.MTART,
# MAGIC ep.LFRET,
# MAGIC ep.AFNAM,
# MAGIC ep.TZONRC,
# MAGIC ep.BERID,
# MAGIC ep.CREATIONTIME,
# MAGIC ek.INCO2_L,
# MAGIC ep.ZZPPV,
# MAGIC ep.ZZREAS_CODE,
# MAGIC ep.ZZHOTQTY,
# MAGIC ep.ZZPQV,
# MAGIC ep.ZZEBELP,
# MAGIC ep.ZZMENGE,
# MAGIC ep.ZZPOSNR_TOLL,
# MAGIC ep.ZZCOITEM,
# MAGIC ep.AFPNR,
# MAGIC ep.SERNP,
# MAGIC ek.BSART,
# MAGIC ek.BSAKZ,
# MAGIC ek.STATU,
# MAGIC ek.ERNAM,
# MAGIC ek.LASTCHANGEDATETIME,
# MAGIC ek.PINCR,
# MAGIC ek.LPONR,
# MAGIC ek.LIFNR,
# MAGIC ek.SPRAS,
# MAGIC ek.ZTERM,
# MAGIC ek.ZBD1P,
# MAGIC ek.ZBD2P,
# MAGIC ek.EKORG,
# MAGIC ek.EKGRP,
# MAGIC ek.WAERS,
# MAGIC ek.WKURS,
# MAGIC ek.BEDAT,
# MAGIC ek.KTWRT,
# MAGIC ek.KNUMV,
# MAGIC ek.KALSM,
# MAGIC ek.UPINC,
# MAGIC ek.LANDS,
# MAGIC ek.STCEG_L,
# MAGIC ek.ABSGR,
# MAGIC ek.PROCSTAT,
# MAGIC ek.RLWRT,
# MAGIC ek.RETPC,
# MAGIC ek.DPPCT,
# MAGIC ek.DPAMT,
# MAGIC ek.SHIPCOND,
# MAGIC ek.GRWCU,
# MAGIC ek.EXT_REV_TMSTMP,
# MAGIC ek.FORCE_CNT,
# MAGIC ek.FSH_ITEM_GROUP,
# MAGIC ek.FSH_VAS_LAST_ITEM,
# MAGIC ek.ZAPCGK,
# MAGIC ek.APCGK_EXTEND,
# MAGIC ek.Z_DEV,
# MAGIC ek.PROCE,
# MAGIC ek.PFM_CONTRACT,
# MAGIC ek.KEY_ID,
# MAGIC ek.OTB_VALUE,
# MAGIC ek.OTB_RES_VALUE,
# MAGIC ek.OTB_SPEC_VALUE,
# MAGIC ep.LOEKZ,
# MAGIC en.sakto,
# MAGIC en.kostl,
# MAGIC 'SAP' as DataSource
# MAGIC from ekpo_tmp as ep
# MAGIC left join ekko_tmp as ek
# MAGIC on ep.EBELN = ek.EBELN
# MAGIC left join (select * from ekkn_tmp where ZEKKN = '1') as en
# MAGIC on ep.EBELN = en.EBELN
# MAGIC and ep.EBELP = en.EBELP;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view tmp_ve70_purchase_order_po_line 
# MAGIC as
# MAGIC select 
# MAGIC pl.PURC_ORDER_ID,
# MAGIC pl.LINE_NO,
# MAGIC trim(pl.PART_ID) as PART_ID,
# MAGIC pl.Plant,
# MAGIC pl.CompanyCode,
# MAGIC pl.ValuationType,
# MAGIC pl.ORDER_QTY,
# MAGIC pl.PURCHASE_UM,
# MAGIC pl.UNIT_PRICE,
# MAGIC pl.SERVICE_ID,
# MAGIC pl.DESIRED_RECV_DATE,
# MAGIC pl.TOTAL_AMT_ORDERED,
# MAGIC pl.TOTAL_AMT_RECVD,
# MAGIC pl.VENDOR_PART_ID,
# MAGIC pl.USER_ORDER_QTY,
# MAGIC pl.LINE_STATUS,
# MAGIC pl.TOTAL_USR_RECD_QTY,
# MAGIC pl.TOTAL_RECEIVED_QTY,
# MAGIC pl.MFG_PART_ID,
# MAGIC pl.VAT_CODE,
# MAGIC pl.VAT_AMOUNT,
# MAGIC pl.VAT_RCV_AMOUNT,
# MAGIC pl.ORIG_STAGE_REVISION_ID,
# MAGIC pl.PurchasingOrganization,
# MAGIC po.CREATE_DATE,
# MAGIC po.VENDOR_ID,
# MAGIC po.CURRENCY_ID,
# MAGIC po.ORDER_DATE,
# MAGIC po.STATUS,
# MAGIC po.SHIP_VIA,
# MAGIC po.SELL_RATE,
# MAGIC po.BUY_RATE,
# MAGIC po.SHIPTO_ID,
# MAGIC po.TERMS_NET_TYPE,
# MAGIC po.TERMS_NET_DAYS,
# MAGIC po.TERMS_DISC_TYPE,
# MAGIC po.TERMS_DISC_DAYS,
# MAGIC po.TERMS_DISC_PERCENT,
# MAGIC po.TERMS_DESCRIPTION,
# MAGIC po.SHIPFROM_ID,
# MAGIC pl.DataSource
# MAGIC from ve70_purc_order_line_tmp as pl
# MAGIC left join 
# MAGIC ve70_purchase_order_tmp as po
# MAGIC on pl.PURC_ORDER_ID = po.ID

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view tmp_ve72_purchase_order_po_line 
# MAGIC as
# MAGIC select 
# MAGIC pl.PURC_ORDER_ID,
# MAGIC pl.LINE_NO,
# MAGIC pl.PART_ID,
# MAGIC pl.Plant,
# MAGIC pl.CompanyCode,
# MAGIC pl.ValuationType,
# MAGIC pl.ORDER_QTY,
# MAGIC pl.PURCHASE_UM,
# MAGIC pl.UNIT_PRICE,
# MAGIC pl.SERVICE_ID,
# MAGIC pl.DESIRED_RECV_DATE,
# MAGIC pl.TOTAL_AMT_ORDERED,
# MAGIC pl.TOTAL_AMT_RECVD,
# MAGIC pl.VENDOR_PART_ID,
# MAGIC pl.USER_ORDER_QTY,
# MAGIC pl.LINE_STATUS,
# MAGIC pl.TOTAL_USR_RECD_QTY,
# MAGIC pl.TOTAL_RECEIVED_QTY,
# MAGIC pl.MFG_PART_ID,
# MAGIC pl.VAT_CODE,
# MAGIC pl.VAT_AMOUNT,
# MAGIC pl.VAT_RCV_AMOUNT,
# MAGIC pl.ORIG_STAGE_REVISION_ID,
# MAGIC pl.PurchasingOrganization,
# MAGIC po.CREATE_DATE,
# MAGIC po.VENDOR_ID,
# MAGIC po.CURRENCY_ID,
# MAGIC po.ORDER_DATE,
# MAGIC po.STATUS,
# MAGIC po.SHIP_VIA,
# MAGIC po.SELL_RATE,
# MAGIC po.BUY_RATE,
# MAGIC po.SHIPTO_ID,
# MAGIC po.TERMS_NET_TYPE,
# MAGIC po.TERMS_NET_DAYS,
# MAGIC po.TERMS_DISC_TYPE,
# MAGIC po.TERMS_DISC_DAYS,
# MAGIC po.TERMS_DISC_PERCENT,
# MAGIC po.TERMS_DESCRIPTION,
# MAGIC po.SHIPFROM_ID,
# MAGIC pl.DataSource
# MAGIC from ve72_purc_order_line_tmp as pl
# MAGIC left join 
# MAGIC ve72_purchase_order_tmp as po
# MAGIC on pl.PURC_ORDER_ID = po.ID

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view tmp_purchase_order_po_line
# MAGIC as
# MAGIC select * from tmp_ve70_purchase_order_po_line
# MAGIC union 
# MAGIC select * from tmp_ve72_purchase_order_po_line

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view PO_Detail
# MAGIC as
# MAGIC select
# MAGIC case when S.EBELN IS NULL then V.PURC_ORDER_ID else S.EBELN end as PurchDocNumber,
# MAGIC case when S.EBELN IS NULL then V.LINE_NO else S.EBELP end as PurchDocItem,
# MAGIC case when S.EBELN IS NULL then V.CREATE_DATE else S.AEDAT end as PurDocCreateDate,
# MAGIC S.TXZ01 as Description,
# MAGIC case when S.EBELN IS NULL then V.PART_ID else S.MATNR end as MaterialNumber,
# MAGIC case when S.EBELN IS NULL then V.Plant else S.WERKS end as Plant,
# MAGIC case when S.EBELN IS NULL then V.CompanyCode else S.BUKRS end as CompanyCode,
# MAGIC S.LGORT as StorageLocation,
# MAGIC S.MATKL as MaterialGroup,
# MAGIC S.INFNR as PurchInfoRecord,
# MAGIC S.KTMNG as TargetQuantity,
# MAGIC case when S.EBELN IS NULL then V.ORDER_QTY else S.MENGE end as PurOrderQty,
# MAGIC case when S.EBELN IS NULL then V.PURCHASE_UM else S.MEINS end as PurchOrdUOM,
# MAGIC S.BPRME as OrderPriceUnit,
# MAGIC S.BPUMZ as NumeConvOrderPriceUnitToOrdUnit,
# MAGIC S.BPUMN as DenoConvOrderPriceUnitToOrdUnit,
# MAGIC S.UMREZ as NumeConvOrdUnitToBaseUnit,
# MAGIC S.UMREN as DenoConvOrderUnitToBaseUnit,
# MAGIC case when S.EBELN IS NULL then 1 else S.PEINH end as PriceUnit,
# MAGIC case when S.EBELN IS NULL then V.UNIT_PRICE else S.NETPR end as PurchDocNetPrice,
# MAGIC case when S.EBELN IS NULL then V.UNIT_PRICE*V.ORDER_QTY else S.NETWR end as NetOrdValuePOCurr,
# MAGIC S.WEBAZ as GRProcessTimeInDays,
# MAGIC S.MWSKZ as TaxSalPurchCode,
# MAGIC S.SPINF as IndicatUpdateInfoRecord,
# MAGIC case when S.EBELN IS NULL then V.ValuationType else S.BWTAR end as ValuationType,
# MAGIC S.BWTTY as ValuationCategory,
# MAGIC S.ELIKZ as DeliCompleteIndicator,
# MAGIC case when S.EBELN IS NULL then V.SERVICE_ID else S.PSTYP end as PurchDocItemCategory,
# MAGIC S.WEPOS as GoodsReceiptIndicator,
# MAGIC S.KONNR as PrincipalPurchaseAgreement,
# MAGIC S.KTPNR as PrincipalPurchaseAgreementItem,
# MAGIC S.LMEIN as BaseUnitOfMeasure,
# MAGIC S.PRDAT as DateOfPriceDetermination,
# MAGIC S.BSTYP as PurchasingDocumentCategory,
# MAGIC S.XOBLR as ItemAffectsCommitments,
# MAGIC S.KUNNR as Customer,
# MAGIC S.STAFO as UpdateGroupForStatisticsUpdate,
# MAGIC S.PLIFZ as PlannedDeliveryInDays,
# MAGIC S.NTGEW as NetWeight,
# MAGIC case when S.EBELN IS NULL then 0 else S.KNTTP end as AccountAssignmentCategory,
# MAGIC S.GEWEI as UnitOfWeight,
# MAGIC S.TXJCD as TaxJurisdiction,
# MAGIC S.SSQSS as ControlKeyQualityMgmtInProcurement,
# MAGIC S.BSTAE as ConfirmationControlKey,
# MAGIC S.REVLV as RevisionLevel,
# MAGIC S.KO_PRCTR as ProfitCenter,
# MAGIC S.BRGEW as GrossWeight,
# MAGIC S.VOLUM as Volume,
# MAGIC S.INCO1 as IncotermsPart1,
# MAGIC S.INCO2 as IncotermsPart2,
# MAGIC S.ATTYP as MaterialCategory,
# MAGIC S.DRUHR as Time,
# MAGIC S.DRUNR as SequentialNumber,
# MAGIC S.BANFN as PurchaseRequisitionNumber,
# MAGIC S.BNFPO as PurchaseRequisitionItemNumber,
# MAGIC S.MTART as MaterialType,
# MAGIC S.LFRET as DeliveryTypeForReturnsToSupplier,
# MAGIC S.AFNAM as RequisitionerOrRequester,
# MAGIC S.TZONRC as TimeZoneRecipientLocation,
# MAGIC S.BERID as MRPArea,
# MAGIC S.KOSTL as CostCenter,
# MAGIC S.SAKTO as GLAccountNumber,
# MAGIC S.CREATIONTIME as PurchaseDocCreateTime,
# MAGIC S.INCO2_L as IncotermsLocation1,
# MAGIC S.ZZPPV as PPV,
# MAGIC S.ZZREAS_CODE as ReasonCode,
# MAGIC S.ZZHOTQTY as HotQuantity,
# MAGIC S.ZZPQV as PQV,
# MAGIC S.ZZEBELP as FromPOLineItem,
# MAGIC S.ZZMENGE as OriginalPOLineItemQuantity,
# MAGIC S.ZZPOSNR_TOLL as SalesDocumentItem,
# MAGIC S.ZZCOITEM as VisualCustomerOrderItem,
# MAGIC S.AFPNR as SalesDocumentItems,
# MAGIC S.SERNP as SerialNumberProfile,
# MAGIC S.BSART as PurchasingDocType,
# MAGIC S.BSAKZ as PurchaseDocTypeControlIndicator,
# MAGIC S.STATU as PurchasingDocumentStatus,
# MAGIC S.ERNAM as ObjectCreator,
# MAGIC S.LASTCHANGEDATETIME as ChangeTimeStamp,
# MAGIC S.PINCR as ItemNumberInterval,
# MAGIC S.LPONR as LastItemNumber,
# MAGIC case when S.EBELN IS NULL then V.VENDOR_ID  else S.LIFNR end as VendorNumber,
# MAGIC S.SPRAS as LanguageKey,
# MAGIC S.ZTERM as TermsOfPaymentKey,
# MAGIC S.ZBD1P as CashDiscountPercentage1,
# MAGIC S.ZBD2P as CashDiscountPercentage2,
# MAGIC case when S.EBELN IS NULL then V.PurchasingOrganization else S.EKORG end as PurchasingOrganization,
# MAGIC S.EKGRP as PurchasingGroup,
# MAGIC case when S.EBELN IS NULL then V.CURRENCY_ID else S.WAERS end as CurrencyKey,
# MAGIC S.WKURS as ExchangeRate,
# MAGIC case when S.EBELN IS NULL then V.ORDER_DATE else S.BEDAT end as PurchasingDocumentDate,
# MAGIC S.KTWRT as TargetValueHeaderAreaPerDistribution,
# MAGIC S.KNUMV as DocumentConditionNumber,
# MAGIC S.KALSM as ProcedurePricing,
# MAGIC S.UPINC as ItemNumberIntervalSubitems,
# MAGIC S.LANDS as CountryTaxReturn,
# MAGIC S.STCEG_L as CountrySalesTaxIDNumber,
# MAGIC S.ABSGR as ReasonForCancellation,
# MAGIC S.PROCSTAT as PurchasingDocumentProcessingState,
# MAGIC S.RLWRT as TotalValueTimeOfRelease,
# MAGIC S.RETPC as RetentionInPercent,
# MAGIC S.DPPCT as DownPaymentPercentage,
# MAGIC S.DPAMT as DownPaymentAmountDocCurrency,
# MAGIC S.SHIPCOND as ShippingConditions,
# MAGIC S.GRWCU as ForeignTradeStatisticalValuesCurrency,
# MAGIC S.EXT_REV_TMSTMP as TimestampRevisionExternalCalls,
# MAGIC S.FORCE_CNT as InternalCounter,
# MAGIC S.FSH_ITEM_GROUP as ItemGroup,
# MAGIC S.FSH_VAS_LAST_ITEM as LastVASItemNumber,
# MAGIC S.ZAPCGK as AnnexingPackageKey,
# MAGIC S.APCGK_EXTEND as ExtendedKeyAnnexingPackage,
# MAGIC S.Z_DEV as DeviationPercentage,
# MAGIC S.PROCE as ProcedureNumber,
# MAGIC S.PFM_CONTRACT as PTFMContract,
# MAGIC S.KEY_ID as BudgetUniqueNumber,
# MAGIC S.OTB_VALUE as RequiredBudget,
# MAGIC S.OTB_RES_VALUE as OTBReservedBudget,
# MAGIC S.OTB_SPEC_VALUE as SpecialReleaseBudget,
# MAGIC case when S.EBELN IS NULL then V.STATUS else S.LOEKZ  end as PurchDocDelInd,
# MAGIC S.RETPO as ReturnsItem,
# MAGIC V.DESIRED_RECV_DATE as DesiredRecvDate,
# MAGIC V.TOTAL_AMT_ORDERED as TotalAmtOrdered,
# MAGIC V.TOTAL_AMT_RECVD as TotalAmtRecvd,
# MAGIC V.VENDOR_PART_ID as VendorPartID,
# MAGIC V.USER_ORDER_QTY as UserOrderQty,
# MAGIC V.LINE_STATUS as LineStatus,
# MAGIC V.TOTAL_USR_RECD_QTY as TotalUserRecvdQty,
# MAGIC V.TOTAL_RECEIVED_QTY as TotalRecvdQty,
# MAGIC V.MFG_PART_ID as MfgPartID,
# MAGIC V.VAT_CODE as VATCode,
# MAGIC V.VAT_AMOUNT as VATAmount,
# MAGIC V.VAT_RCV_AMOUNT as VATRecvdAmount,
# MAGIC V.ORIG_STAGE_REVISION_ID as OrigStateRevisionID,
# MAGIC V.SHIPTO_ID as ShipToID,
# MAGIC V.SHIPFROM_ID as ShipFromID,
# MAGIC V.SHIP_VIA as ShipVia,
# MAGIC V.SELL_RATE as SellRate,
# MAGIC V.BUY_RATE as BuyRate,
# MAGIC V.TERMS_NET_TYPE as TermsNetType,
# MAGIC V.TERMS_NET_DAYS as TermsNetDays,
# MAGIC V.TERMS_DISC_TYPE as TermsDiscType,
# MAGIC V.TERMS_DISC_DAYS as TermsDiscDays,
# MAGIC V.TERMS_DISC_PERCENT as TermsDiscPercent,
# MAGIC V.TERMS_DESCRIPTION as TermsDescription,
# MAGIC now() as UpdatedOn,
# MAGIC case when S.EBELN IS NULL then V.DataSource else S.DataSource end as DataSource
# MAGIC from tmp_ekko_ekpo_ekkn as S
# MAGIC full join 
# MAGIC tmp_purchase_order_po_line as V
# MAGIC on S.EBELN = V.PURC_ORDER_ID and S.EBELP = V.LINE_NO

# COMMAND ----------

df_fact_purchase = spark.sql("select * from PO_Detail")

# COMMAND ----------

df_fact_purchase.count()

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_fact_purchase.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")    

# COMMAND ----------

df_fact_purchase.createOrReplaceTempView('tmp_PoDetail')

# COMMAND ----------

# MAGIC %sql 
# MAGIC Merge into fedw.PO_Detail as S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY PurchDocNumber,PurchDocItem ORDER BY PurchDocNumber DESC) as rn,* from tmp_PoDetail)A where A.rn = 1 ) as T 
# MAGIC on S.PurchDocNumber = T.PurchDocNumber and S.PurchDocItem = T.PurchDocItem and S.DataSource = T.DataSource
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC S.PurchDocNumber = T.PurchDocNumber,
# MAGIC S.PurchDocItem = T.PurchDocItem,
# MAGIC S.PurDocCreateDate = T.PurDocCreateDate,
# MAGIC S.Description = T.Description,
# MAGIC S.MaterialNumber = T.MaterialNumber,
# MAGIC S.Plant = T.Plant,
# MAGIC S.CompanyCode = T.CompanyCode,
# MAGIC S.StorageLocation = T.StorageLocation,
# MAGIC S.MaterialGroup = T.MaterialGroup,
# MAGIC S.PurchInfoRecord = T.PurchInfoRecord,
# MAGIC S.TargetQuantity = T.TargetQuantity,
# MAGIC S.PurOrderQty = T.PurOrderQty,
# MAGIC S.PurchOrdUOM = T.PurchOrdUOM,
# MAGIC S.OrderPriceUnit = T.OrderPriceUnit,
# MAGIC S.NumeConvOrderPriceUnitToOrdUnit = T.NumeConvOrderPriceUnitToOrdUnit,
# MAGIC S.DenoConvOrderPriceUnitToOrdUnit = T.DenoConvOrderPriceUnitToOrdUnit,
# MAGIC S.NumeConvOrdUnitToBaseUnit = T.NumeConvOrdUnitToBaseUnit,
# MAGIC S.DenoConvOrderUnitToBaseUnit = T.DenoConvOrderUnitToBaseUnit,
# MAGIC S.AccountAssignmentCategory =  T.AccountAssignmentCategory,
# MAGIC S.PriceUnit = T.PriceUnit,
# MAGIC S.PurchDocNetPrice = T.PurchDocNetPrice,
# MAGIC S.NetOrdValuePOCurr = T.NetOrdValuePOCurr,
# MAGIC S.GRProcessTimeInDays = T.GRProcessTimeInDays,
# MAGIC S.TaxSalPurchCode = T.TaxSalPurchCode,
# MAGIC S.IndicatUpdateInfoRecord = T.IndicatUpdateInfoRecord,
# MAGIC S.ValuationType = T.ValuationType,
# MAGIC S.ValuationCategory = T.ValuationCategory,
# MAGIC S.DeliCompleteIndicator = T.DeliCompleteIndicator,
# MAGIC S.PurchDocItemCategory = T.PurchDocItemCategory,
# MAGIC S.GoodsReceiptIndicator = T.GoodsReceiptIndicator,
# MAGIC S.PrincipalPurchaseAgreement = T.PrincipalPurchaseAgreement,
# MAGIC S.PrincipalPurchaseAgreementItem = T.PrincipalPurchaseAgreementItem,
# MAGIC S.BaseUnitOfMeasure = T.BaseUnitOfMeasure,
# MAGIC S.DateOfPriceDetermination = T.DateOfPriceDetermination,
# MAGIC S.PurchasingDocumentCategory = T.PurchasingDocumentCategory,
# MAGIC S.ItemAffectsCommitments = T.ItemAffectsCommitments,
# MAGIC S.Customer = T.Customer,
# MAGIC S.UpdateGroupForStatisticsUpdate = T.UpdateGroupForStatisticsUpdate,
# MAGIC S.PlannedDeliveryInDays = T.PlannedDeliveryInDays,
# MAGIC S.NetWeight = T.NetWeight,
# MAGIC S.UnitOfWeight = T.UnitOfWeight,
# MAGIC S.TaxJurisdiction = T.TaxJurisdiction,
# MAGIC S.ControlKeyQualityMgmtInProcurement = T.ControlKeyQualityMgmtInProcurement,
# MAGIC S.ConfirmationControlKey = T.ConfirmationControlKey,
# MAGIC S.RevisionLevel = T.RevisionLevel,
# MAGIC S.ProfitCenter = T.ProfitCenter,
# MAGIC S.GrossWeight = T.GrossWeight,
# MAGIC S.Volume = T.Volume,
# MAGIC S.IncotermsPart1 = T.IncotermsPart1,
# MAGIC S.IncotermsPart2 = T.IncotermsPart2,
# MAGIC S.MaterialCategory = T.MaterialCategory,
# MAGIC S.Time = T.Time,
# MAGIC S.SequentialNumber = T.SequentialNumber,
# MAGIC S.PurchaseRequisitionNumber = T.PurchaseRequisitionNumber,
# MAGIC S.PurchaseRequisitionItemNumber = T.PurchaseRequisitionItemNumber,
# MAGIC S.MaterialType = T.MaterialType,
# MAGIC S.DeliveryTypeForReturnsToSupplier = T.DeliveryTypeForReturnsToSupplier,
# MAGIC S.RequisitionerOrRequester = T.RequisitionerOrRequester,
# MAGIC S.TimeZoneRecipientLocation = T.TimeZoneRecipientLocation,
# MAGIC S.MRPArea = T.MRPArea,
# MAGIC S.CostCenter = T.CostCenter,
# MAGIC S.GLAccountNumber = T.GLAccountNumber,
# MAGIC S.PurchaseDocCreateTime = T.PurchaseDocCreateTime,
# MAGIC S.IncotermsLocation1 = T.IncotermsLocation1,
# MAGIC S.PPV = T.PPV,
# MAGIC S.ReasonCode = T.ReasonCode,
# MAGIC S.HotQuantity = T.HotQuantity,
# MAGIC S.PQV = T.PQV,
# MAGIC S.FromPOLineItem = T.FromPOLineItem,
# MAGIC S.OriginalPOLineItemQuantity = T.OriginalPOLineItemQuantity,
# MAGIC S.SalesDocumentItem = T.SalesDocumentItem,
# MAGIC S.VisualCustomerOrderItem = T.VisualCustomerOrderItem,
# MAGIC S.SalesDocumentItems = T.SalesDocumentItems,
# MAGIC S.SerialNumberProfile = T.SerialNumberProfile,
# MAGIC S.PurchasingDocType = T.PurchasingDocType,
# MAGIC S.PurchaseDocTypeControlIndicator = T.PurchaseDocTypeControlIndicator,
# MAGIC S.PurchasingDocumentStatus = T.PurchasingDocumentStatus,
# MAGIC S.ObjectCreator = T.ObjectCreator,
# MAGIC S.ChangeTimeStamp = T.ChangeTimeStamp,
# MAGIC S.ItemNumberInterval = T.ItemNumberInterval,
# MAGIC S.LastItemNumber = T.LastItemNumber,
# MAGIC S.VendorNumber = T.VendorNumber,
# MAGIC S.LanguageKey = T.LanguageKey,
# MAGIC S.TermsOfPaymentKey = T.TermsOfPaymentKey,
# MAGIC S.CashDiscountPercentage1 = T.CashDiscountPercentage1,
# MAGIC S.CashDiscountPercentage2 = T.CashDiscountPercentage2,
# MAGIC S.PurchasingOrganization = T.PurchasingOrganization,
# MAGIC S.PurchasingGroup = T.PurchasingGroup,
# MAGIC S.CurrencyKey = T.CurrencyKey,
# MAGIC S.ExchangeRate = T.ExchangeRate,
# MAGIC S.PurchasingDocumentDate = T.PurchasingDocumentDate,
# MAGIC S.TargetValueHeaderAreaPerDistribution = T.TargetValueHeaderAreaPerDistribution,
# MAGIC S.DocumentConditionNumber = T.DocumentConditionNumber,
# MAGIC S.ProcedurePricing = T.ProcedurePricing,
# MAGIC S.ItemNumberIntervalSubitems = T.ItemNumberIntervalSubitems,
# MAGIC S.CountryTaxReturn = T.CountryTaxReturn,
# MAGIC S.CountrySalesTaxIDNumber = T.CountrySalesTaxIDNumber,
# MAGIC S.ReasonForCancellation = T.ReasonForCancellation,
# MAGIC S.PurchasingDocumentProcessingState = T.PurchasingDocumentProcessingState,
# MAGIC S.TotalValueTimeOfRelease = T.TotalValueTimeOfRelease,
# MAGIC S.RetentionInPercent = T.RetentionInPercent,
# MAGIC S.DownPaymentPercentage = T.DownPaymentPercentage,
# MAGIC S.DownPaymentAmountDocCurrency = T.DownPaymentAmountDocCurrency,
# MAGIC S.ShippingConditions = T.ShippingConditions,
# MAGIC S.ForeignTradeStatisticalValuesCurrency = T.ForeignTradeStatisticalValuesCurrency,
# MAGIC S.TimestampRevisionExternalCalls = T.TimestampRevisionExternalCalls,
# MAGIC S.InternalCounter = T.InternalCounter,
# MAGIC S.ItemGroup = T.ItemGroup,
# MAGIC S.LastVASItemNumber = T.LastVASItemNumber,
# MAGIC S.AnnexingPackageKey = T.AnnexingPackageKey,
# MAGIC S.ExtendedKeyAnnexingPackage = T.ExtendedKeyAnnexingPackage,
# MAGIC S.DeviationPercentage = T.DeviationPercentage,
# MAGIC S.ProcedureNumber = T.ProcedureNumber,
# MAGIC S.PTFMContract = T.PTFMContract,
# MAGIC S.BudgetUniqueNumber = T.BudgetUniqueNumber,
# MAGIC S.RequiredBudget = T.RequiredBudget,
# MAGIC S.OTBReservedBudget = T.OTBReservedBudget,
# MAGIC S.SpecialReleaseBudget = T.SpecialReleaseBudget,
# MAGIC S.PurchDocDelInd = T.PurchDocDelInd,
# MAGIC S.ReturnsItem = T.ReturnsItem,
# MAGIC S.DesiredRecvDate = T.DesiredRecvDate,
# MAGIC S.TotalAmtOrdered = T.TotalAmtOrdered,
# MAGIC S.TotalAmtRecvd = T.TotalAmtRecvd,
# MAGIC S.VendorPartID = T.VendorPartID,
# MAGIC S.UserOrderQty = T.UserOrderQty,
# MAGIC S.LineStatus = T.LineStatus,
# MAGIC S.TotalUserRecvdQty = T.TotalUserRecvdQty,
# MAGIC S.TotalRecvdQty = T.TotalRecvdQty,
# MAGIC S.MfgPartID = T.MfgPartID,
# MAGIC S.VATCode = T.VATCode,
# MAGIC S.VATAmount = T.VATAmount,
# MAGIC S.VATRecvdAmount = T.VATRecvdAmount,
# MAGIC S.OrigStateRevisionID = T.OrigStateRevisionID,
# MAGIC S.ShipToID = T.ShipToID,
# MAGIC S.ShipFromID = T.ShipFromID,
# MAGIC S.ShipVia = T.ShipVia,
# MAGIC S.SellRate = T.SellRate,
# MAGIC S.BuyRate = T.BuyRate,
# MAGIC S.TermsNetType = T.TermsNetType,
# MAGIC S.TermsNetDays = T.TermsNetDays,
# MAGIC S.TermsDiscType = T.TermsDiscType,
# MAGIC S.TermsDiscDays = T.TermsDiscDays,
# MAGIC S.TermsDiscPercent = T.TermsDiscPercent,
# MAGIC S.TermsDescription = T.TermsDescription,
# MAGIC S.UpdatedOn = now(),
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     PurchDocNumber ,
# MAGIC PurchDocItem ,
# MAGIC PurDocCreateDate ,
# MAGIC Description ,
# MAGIC MaterialNumber ,
# MAGIC Plant ,
# MAGIC CompanyCode ,
# MAGIC StorageLocation ,
# MAGIC MaterialGroup ,
# MAGIC PurchInfoRecord ,
# MAGIC TargetQuantity ,
# MAGIC PurOrderQty ,
# MAGIC PurchOrdUOM ,
# MAGIC OrderPriceUnit ,
# MAGIC NumeConvOrderPriceUnitToOrdUnit ,
# MAGIC DenoConvOrderPriceUnitToOrdUnit ,
# MAGIC NumeConvOrdUnitToBaseUnit ,
# MAGIC DenoConvOrderUnitToBaseUnit ,
# MAGIC AccountAssignmentCategory,
# MAGIC PriceUnit ,
# MAGIC PurchDocNetPrice ,
# MAGIC NetOrdValuePOCurr ,
# MAGIC GRProcessTimeInDays ,
# MAGIC TaxSalPurchCode ,
# MAGIC IndicatUpdateInfoRecord ,
# MAGIC ValuationType ,
# MAGIC ValuationCategory ,
# MAGIC DeliCompleteIndicator ,
# MAGIC PurchDocItemCategory ,
# MAGIC GoodsReceiptIndicator ,
# MAGIC PrincipalPurchaseAgreement ,
# MAGIC PrincipalPurchaseAgreementItem ,
# MAGIC BaseUnitOfMeasure ,
# MAGIC DateOfPriceDetermination ,
# MAGIC PurchasingDocumentCategory ,
# MAGIC ItemAffectsCommitments ,
# MAGIC Customer ,
# MAGIC UpdateGroupForStatisticsUpdate ,
# MAGIC PlannedDeliveryInDays ,
# MAGIC NetWeight ,
# MAGIC UnitOfWeight ,
# MAGIC TaxJurisdiction ,
# MAGIC ControlKeyQualityMgmtInProcurement ,
# MAGIC ConfirmationControlKey ,
# MAGIC RevisionLevel ,
# MAGIC ProfitCenter ,
# MAGIC GrossWeight ,
# MAGIC Volume ,
# MAGIC IncotermsPart1 ,
# MAGIC IncotermsPart2 ,
# MAGIC MaterialCategory ,
# MAGIC Time ,
# MAGIC SequentialNumber ,
# MAGIC PurchaseRequisitionNumber ,
# MAGIC PurchaseRequisitionItemNumber ,
# MAGIC MaterialType ,
# MAGIC DeliveryTypeForReturnsToSupplier ,
# MAGIC RequisitionerOrRequester ,
# MAGIC TimeZoneRecipientLocation ,
# MAGIC MRPArea ,
# MAGIC CostCenter ,
# MAGIC GLAccountNumber ,
# MAGIC PurchaseDocCreateTime ,
# MAGIC IncotermsLocation1 ,
# MAGIC PPV ,
# MAGIC ReasonCode ,
# MAGIC HotQuantity ,
# MAGIC PQV ,
# MAGIC FromPOLineItem ,
# MAGIC OriginalPOLineItemQuantity ,
# MAGIC SalesDocumentItem ,
# MAGIC VisualCustomerOrderItem ,
# MAGIC SalesDocumentItems ,
# MAGIC SerialNumberProfile ,
# MAGIC PurchasingDocType ,
# MAGIC PurchaseDocTypeControlIndicator ,
# MAGIC PurchasingDocumentStatus ,
# MAGIC ObjectCreator ,
# MAGIC ChangeTimeStamp ,
# MAGIC ItemNumberInterval ,
# MAGIC LastItemNumber ,
# MAGIC VendorNumber ,
# MAGIC LanguageKey ,
# MAGIC TermsOfPaymentKey ,
# MAGIC CashDiscountPercentage1 ,
# MAGIC CashDiscountPercentage2 ,
# MAGIC PurchasingOrganization ,
# MAGIC PurchasingGroup ,
# MAGIC CurrencyKey ,
# MAGIC ExchangeRate ,
# MAGIC PurchasingDocumentDate ,
# MAGIC TargetValueHeaderAreaPerDistribution ,
# MAGIC DocumentConditionNumber ,
# MAGIC ProcedurePricing ,
# MAGIC ItemNumberIntervalSubitems ,
# MAGIC CountryTaxReturn ,
# MAGIC CountrySalesTaxIDNumber ,
# MAGIC ReasonForCancellation ,
# MAGIC PurchasingDocumentProcessingState ,
# MAGIC TotalValueTimeOfRelease ,
# MAGIC RetentionInPercent ,
# MAGIC DownPaymentPercentage ,
# MAGIC DownPaymentAmountDocCurrency ,
# MAGIC ShippingConditions ,
# MAGIC ForeignTradeStatisticalValuesCurrency ,
# MAGIC TimestampRevisionExternalCalls ,
# MAGIC InternalCounter ,
# MAGIC ItemGroup ,
# MAGIC LastVASItemNumber ,
# MAGIC AnnexingPackageKey ,
# MAGIC ExtendedKeyAnnexingPackage ,
# MAGIC DeviationPercentage ,
# MAGIC ProcedureNumber ,
# MAGIC PTFMContract ,
# MAGIC BudgetUniqueNumber ,
# MAGIC RequiredBudget ,
# MAGIC OTBReservedBudget ,
# MAGIC SpecialReleaseBudget ,
# MAGIC PurchDocDelInd ,
# MAGIC ReturnsItem,
# MAGIC DesiredRecvDate ,
# MAGIC TotalAmtOrdered ,
# MAGIC TotalAmtRecvd ,
# MAGIC VendorPartID ,
# MAGIC UserOrderQty ,
# MAGIC LineStatus ,
# MAGIC TotalUserRecvdQty ,
# MAGIC TotalRecvdQty ,
# MAGIC MfgPartID ,
# MAGIC VATCode ,
# MAGIC VATAmount ,
# MAGIC VATRecvdAmount ,
# MAGIC OrigStateRevisionID ,
# MAGIC ShipToID ,
# MAGIC ShipFromID ,
# MAGIC ShipVia ,
# MAGIC SellRate ,
# MAGIC BuyRate ,
# MAGIC TermsNetType ,
# MAGIC TermsNetDays ,
# MAGIC TermsDiscType ,
# MAGIC TermsDiscDays ,
# MAGIC TermsDiscPercent ,
# MAGIC TermsDescription ,
# MAGIC UpdatedOn ,
# MAGIC DataSource)
# MAGIC Values
# MAGIC (
# MAGIC T.PurchDocNumber ,
# MAGIC T.PurchDocItem ,
# MAGIC T.PurDocCreateDate ,
# MAGIC T.Description ,
# MAGIC T.MaterialNumber ,
# MAGIC T.Plant ,
# MAGIC T.CompanyCode ,
# MAGIC T.StorageLocation ,
# MAGIC T.MaterialGroup ,
# MAGIC T.PurchInfoRecord ,
# MAGIC T.TargetQuantity ,
# MAGIC T.PurOrderQty ,
# MAGIC T.PurchOrdUOM ,
# MAGIC T.OrderPriceUnit ,
# MAGIC T.NumeConvOrderPriceUnitToOrdUnit ,
# MAGIC T.DenoConvOrderPriceUnitToOrdUnit ,
# MAGIC T.NumeConvOrdUnitToBaseUnit ,
# MAGIC T.DenoConvOrderUnitToBaseUnit ,
# MAGIC T.AccountAssignmentCategory,
# MAGIC T.PriceUnit ,
# MAGIC T.PurchDocNetPrice ,
# MAGIC T.NetOrdValuePOCurr ,
# MAGIC T.GRProcessTimeInDays ,
# MAGIC T.TaxSalPurchCode ,
# MAGIC T.IndicatUpdateInfoRecord ,
# MAGIC T.ValuationType ,
# MAGIC T.ValuationCategory ,
# MAGIC T.DeliCompleteIndicator ,
# MAGIC T.PurchDocItemCategory ,
# MAGIC T.GoodsReceiptIndicator ,
# MAGIC T.PrincipalPurchaseAgreement ,
# MAGIC T.PrincipalPurchaseAgreementItem ,
# MAGIC T.BaseUnitOfMeasure ,
# MAGIC T.DateOfPriceDetermination ,
# MAGIC T.PurchasingDocumentCategory ,
# MAGIC T.ItemAffectsCommitments ,
# MAGIC T.Customer ,
# MAGIC T.UpdateGroupForStatisticsUpdate ,
# MAGIC T.PlannedDeliveryInDays ,
# MAGIC T.NetWeight ,
# MAGIC T.UnitOfWeight ,
# MAGIC T.TaxJurisdiction ,
# MAGIC T.ControlKeyQualityMgmtInProcurement ,
# MAGIC T.ConfirmationControlKey ,
# MAGIC T.RevisionLevel ,
# MAGIC T.ProfitCenter ,
# MAGIC T.GrossWeight ,
# MAGIC T.Volume ,
# MAGIC T.IncotermsPart1 ,
# MAGIC T.IncotermsPart2 ,
# MAGIC T.MaterialCategory ,
# MAGIC T.Time ,
# MAGIC T.SequentialNumber ,
# MAGIC T.PurchaseRequisitionNumber ,
# MAGIC T.PurchaseRequisitionItemNumber ,
# MAGIC T.MaterialType ,
# MAGIC T.DeliveryTypeForReturnsToSupplier ,
# MAGIC T.RequisitionerOrRequester ,
# MAGIC T.TimeZoneRecipientLocation ,
# MAGIC T.MRPArea ,
# MAGIC T.CostCenter ,
# MAGIC T.GLAccountNumber ,
# MAGIC T.PurchaseDocCreateTime ,
# MAGIC T.IncotermsLocation1 ,
# MAGIC T.PPV ,
# MAGIC T.ReasonCode ,
# MAGIC T.HotQuantity ,
# MAGIC T.PQV ,
# MAGIC T.FromPOLineItem ,
# MAGIC T.OriginalPOLineItemQuantity ,
# MAGIC T.SalesDocumentItem ,
# MAGIC T.VisualCustomerOrderItem ,
# MAGIC T.SalesDocumentItems ,
# MAGIC T.SerialNumberProfile ,
# MAGIC T.PurchasingDocType ,
# MAGIC T.PurchaseDocTypeControlIndicator ,
# MAGIC T.PurchasingDocumentStatus ,
# MAGIC T.ObjectCreator ,
# MAGIC T.ChangeTimeStamp ,
# MAGIC T.ItemNumberInterval ,
# MAGIC T.LastItemNumber ,
# MAGIC T.VendorNumber ,
# MAGIC T.LanguageKey ,
# MAGIC T.TermsOfPaymentKey ,
# MAGIC T.CashDiscountPercentage1 ,
# MAGIC T.CashDiscountPercentage2 ,
# MAGIC T.PurchasingOrganization ,
# MAGIC T.PurchasingGroup ,
# MAGIC T.CurrencyKey ,
# MAGIC T.ExchangeRate ,
# MAGIC T.PurchasingDocumentDate ,
# MAGIC T.TargetValueHeaderAreaPerDistribution ,
# MAGIC T.DocumentConditionNumber ,
# MAGIC T.ProcedurePricing ,
# MAGIC T.ItemNumberIntervalSubitems ,
# MAGIC T.CountryTaxReturn ,
# MAGIC T.CountrySalesTaxIDNumber ,
# MAGIC T.ReasonForCancellation ,
# MAGIC T.PurchasingDocumentProcessingState ,
# MAGIC T.TotalValueTimeOfRelease ,
# MAGIC T.RetentionInPercent ,
# MAGIC T.DownPaymentPercentage ,
# MAGIC T.DownPaymentAmountDocCurrency ,
# MAGIC T.ShippingConditions ,
# MAGIC T.ForeignTradeStatisticalValuesCurrency ,
# MAGIC T.TimestampRevisionExternalCalls ,
# MAGIC T.InternalCounter ,
# MAGIC T.ItemGroup ,
# MAGIC T.LastVASItemNumber ,
# MAGIC T.AnnexingPackageKey ,
# MAGIC T.ExtendedKeyAnnexingPackage ,
# MAGIC T.DeviationPercentage ,
# MAGIC T.ProcedureNumber ,
# MAGIC T.PTFMContract ,
# MAGIC T.BudgetUniqueNumber ,
# MAGIC T.RequiredBudget ,
# MAGIC T.OTBReservedBudget ,
# MAGIC T.SpecialReleaseBudget ,
# MAGIC T.PurchDocDelInd ,
# MAGIC T.ReturnsItem,
# MAGIC T.DesiredRecvDate ,
# MAGIC T.TotalAmtOrdered ,
# MAGIC T.TotalAmtRecvd ,
# MAGIC T.VendorPartID ,
# MAGIC T.UserOrderQty ,
# MAGIC T.LineStatus ,
# MAGIC T.TotalUserRecvdQty ,
# MAGIC T.TotalRecvdQty ,
# MAGIC T.MfgPartID ,
# MAGIC T.VATCode ,
# MAGIC T.VATAmount ,
# MAGIC T.VATRecvdAmount ,
# MAGIC T.OrigStateRevisionID ,
# MAGIC T.ShipToID ,
# MAGIC T.ShipFromID ,
# MAGIC T.ShipVia ,
# MAGIC T.SellRate ,
# MAGIC T.BuyRate ,
# MAGIC T.TermsNetType ,
# MAGIC T.TermsNetDays ,
# MAGIC T.TermsDiscType ,
# MAGIC T.TermsDiscDays ,
# MAGIC T.TermsDiscPercent ,
# MAGIC T.TermsDescription ,
# MAGIC now() ,
# MAGIC T.DataSource 
# MAGIC )

# COMMAND ----------

if(ts_ekko != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EKKO' and DatabaseName = 'S42'".format(ts_ekko))

if(ts_ekpo != None):    
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EKPO' and DatabaseName = 'S42'".format(ts_ekpo))
    
if(ts_ekkn != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EKKN' and DatabaseName = 'S42'".format(ts_ekkn))

if(ts_ve70_purchase_order != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PURCHASE_ORDER' and DatabaseName = 'VE70'".format(ts_ve70_purchase_order))

if(ts_ve72_purchase_order != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PURCHASE_ORDER' and DatabaseName = 'VE72'".format(ts_ve72_purchase_order))

if(ts_ve70_purc_order_line != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PURC_ORDER_LINE' and DatabaseName = 'VE70'".format(ts_ve70_purc_order_line))

if(ts_ve72_purc_order_line != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PURC_ORDER_LINE' and DatabaseName = 'VE72'".format(ts_ve72_purc_order_line))


# COMMAND ----------

df_sf = spark.sql("select * from FEDW.PO_Detail where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'PO_Detail' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_po_details = ts_sf["max(UpdatedOn)"]
print(ts_po_details)

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
  .option("dbtable", "PO_Detail") \
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
# MAGIC 
# MAGIC 
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC  
# MAGIC Utils.runQuery(options, """call sp_load_po_detail()""")

# COMMAND ----------

if(ts_po_details != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'PO_Detail' and DatabaseName = 'FEDW'".format(ts_po_details))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from fedw.po_detail where datasource = 'SAP';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select PurchDocNumber,PurchDocItem,GLAccountnumber,costcenter
# MAGIC  from fedw.po_Detail where
# MAGIC PurchDocNumber in
# MAGIC (
# MAGIC '4500000011',
# MAGIC '4500000033'
# MAGIC )
# MAGIC and PurchDocItem in ('10');

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from s42.ekkn
# MAGIC ebeln in
# MAGIC (
# MAGIC '4500000011',
# MAGIC '4500000033'
# MAGIC )
# MAGIC and ebelp in ('10');

# COMMAND ----------

--650001, 0040003201

630504, 0010003201
