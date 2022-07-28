# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'PO_Receipts'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Fact/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_ekbe = [
"EBELN",
"EBELP",
"ZEKKN",
"VGABE",
"GJAHR",
"BELNR",
"BUZEI",
"BEWTP",
"BWART",
"BUDAT",
"MENGE",
"BPMNG",
"DMBTR",
"WRBTR",
"WAERS",
"AREWR",
"WESBS",
"BPWES",
"SHKZG",
"ELIKZ",
"LFGJA",
"LFBNR",
"LFPOS",
"GRUND",
"CPUDT",
"CPUTM",
"REEWR",
"REFWR",
"WERKS",
"ETENS",
"LSMNG",
"AREWW",
"HSWAE",
"BAMNG",
"BLDAT",
"ERNAM",
"PACKNO",
"INTROW",
"BEKKN",
"AREWB",
"REWRB",
"SAPRL",
"MENGE_POP",
"BPMNG_POP",
"DMBTR_POP",
"WRBTR_POP",
"WESBB",
"BPWEB",
"AREWR_POP",
"KUDIF",
"RETAMT_FC",
"RETAMT_LC",
"RETAMTP_FC",
"RETAMTP_LC",
"WKURS",
"VBELP_ST",
"/CWM/BAMNG",
"/CWM/WESBS",
"/CWM/WESBB",
"QTY_DIFF"
]

# COMMAND ----------

col_names_receiver_line = [
"RECEIVER_ID",
"LINE_NO",
"PURC_ORDER_ID",
"PURC_ORDER_LINE_NO",
"USER_RECEIVED_QTY",
"RECEIVED_QTY",
"INSPECT_QTY",
"ACT_FREIGHT",
"INVOICE_ID",
"INVOICED_DATE",
"WAREHOUSE_ID",
"LOCATION_ID",
"TRANSACTION_ID",
"SERV_TRANS_ID",
"PIECE_COUNT",
"LENGTH",
"WIDTH",
"HEIGHT",
"DIMENSIONS_UM",
"REJECTED_QTY",
"HTS_CODE",
"ORIG_COUNTRY_ID",
"UNIT_PRICE",
"LANDED_UNIT_PRICE",
"FIXED_CHARGE",
"INTRASTAT_AMOUNT",
"DATA_COLL_COMP",
"GROSS_WEIGHT"
]

# COMMAND ----------

col_names_receiver = [
    "ID",
"PURC_ORDER_ID",
"RECEIVED_DATE",
"CREATE_DATE",
"USER_ID",
"MARKED_FOR_PURGE",
"SHIP_REASON_CD",
"CARRIER_ID",
"BOL_ID",
"UCT_ASSOCIATED_SH"
]

# COMMAND ----------

df = spark.sql("select * from S42.EKBE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'EKBE' and DatabaseName = 'S42')")
df_ekbe = df.select(col_names_ekbe)
df_ekbe.createOrReplaceTempView("ekbe_tmp")
df_ts_ekbe = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ekbe = df_ts_ekbe["max(UpdatedOn)"]
print(ts_ekbe)

# COMMAND ----------

df = spark.sql("select * from VE70.RECEIVER_LINE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'RECEIVER_LINE' and DatabaseName = 'VE70')")
df_ve70_receiver_line = df.select(col_names_receiver_line).withColumn('DataSource',lit('VE70'))
df_ve70_receiver_line.createOrReplaceTempView("ve70_receiver_line_tmp")
df_ts_ve70_receiver_line = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_receiver_line = df_ts_ve70_receiver_line["max(UpdatedOn)"]
print(ts_ve70_receiver_line)

# COMMAND ----------

df = spark.sql("select * from VE72.RECEIVER_LINE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'RECEIVER_LINE' and DatabaseName = 'VE72')")
df_ve72_receiver_line = df.select(col_names_receiver_line).withColumn('DataSource',lit('VE72'))
df_ve72_receiver_line.createOrReplaceTempView("ve72_receiver_line_tmp")
df_ts_ve72_receiver_line = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_receiver_line = df_ts_ve72_receiver_line["max(UpdatedOn)"]
print(ts_ve72_receiver_line)

# COMMAND ----------

df = spark.sql("select * from VE70.RECEIVER where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'RECEIVER' and DatabaseName = 'VE70')")
df_ve70_receiver = df.select(col_names_receiver).withColumn('DataSource',lit('VE70'))
df_ve70_receiver.createOrReplaceTempView("ve70_receiver_tmp")
df_ts_ve70_receiver = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_receiver = df_ts_ve70_receiver["max(UpdatedOn)"]
print(ts_ve70_receiver)

# COMMAND ----------

df = spark.sql("select * from VE72.RECEIVER where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'RECEIVER' and DatabaseName = 'VE72')")
df_ve72_receiver = df.select(col_names_receiver).withColumn('DataSource',lit('VE72'))
df_ve72_receiver.createOrReplaceTempView("ve72_receiver_tmp")
df_ts_ve72_receiver = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_receiver = df_ts_ve72_receiver["max(UpdatedOn)"]
print(ts_ve72_receiver)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC create or replace temp view ve70_receiver_line
# MAGIC as
# MAGIC select 
# MAGIC rl.RECEIVER_ID,
# MAGIC rl.LINE_NO,
# MAGIC rl.PURC_ORDER_ID,
# MAGIC rl.PURC_ORDER_LINE_NO,
# MAGIC rl.USER_RECEIVED_QTY,
# MAGIC rl.RECEIVED_QTY,
# MAGIC rl.INSPECT_QTY,
# MAGIC rl.ACT_FREIGHT,
# MAGIC rl.INVOICE_ID,
# MAGIC rl.INVOICED_DATE,
# MAGIC rl.WAREHOUSE_ID,
# MAGIC rl.LOCATION_ID,
# MAGIC rl.TRANSACTION_ID,
# MAGIC rl.SERV_TRANS_ID,
# MAGIC rl.PIECE_COUNT,
# MAGIC rl.LENGTH,
# MAGIC rl.WIDTH,
# MAGIC rl.HEIGHT,
# MAGIC rl.DIMENSIONS_UM,
# MAGIC rl.REJECTED_QTY,
# MAGIC rl.HTS_CODE,
# MAGIC rl.ORIG_COUNTRY_ID,
# MAGIC rl.UNIT_PRICE,
# MAGIC rl.LANDED_UNIT_PRICE,
# MAGIC rl.FIXED_CHARGE,
# MAGIC rl.INTRASTAT_AMOUNT,
# MAGIC rl.DATA_COLL_COMP,
# MAGIC rl.GROSS_WEIGHT,
# MAGIC r.RECEIVED_DATE,
# MAGIC r.CREATE_DATE,
# MAGIC r.USER_ID,
# MAGIC r.MARKED_FOR_PURGE,
# MAGIC r.SHIP_REASON_CD,
# MAGIC r.CARRIER_ID,
# MAGIC r.BOL_ID,
# MAGIC r.UCT_ASSOCIATED_SH,
# MAGIC rl.DataSource
# MAGIC from ve70_receiver_line_tmp as rl
# MAGIC left join ve70_receiver_tmp as r 
# MAGIC on 
# MAGIC rl.RECEIVER_ID = r.ID

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC create or replace temp view ve72_receiver_line
# MAGIC as
# MAGIC select 
# MAGIC rl.RECEIVER_ID,
# MAGIC rl.LINE_NO,
# MAGIC rl.PURC_ORDER_ID,
# MAGIC rl.PURC_ORDER_LINE_NO,
# MAGIC rl.USER_RECEIVED_QTY,
# MAGIC rl.RECEIVED_QTY,
# MAGIC rl.INSPECT_QTY,
# MAGIC rl.ACT_FREIGHT,
# MAGIC rl.INVOICE_ID,
# MAGIC rl.INVOICED_DATE,
# MAGIC rl.WAREHOUSE_ID,
# MAGIC rl.LOCATION_ID,
# MAGIC rl.TRANSACTION_ID,
# MAGIC rl.SERV_TRANS_ID,
# MAGIC rl.PIECE_COUNT,
# MAGIC rl.LENGTH,
# MAGIC rl.WIDTH,
# MAGIC rl.HEIGHT,
# MAGIC rl.DIMENSIONS_UM,
# MAGIC rl.REJECTED_QTY,
# MAGIC rl.HTS_CODE,
# MAGIC rl.ORIG_COUNTRY_ID,
# MAGIC rl.UNIT_PRICE,
# MAGIC rl.LANDED_UNIT_PRICE,
# MAGIC rl.FIXED_CHARGE,
# MAGIC rl.INTRASTAT_AMOUNT,
# MAGIC rl.DATA_COLL_COMP,
# MAGIC rl.GROSS_WEIGHT,
# MAGIC r.RECEIVED_DATE,
# MAGIC r.CREATE_DATE,
# MAGIC r.USER_ID,
# MAGIC r.MARKED_FOR_PURGE,
# MAGIC r.SHIP_REASON_CD,
# MAGIC r.CARRIER_ID,
# MAGIC r.BOL_ID,
# MAGIC r.UCT_ASSOCIATED_SH,
# MAGIC rl.DataSource
# MAGIC from ve72_receiver_line_tmp as rl
# MAGIC left join ve72_receiver_tmp as r 
# MAGIC on 
# MAGIC rl.RECEIVER_ID = r.ID

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view tmp_receiver_line
# MAGIC as
# MAGIC select * from ve70_receiver_line
# MAGIC union
# MAGIC select * from ve72_receiver_line

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view PO_Receipts as
# MAGIC select
# MAGIC case when S.EBELN IS NULL then V.PURC_ORDER_ID else S.EBELN end as PurchDocNumber,
# MAGIC case when S.EBELN IS NULL then V.PURC_ORDER_LINE_NO else S.EBELP end as PurchDocItem,
# MAGIC case when S.EBELN IS NULL then '' else S.ZEKKN end as SeqNumberAccountAssignmt,
# MAGIC case when S.EBELN IS NULL then '' else S.VGABE end as TransactionType,
# MAGIC case when S.EBELN IS NULL then YEAR(V.RECEIVED_DATE) else S.GJAHR end as MaterialDocumentYear,
# MAGIC case when S.EBELN IS NULL then V.RECEIVER_ID else S.BELNR end as NumberOfMaterialDocument,
# MAGIC case when S.EBELN IS NULL then V.LINE_NO else S.BUZEI end as MaterialDocumentItem,
# MAGIC S.BEWTP as PurchOrderHistoryCat,
# MAGIC S.BWART as MovementType,
# MAGIC case when S.EBELN IS NULL then V.RECEIVED_DATE else S.BUDAT end as PostingDate,
# MAGIC case when S.EBELN IS NULL then V.RECEIVED_QTY else S.MENGE end as Quantity,
# MAGIC S.BPMNG as QtyInPurchOrderPriceUnit,
# MAGIC S.DMBTR as LocalCurrencyAmount,
# MAGIC S.WRBTR as DocumentCurrencyAmount,
# MAGIC S.WAERS as CurrencyKey,
# MAGIC S.AREWR as GrAccountClearingValueInLocalCurrency,
# MAGIC S.WESBS as GoodsReceiptBlockedStockInOrderUnit,
# MAGIC S.BPWES as QuantityInGrBlockedStockInOrderPriceUnit,
# MAGIC S.SHKZG as DebitCreditIndicator,
# MAGIC S.ELIKZ as DeliveryCompletedIndicator,
# MAGIC S.LFGJA as FiscalYearOfAReferenceDocument,
# MAGIC S.LFBNR as DocNoOfAReferenceDocument,
# MAGIC S.LFPOS as ReferenceDocumentItem,
# MAGIC S.GRUND as ReasonForMovement,
# MAGIC S.CPUDT as DayOnWhichAccountingDocumentWasEntered,
# MAGIC S.CPUTM as TimeOfEntry,
# MAGIC S.REEWR as InvoiceValueEnteredInLocalCurrency,
# MAGIC S.REFWR as InvoiceValueInForeignCurrency,
# MAGIC S.WERKS as Plant,
# MAGIC S.ETENS as SequentialNumberOfSupplierConfirmation,
# MAGIC S.LSMNG as QuantityInUnitOfMeasureFromDeliveryNote,
# MAGIC S.AREWW as ClearingValueOnGrClearingAccountTransCurrency,
# MAGIC S.HSWAE as LocalCurrencyKey,
# MAGIC S.BAMNG as QuantityBAMNG,
# MAGIC case when S.EBELN IS NULL then V.CREATE_DATE else S.BLDAT end as DocumentDateInDocument,
# MAGIC S.ERNAM as NameOfPersonWhoCreatedTheObject,
# MAGIC S.PACKNO as PackageNumberOfService,
# MAGIC S.INTROW as LineNumberOfService,
# MAGIC S.BEKKN as NumberOfPoAccountAssignment,
# MAGIC S.AREWB as ClearingValueOnGrAccountInPoCurrency,
# MAGIC S.REWRB as InvoiceAmountInPoCurrency,
# MAGIC S.SAPRL as SapRelease,
# MAGIC S.MENGE_POP as QuantityMENGEPOP,
# MAGIC S.BPMNG_POP as QuantityInPurchaseOrderPriceUnit,
# MAGIC S.DMBTR_POP as AmountInLocalCurrency,
# MAGIC S.WRBTR_POP as AmountInDocumentCurrency,
# MAGIC S.WESBB as ValuatedGoodsReceiptBlockedStockInOrderUnit,
# MAGIC S.BPWEB as QtyInValuatedGrBlockedStockInOrderPriceUnit,
# MAGIC S.AREWR_POP as GrAccountClearingValueInCurrency,
# MAGIC S.KUDIF as ExchangeRateDifferenceAmount,
# MAGIC S.RETAMT_FC as RetentionAmountInDocumentCurrency,
# MAGIC S.RETAMT_LC as RetentionAmountInCompanyCodeCurrency,
# MAGIC S.RETAMTP_FC as PostedRetentionAmtInDocCurrency,
# MAGIC S.RETAMTP_LC as PostedSecurityRetentionAmtInCompCodeCurrency,
# MAGIC S.WKURS as ExchangeRate,
# MAGIC S.VBELP_ST as DeliveryItem,
# MAGIC S.`/CWM/BAMNG` as QuantityInParallelUnitOfMeasure,
# MAGIC S.`/CWM/WESBS` as GoodsReceiptBlockedStockInBaseParallelUnitOfMeasure,
# MAGIC S.`/CWM/WESBB` as GoodsReceiptBlockedStockInBasisOrParallelUom,
# MAGIC S.QTY_DIFF as QuantityQTYDIFF,
# MAGIC V.USER_RECEIVED_QTY as Userreceivedqty,
# MAGIC V.INSPECT_QTY as Inspectqty,
# MAGIC V.ACT_FREIGHT as Actfreight,
# MAGIC V.INVOICE_ID as Invoiceid,
# MAGIC V.INVOICED_DATE as Invoiceddate,
# MAGIC V.WAREHOUSE_ID as Warehouseid,
# MAGIC V.LOCATION_ID as Locationid,
# MAGIC V.TRANSACTION_ID as Transactionid,
# MAGIC V.SERV_TRANS_ID as Servtransid,
# MAGIC V.PIECE_COUNT as Piececount,
# MAGIC V.LENGTH as Length,
# MAGIC V.WIDTH as Width,
# MAGIC V.HEIGHT as Height,
# MAGIC V.DIMENSIONS_UM as Dimensionsum,
# MAGIC V.REJECTED_QTY as Rejectedqty,
# MAGIC V.HTS_CODE as Htscode,
# MAGIC V.ORIG_COUNTRY_ID as Origcountryid,
# MAGIC V.UNIT_PRICE as Unitprice,
# MAGIC V.LANDED_UNIT_PRICE as Landedunitprice,
# MAGIC V.FIXED_CHARGE as Fixedcharge,
# MAGIC V.INTRASTAT_AMOUNT as Intrastatamount,
# MAGIC V.DATA_COLL_COMP as Datacollcomp,
# MAGIC V.GROSS_WEIGHT as Grossweight,
# MAGIC V.CREATE_DATE as Createdate,
# MAGIC V.USER_ID as Userid,
# MAGIC V.MARKED_FOR_PURGE as Markedforpurge,
# MAGIC V.SHIP_REASON_CD as Shipreasoncd,
# MAGIC V.CARRIER_ID as Carrierid,
# MAGIC V.BOL_ID as Bolid,
# MAGIC V.UCT_ASSOCIATED_SH as Uctassociatedsh,
# MAGIC case when S.EBELN IS NULL then V.DataSource else 'SAP' END as DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from ekbe_tmp as S
# MAGIC full join
# MAGIC tmp_receiver_line as V
# MAGIC on
# MAGIC S.EBELN = V.PURC_ORDER_ID and 
# MAGIC S.EBELP = V.PURC_ORDER_LINE_NO and 
# MAGIC S.GJAHR = YEAR(V.RECEIVED_DATE) and
# MAGIC 'SAP' = V.DataSource and
# MAGIC S.BELNR = V.RECEIVER_ID and
# MAGIC S.BUZEI = V.LINE_NO 

# COMMAND ----------

df_po_receipts = spark.sql("select * from PO_Receipts")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_po_receipts.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_po_receipts.createOrReplaceTempView('tmp_PoRecipets')

# COMMAND ----------

# MAGIC %sql --merge logic
# MAGIC Merge into fedw.PO_Receipts as S
# MAGIC Using tmp_PoRecipets as T
# MAGIC on S.PurchDocNumber = T.PurchDocNumber and S.PurchDocItem = T.PurchDocItem and S.DataSource = T.DataSource
# MAGIC and S.SeqNumberAccountAssignmt = T.SeqNumberAccountAssignmt and S.TransactionType = T.TransactionType
# MAGIC and S.MaterialDocumentYear = T.MaterialDocumentYear and S.NumberOfMaterialDocument = T.NumberOfMaterialDocument
# MAGIC and S.MaterialDocumentItem = T.MaterialDocumentItem
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC S.PurchDocNumber = T.PurchDocNumber,
# MAGIC S.PurchDocItem = T.PurchDocItem,
# MAGIC S.SeqNumberAccountAssignmt = T.SeqNumberAccountAssignmt,
# MAGIC S.TransactionType = T.TransactionType,
# MAGIC S.MaterialDocumentYear = T.MaterialDocumentYear,
# MAGIC S.NumberOfMaterialDocument = T.NumberOfMaterialDocument,
# MAGIC S.MaterialDocumentItem = T.MaterialDocumentItem,
# MAGIC S.PurchOrderHistoryCat = T.PurchOrderHistoryCat,
# MAGIC S.MovementType = T.MovementType,
# MAGIC S.PostingDate = T.PostingDate,
# MAGIC S.Quantity = T.Quantity,
# MAGIC S.QtyInPurchOrderPriceUnit = T.QtyInPurchOrderPriceUnit,
# MAGIC S.LocalCurrencyAmount = T.LocalCurrencyAmount,
# MAGIC S.DocumentCurrencyAmount = T.DocumentCurrencyAmount,
# MAGIC S.CurrencyKey = T.CurrencyKey,
# MAGIC S.GrAccountClearingValueInLocalCurrency = T.GrAccountClearingValueInLocalCurrency,
# MAGIC S.GoodsReceiptBlockedStockInOrderUnit = T.GoodsReceiptBlockedStockInOrderUnit,
# MAGIC S.QuantityInGrBlockedStockInOrderPriceUnit = T.QuantityInGrBlockedStockInOrderPriceUnit,
# MAGIC S.DebitCreditIndicator = T.DebitCreditIndicator,
# MAGIC S.DeliveryCompletedIndicator = T.DeliveryCompletedIndicator,
# MAGIC S.FiscalYearOfAReferenceDocument = T.FiscalYearOfAReferenceDocument,
# MAGIC S.DocNoOfAReferenceDocument = T.DocNoOfAReferenceDocument,
# MAGIC S.ReferenceDocumentItem = T.ReferenceDocumentItem,
# MAGIC S.ReasonForMovement = T.ReasonForMovement,
# MAGIC S.DayOnWhichAccountingDocumentWasEntered = T.DayOnWhichAccountingDocumentWasEntered,
# MAGIC S.TimeOfEntry = T.TimeOfEntry,
# MAGIC S.InvoiceValueEnteredInLocalCurrency = T.InvoiceValueEnteredInLocalCurrency,
# MAGIC S.InvoiceValueInForeignCurrency = T.InvoiceValueInForeignCurrency,
# MAGIC S.Plant = T.Plant,
# MAGIC S.SequentialNumberOfSupplierConfirmation = T.SequentialNumberOfSupplierConfirmation,
# MAGIC S.QuantityInUnitOfMeasureFromDeliveryNote = T.QuantityInUnitOfMeasureFromDeliveryNote,
# MAGIC S.ClearingValueOnGrClearingAccountTransCurrency = T.ClearingValueOnGrClearingAccountTransCurrency,
# MAGIC S.LocalCurrencyKey = T.LocalCurrencyKey,
# MAGIC S.QuantityBAMNG = T.QuantityBAMNG,
# MAGIC S.DocumentDateInDocument = T.DocumentDateInDocument,
# MAGIC S.NameOfPersonWhoCreatedTheObject = T.NameOfPersonWhoCreatedTheObject,
# MAGIC S.PackageNumberOfService = T.PackageNumberOfService,
# MAGIC S.LineNumberOfService = T.LineNumberOfService,
# MAGIC S.NumberOfPoAccountAssignment = T.NumberOfPoAccountAssignment,
# MAGIC S.ClearingValueOnGrAccountInPoCurrency = T.ClearingValueOnGrAccountInPoCurrency,
# MAGIC S.InvoiceAmountInPoCurrency = T.InvoiceAmountInPoCurrency,
# MAGIC S.SapRelease = T.SapRelease,
# MAGIC S.QuantityMENGEPOP = T.QuantityMENGEPOP,
# MAGIC S.QuantityInPurchaseOrderPriceUnit = T.QuantityInPurchaseOrderPriceUnit,
# MAGIC S.AmountInLocalCurrency = T.AmountInLocalCurrency,
# MAGIC S.AmountInDocumentCurrency = T.AmountInDocumentCurrency,
# MAGIC S.ValuatedGoodsReceiptBlockedStockInOrderUnit = T.ValuatedGoodsReceiptBlockedStockInOrderUnit,
# MAGIC S.QtyInValuatedGrBlockedStockInOrderPriceUnit = T.QtyInValuatedGrBlockedStockInOrderPriceUnit,
# MAGIC S.GrAccountClearingValueInCurrency = T.GrAccountClearingValueInCurrency,
# MAGIC S.ExchangeRateDifferenceAmount = T.ExchangeRateDifferenceAmount,
# MAGIC S.RetentionAmountInDocumentCurrency = T.RetentionAmountInDocumentCurrency,
# MAGIC S.RetentionAmountInCompanyCodeCurrency = T.RetentionAmountInCompanyCodeCurrency,
# MAGIC S.PostedRetentionAmtInDocCurrency = T.PostedRetentionAmtInDocCurrency,
# MAGIC S.PostedSecurityRetentionAmtInCompCodeCurrency = T.PostedSecurityRetentionAmtInCompCodeCurrency,
# MAGIC S.ExchangeRate = T.ExchangeRate,
# MAGIC S.DeliveryItem = T.DeliveryItem,
# MAGIC S.QuantityInParallelUnitOfMeasure = T.QuantityInParallelUnitOfMeasure,
# MAGIC S.GoodsReceiptBlockedStockInBaseParallelUnitOfMeasure = T.GoodsReceiptBlockedStockInBaseParallelUnitOfMeasure,
# MAGIC S.GoodsReceiptBlockedStockInBasisOrParallelUom = T.GoodsReceiptBlockedStockInBasisOrParallelUom,
# MAGIC S.QuantityQTYDIFF = T.QuantityQTYDIFF,
# MAGIC S.Userreceivedqty = T.Userreceivedqty,
# MAGIC S.Inspectqty = T.Inspectqty,
# MAGIC S.Actfreight = T.Actfreight,
# MAGIC S.Invoiceid = T.Invoiceid,
# MAGIC S.Invoiceddate = T.Invoiceddate,
# MAGIC S.Warehouseid = T.Warehouseid,
# MAGIC S.Locationid = T.Locationid,
# MAGIC S.Transactionid = T.Transactionid,
# MAGIC S.Servtransid = T.Servtransid,
# MAGIC S.Piececount = T.Piececount,
# MAGIC S.Length = T.Length,
# MAGIC S.Width = T.Width,
# MAGIC S.Height = T.Height,
# MAGIC S.Dimensionsum = T.Dimensionsum,
# MAGIC S.Rejectedqty = T.Rejectedqty,
# MAGIC S.Htscode = T.Htscode,
# MAGIC S.Origcountryid = T.Origcountryid,
# MAGIC S.Unitprice = T.Unitprice,
# MAGIC S.Landedunitprice = T.Landedunitprice,
# MAGIC S.Fixedcharge = T.Fixedcharge,
# MAGIC S.Intrastatamount = T.Intrastatamount,
# MAGIC S.Datacollcomp = T.Datacollcomp,
# MAGIC S.Grossweight = T.Grossweight,
# MAGIC S.Createdate = T.Createdate,
# MAGIC S.Userid = T.Userid,
# MAGIC S.Markedforpurge = T.Markedforpurge,
# MAGIC S.Shipreasoncd = T.Shipreasoncd,
# MAGIC S.Carrierid = T.Carrierid,
# MAGIC S.Bolid = T.Bolid,
# MAGIC S.Uctassociatedsh = T.Uctassociatedsh,
# MAGIC S.DataSource = T.DataSource,
# MAGIC S.UpdatedOn = now()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT
# MAGIC (
# MAGIC PurchDocNumber,
# MAGIC PurchDocItem,
# MAGIC SeqNumberAccountAssignmt,
# MAGIC TransactionType,
# MAGIC MaterialDocumentYear,
# MAGIC NumberOfMaterialDocument,
# MAGIC MaterialDocumentItem,
# MAGIC PurchOrderHistoryCat,
# MAGIC MovementType,
# MAGIC PostingDate,
# MAGIC Quantity,
# MAGIC QtyInPurchOrderPriceUnit,
# MAGIC LocalCurrencyAmount,
# MAGIC DocumentCurrencyAmount,
# MAGIC CurrencyKey,
# MAGIC GrAccountClearingValueInLocalCurrency,
# MAGIC GoodsReceiptBlockedStockInOrderUnit,
# MAGIC QuantityInGrBlockedStockInOrderPriceUnit,
# MAGIC DebitCreditIndicator,
# MAGIC DeliveryCompletedIndicator,
# MAGIC FiscalYearOfAReferenceDocument,
# MAGIC DocNoOfAReferenceDocument,
# MAGIC ReferenceDocumentItem,
# MAGIC ReasonForMovement,
# MAGIC DayOnWhichAccountingDocumentWasEntered,
# MAGIC TimeOfEntry,
# MAGIC InvoiceValueEnteredInLocalCurrency,
# MAGIC InvoiceValueInForeignCurrency,
# MAGIC Plant,
# MAGIC SequentialNumberOfSupplierConfirmation,
# MAGIC QuantityInUnitOfMeasureFromDeliveryNote,
# MAGIC ClearingValueOnGrClearingAccountTransCurrency,
# MAGIC LocalCurrencyKey,
# MAGIC QuantityBAMNG,
# MAGIC DocumentDateInDocument,
# MAGIC NameOfPersonWhoCreatedTheObject,
# MAGIC PackageNumberOfService,
# MAGIC LineNumberOfService,
# MAGIC NumberOfPoAccountAssignment,
# MAGIC ClearingValueOnGrAccountInPoCurrency,
# MAGIC InvoiceAmountInPoCurrency,
# MAGIC SapRelease,
# MAGIC QuantityMENGEPOP,
# MAGIC QuantityInPurchaseOrderPriceUnit,
# MAGIC AmountInLocalCurrency,
# MAGIC AmountInDocumentCurrency,
# MAGIC ValuatedGoodsReceiptBlockedStockInOrderUnit,
# MAGIC QtyInValuatedGrBlockedStockInOrderPriceUnit,
# MAGIC GrAccountClearingValueInCurrency,
# MAGIC ExchangeRateDifferenceAmount,
# MAGIC RetentionAmountInDocumentCurrency,
# MAGIC RetentionAmountInCompanyCodeCurrency,
# MAGIC PostedRetentionAmtInDocCurrency,
# MAGIC PostedSecurityRetentionAmtInCompCodeCurrency,
# MAGIC ExchangeRate,
# MAGIC DeliveryItem,
# MAGIC QuantityInParallelUnitOfMeasure,
# MAGIC GoodsReceiptBlockedStockInBaseParallelUnitOfMeasure,
# MAGIC GoodsReceiptBlockedStockInBasisOrParallelUom,
# MAGIC QuantityQTYDIFF,
# MAGIC Userreceivedqty,
# MAGIC Inspectqty,
# MAGIC Actfreight,
# MAGIC Invoiceid,
# MAGIC Invoiceddate,
# MAGIC Warehouseid,
# MAGIC Locationid,
# MAGIC Transactionid,
# MAGIC Servtransid,
# MAGIC Piececount,
# MAGIC Length,
# MAGIC Width,
# MAGIC Height,
# MAGIC Dimensionsum,
# MAGIC Rejectedqty,
# MAGIC Htscode,
# MAGIC Origcountryid,
# MAGIC Unitprice,
# MAGIC Landedunitprice,
# MAGIC Fixedcharge,
# MAGIC Intrastatamount,
# MAGIC Datacollcomp,
# MAGIC Grossweight,
# MAGIC Createdate,
# MAGIC Userid,
# MAGIC Markedforpurge,
# MAGIC Shipreasoncd,
# MAGIC Carrierid,
# MAGIC Bolid,
# MAGIC Uctassociatedsh,
# MAGIC DataSource,
# MAGIC UpdatedOn
# MAGIC )
# MAGIC Values
# MAGIC (
# MAGIC T.PurchDocNumber ,
# MAGIC T.PurchDocItem ,
# MAGIC T.SeqNumberAccountAssignmt ,
# MAGIC T.TransactionType ,
# MAGIC T.MaterialDocumentYear ,
# MAGIC T.NumberOfMaterialDocument ,
# MAGIC T.MaterialDocumentItem ,
# MAGIC T.PurchOrderHistoryCat ,
# MAGIC T.MovementType ,
# MAGIC T.PostingDate ,
# MAGIC T.Quantity ,
# MAGIC T.QtyInPurchOrderPriceUnit ,
# MAGIC T.LocalCurrencyAmount ,
# MAGIC T.DocumentCurrencyAmount ,
# MAGIC T.CurrencyKey ,
# MAGIC T.GrAccountClearingValueInLocalCurrency ,
# MAGIC T.GoodsReceiptBlockedStockInOrderUnit ,
# MAGIC T.QuantityInGrBlockedStockInOrderPriceUnit ,
# MAGIC T.DebitCreditIndicator ,
# MAGIC T.DeliveryCompletedIndicator ,
# MAGIC T.FiscalYearOfAReferenceDocument ,
# MAGIC T.DocNoOfAReferenceDocument ,
# MAGIC T.ReferenceDocumentItem ,
# MAGIC T.ReasonForMovement ,
# MAGIC T.DayOnWhichAccountingDocumentWasEntered ,
# MAGIC T.TimeOfEntry ,
# MAGIC T.InvoiceValueEnteredInLocalCurrency ,
# MAGIC T.InvoiceValueInForeignCurrency ,
# MAGIC T.Plant ,
# MAGIC T.SequentialNumberOfSupplierConfirmation ,
# MAGIC T.QuantityInUnitOfMeasureFromDeliveryNote ,
# MAGIC T.ClearingValueOnGrClearingAccountTransCurrency ,
# MAGIC T.LocalCurrencyKey ,
# MAGIC T.QuantityBAMNG ,
# MAGIC T.DocumentDateInDocument ,
# MAGIC T.NameOfPersonWhoCreatedTheObject ,
# MAGIC T.PackageNumberOfService ,
# MAGIC T.LineNumberOfService ,
# MAGIC T.NumberOfPoAccountAssignment ,
# MAGIC T.ClearingValueOnGrAccountInPoCurrency ,
# MAGIC T.InvoiceAmountInPoCurrency ,
# MAGIC T.SapRelease ,
# MAGIC T.QuantityMENGEPOP ,
# MAGIC T.QuantityInPurchaseOrderPriceUnit ,
# MAGIC T.AmountInLocalCurrency ,
# MAGIC T.AmountInDocumentCurrency ,
# MAGIC T.ValuatedGoodsReceiptBlockedStockInOrderUnit ,
# MAGIC T.QtyInValuatedGrBlockedStockInOrderPriceUnit ,
# MAGIC T.GrAccountClearingValueInCurrency ,
# MAGIC T.ExchangeRateDifferenceAmount ,
# MAGIC T.RetentionAmountInDocumentCurrency ,
# MAGIC T.RetentionAmountInCompanyCodeCurrency ,
# MAGIC T.PostedRetentionAmtInDocCurrency ,
# MAGIC T.PostedSecurityRetentionAmtInCompCodeCurrency ,
# MAGIC T.ExchangeRate ,
# MAGIC T.DeliveryItem ,
# MAGIC T.QuantityInParallelUnitOfMeasure ,
# MAGIC T.GoodsReceiptBlockedStockInBaseParallelUnitOfMeasure ,
# MAGIC T.GoodsReceiptBlockedStockInBasisOrParallelUom ,
# MAGIC T.QuantityQTYDIFF ,
# MAGIC T.Userreceivedqty ,
# MAGIC T.Inspectqty ,
# MAGIC T.Actfreight ,
# MAGIC T.Invoiceid ,
# MAGIC T.Invoiceddate ,
# MAGIC T.Warehouseid ,
# MAGIC T.Locationid ,
# MAGIC T.Transactionid ,
# MAGIC T.Servtransid ,
# MAGIC T.Piececount ,
# MAGIC T.Length ,
# MAGIC T.Width ,
# MAGIC T.Height ,
# MAGIC T.Dimensionsum ,
# MAGIC T.Rejectedqty ,
# MAGIC T.Htscode ,
# MAGIC T.Origcountryid ,
# MAGIC T.Unitprice ,
# MAGIC T.Landedunitprice ,
# MAGIC T.Fixedcharge ,
# MAGIC T.Intrastatamount ,
# MAGIC T.Datacollcomp ,
# MAGIC T.Grossweight ,
# MAGIC T.Createdate ,
# MAGIC T.Userid ,
# MAGIC T.Markedforpurge ,
# MAGIC T.Shipreasoncd ,
# MAGIC T.Carrierid ,
# MAGIC T.Bolid ,
# MAGIC T.Uctassociatedsh ,
# MAGIC T.DataSource ,
# MAGIC now()
# MAGIC )

# COMMAND ----------

if(ts_ekbe != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'EKBE' and DatabaseName = 'S42'".format(ts_ekbe))

if(ts_ve70_receiver_line != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'RECEIVER_LINE' and DatabaseName = 'VE70'".format(ts_ve70_receiver_line))

if(ts_ve72_receiver_line != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'RECEIVER_LINE' and DatabaseName = 'VE72'".format(ts_ve72_receiver_line))

if(ts_ve70_receiver != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'RECEIVER' and DatabaseName = 'VE70'".format(ts_ve70_receiver))

if(ts_ve72_receiver != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'RECEIVER' and DatabaseName = 'VE72'".format(ts_ve72_receiver))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.PO_Receipts where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'PO_Receipts' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_po_receipts = ts_sf["max(UpdatedOn)"]
print(ts_po_receipts)

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
  .option("dbtable", "PO_Receipts") \
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
# MAGIC Utils.runQuery(options, """call sp_load_poreciepts()""")

# COMMAND ----------

if(ts_po_receipts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'PO_Receipts' and DatabaseName = 'FEDW'".format(ts_po_receipts))

# COMMAND ----------


