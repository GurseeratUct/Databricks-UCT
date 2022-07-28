# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Part_Cost_Detail'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Fact/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_ckis =[
    "LEDNR",
"BZOBJ",
"KALNR",
"KALKA",
"KADKY",
"TVERS",
"BWVAR",
"POSNR",
"KKZMA",
"TYPPS",
"KSTAR",
"ELEMT",
"ELEMTNS",
"KOKRS_HRK",
"GPREIS",
"FPREIS",
"PEINH",
"PMEHT",
"WERTB",
"WERTN",
"WRTFX",
"WRTFW_KPF",
"WRTFW_KFX",
"FWAER_KPF",
"WRTFW_POS",
"WRTFW_PFX",
"FWAER",
"MKURS",
"FWEHT",
"MENGE",
"MEEHT",
"SUMM1",
"SUMM2",
"SUMM3",
"DPREIS",
"PREIS1",
"PREIS2",
"PREIS3",
"PREIS4",
"PREIS5",
"ZUABS",
"ZUFKT",
"ARBID",
"ESOKZ",
"EBELP",
"STPOS",
"UMREZ",
"UMREN",
"AUSMG",
"AUSMGKO",
"AUSPROZ",
"VORNR",
"STEAS",
"POSNR_EXT",
"POINTER1",
"POINTER2",
"POINTER3",
"OPREIS",
"OPREIFX",
"TPREIS",
"TPREIFX",
"PATNR",
"VERWS",
"STRAT",
"TKURS",
"SELKZ",
"SSEDD",
"UKALN",
"UKALKA",
"UKADKY",
"UTVERS",
"UBWVAR",
"HKMAT",
"SPOSN",
"USTRAT",
"PEINH_2",
"PEINH_3",
"/CWM/MENGE_BEW",
"/CWM/AUSMG_BEW",
"/CWM/AUSMGKO_BEW"
]

# COMMAND ----------

col_names_keko =[
 "BZOBJ",
"KALNR",
"KALKA",
"KADKY",
"TVERS",
"BWVAR",
"POSNR",
"WERKS",
"MATNR",
"BWTAR",
"PRCTR",
"PSPNR",
"DISST",
"CUOBJ",
"BWKEY",
"KOKRS",
"KADAT",
"BIDAT",
"BWDAT",
"ALDAT",
"STCNT",
"PLNCT",
"LOSGR",
"MEINS",
"ERFNM",
"CPUDT",
"CPUTIME",
"FEH_STA",
"FREIG",
"PLSCN",
"PLMNG",
"KALSM",
"KLVAR",
"KOSGR",
"ZSCHL",
"POPER",
"BDATJ",
"STKOZ",
"ZAEHL",
"CMF_NR",
"OCS_COUNT",
"ERZKA",
"LOSAU",
"AUSSS",
"SAPRL",
"KZROH",
"AUFPL",
"VORMDAT",
"ZIFFR",
"MLMAA",
"BESKZ",
"KALST",
"PART_VRSN",
"VOCNT",
"KURST",
"HWAER",
"REFID",
"KALAID",
"KALADAT",
"TVERS_SENDER",
"/CWM/LOSGR_BAS",
"/CWM/LOSGR_BEW",
"PACKNO",
"INTROW",
"BOSDVERSION",
"CLINT"
]

# COMMAND ----------

col_names_uct_part_cost_change = [
"DATE_POSTED",
"PART_ID",
"N_UNIT_MATERIAL_COST",
"N_UNIT_BURDEN_COST",
"N_UNIT_LABOR_COST",
"N_UNIT_SERVICE_COST",
"O_UNIT_BURDEN_COST",
"O_UNIT_LABOR_COST",
"O_UNIT_MATERIAL_COST",
"O_UNIT_SERVICE_COST",
"PRODUCT_CODE",
"QTY_ON_HAND"
]

# COMMAND ----------

df = spark.sql("select * from S42.KEKO where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'KEKO' and DatabaseName = 'S42')")
df_keko = df.select(col_names_keko)
df_keko.createOrReplaceTempView("keko_tmp")
df_ts_keko = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_keko = df_ts_keko["max(UpdatedOn)"]
print(ts_keko)

# COMMAND ----------

df = spark.sql("select * from S42.CKIS where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'CKIS' and DatabaseName = 'S42')")
df_ckis = df.select(col_names_ckis)
df_ckis.createOrReplaceTempView("ckis_tmp")
df_ts_ckis = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ckis = df_ts_ckis["max(UpdatedOn)"]
print(ts_ckis)

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

df = spark.sql("select * from VE70.UCT_PART_COST_CHANGE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_PART_COST_CHANGE' and DatabaseName = 'VE70')")
df_ve70_uct_part_cost_change = df.select(col_names_uct_part_cost_change) \
.withColumn('CostingType',lit('01')) \
.withColumn('ValuationVariantInCosting',lit(1)) \
.withColumn('UnitCostingLineItemNumber',lit(1)) \
.withColumn('PriceUnitOfPricesInCOCurrency',lit(1)) \
.withColumn('Plant',lit(plant_code_shangai)) \
.withColumn('DataSource',lit('VE70')) \
.withColumn('Quantity',lit(1)) \
.withColumn('ValuationArea',lit(plant_code_shangai)) \
.withColumn('CostingLotSize',lit(1)) \
.withColumn('ValuationUOM',lit('EA'))
df_ve70_uct_part_cost_change.createOrReplaceTempView("ve70_uct_part_cost_change_tmp")
df_ts_ve70_uct_part_cost_change = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_uct_part_cost_change = df_ts_ve70_uct_part_cost_change["max(UpdatedOn)"]
print(ts_ve70_uct_part_cost_change)

# COMMAND ----------

df = spark.sql("select * from VE72.UCT_PART_COST_CHANGE where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_PART_COST_CHANGE' and DatabaseName = 'VE72')")
df_ve72_uct_part_cost_change = df.select(col_names_uct_part_cost_change) \
.withColumn('CostingType',lit('01')) \
.withColumn('ValuationVariantInCosting',lit(1)) \
.withColumn('UnitCostingLineItemNumber',lit(1)) \
.withColumn('PriceUnitOfPricesInCOCurrency',lit(1)) \
.withColumn('Plant',lit(plant_code_singapore)) \
.withColumn('DataSource',lit('VE72')) \
.withColumn('Quantity',lit(1)) \
.withColumn('ValuationArea',lit(plant_code_singapore)) \
.withColumn('CostingLotSize',lit(1)) \
.withColumn('ValuationUOM',lit('EA'))
df_ve72_uct_part_cost_change.createOrReplaceTempView("ve72_uct_part_cost_change_tmp")
df_ts_ve72_uct_part_cost_change = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_uct_part_cost_change = df_ts_ve72_uct_part_cost_change["max(UpdatedOn)"]
print(ts_ve72_uct_part_cost_change)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view tmp_uct_part_cost_change
# MAGIC as
# MAGIC select * from ve70_uct_part_cost_change_tmp
# MAGIC union 
# MAGIC select * from ve72_uct_part_cost_change_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view tmp_ckis_keko
# MAGIC as
# MAGIC select 
# MAGIC ck.LEDNR,
# MAGIC ck.BZOBJ,
# MAGIC ck.KALNR,
# MAGIC ck.KALKA,
# MAGIC ck.KADKY,
# MAGIC ck.TVERS,
# MAGIC ck.BWVAR,
# MAGIC ck.POSNR,
# MAGIC ck.KKZMA,
# MAGIC ck.TYPPS,
# MAGIC ck.KSTAR,
# MAGIC ck.ELEMT,
# MAGIC ck.ELEMTNS,
# MAGIC ck.KOKRS_HRK,
# MAGIC ck.GPREIS,
# MAGIC ck.FPREIS,
# MAGIC ck.PEINH,
# MAGIC ck.PMEHT,
# MAGIC ck.WERTB,
# MAGIC ck.WERTN,
# MAGIC ck.WRTFX,
# MAGIC ck.WRTFW_KPF,
# MAGIC ck.WRTFW_KFX,
# MAGIC ck.FWAER_KPF,
# MAGIC ck.WRTFW_POS,
# MAGIC ck.WRTFW_PFX,
# MAGIC ck.FWAER,
# MAGIC ck.MKURS,
# MAGIC ck.FWEHT,
# MAGIC ck.MENGE,
# MAGIC ck.MEEHT,
# MAGIC ck.SUMM1,
# MAGIC ck.SUMM2,
# MAGIC ck.SUMM3,
# MAGIC ck.DPREIS,
# MAGIC ck.PREIS1,
# MAGIC ck.PREIS2,
# MAGIC ck.PREIS3,
# MAGIC ck.PREIS4,
# MAGIC ck.PREIS5,
# MAGIC ck.ZUABS,
# MAGIC ck.ZUFKT,
# MAGIC ck.ARBID,
# MAGIC ck.ESOKZ,
# MAGIC ck.EBELP,
# MAGIC ck.STPOS,
# MAGIC ck.UMREZ,
# MAGIC ck.UMREN,
# MAGIC ck.AUSMG,
# MAGIC ck.AUSMGKO,
# MAGIC ck.AUSPROZ,
# MAGIC ck.VORNR,
# MAGIC ck.STEAS,
# MAGIC ck.POSNR_EXT,
# MAGIC ck.POINTER1,
# MAGIC ck.POINTER2,
# MAGIC ck.POINTER3,
# MAGIC ck.OPREIS,
# MAGIC ck.OPREIFX,
# MAGIC ck.TPREIS,
# MAGIC ck.TPREIFX,
# MAGIC ck.PATNR,
# MAGIC ck.VERWS,
# MAGIC ck.STRAT,
# MAGIC ck.TKURS,
# MAGIC ck.SELKZ,
# MAGIC ck.SSEDD,
# MAGIC ck.UKALN,
# MAGIC ck.UKALKA,
# MAGIC ck.UKADKY,
# MAGIC ck.UTVERS,
# MAGIC ck.UBWVAR,
# MAGIC ck.HKMAT,
# MAGIC ck.SPOSN,
# MAGIC ck.USTRAT,
# MAGIC ck.PEINH_2,
# MAGIC ck.PEINH_3,
# MAGIC ck.`/CWM/MENGE_BEW`,
# MAGIC ck.`/CWM/AUSMG_BEW`,
# MAGIC ck.`/CWM/AUSMGKO_BEW`,
# MAGIC ke.WERKS,
# MAGIC ke.MATNR,
# MAGIC ke.BWTAR,
# MAGIC ke.PRCTR,
# MAGIC ke.PSPNR,
# MAGIC ke.DISST,
# MAGIC ke.CUOBJ,
# MAGIC ke.BWKEY,
# MAGIC ke.KOKRS,
# MAGIC ke.KADAT,
# MAGIC ke.BIDAT,
# MAGIC ke.BWDAT,
# MAGIC ke.ALDAT,
# MAGIC ke.STCNT,
# MAGIC ke.PLNCT,
# MAGIC ke.LOSGR,
# MAGIC ke.MEINS,
# MAGIC ke.ERFNM,
# MAGIC ke.CPUDT,
# MAGIC ke.CPUTIME,
# MAGIC ke.FEH_STA,
# MAGIC ke.FREIG,
# MAGIC ke.PLSCN,
# MAGIC ke.PLMNG,
# MAGIC ke.KALSM,
# MAGIC ke.KLVAR,
# MAGIC ke.KOSGR,
# MAGIC ke.ZSCHL,
# MAGIC ke.POPER,
# MAGIC ke.BDATJ,
# MAGIC ke.STKOZ,
# MAGIC ke.ZAEHL,
# MAGIC ke.CMF_NR,
# MAGIC ke.OCS_COUNT,
# MAGIC ke.ERZKA,
# MAGIC ke.LOSAU,
# MAGIC ke.AUSSS,
# MAGIC ke.SAPRL,
# MAGIC ke.KZROH,
# MAGIC ke.AUFPL,
# MAGIC ke.VORMDAT,
# MAGIC ke.ZIFFR,
# MAGIC ke.MLMAA,
# MAGIC ke.BESKZ,
# MAGIC ke.KALST,
# MAGIC ke.PART_VRSN,
# MAGIC ke.VOCNT,
# MAGIC ke.KURST,
# MAGIC ke.HWAER,
# MAGIC ke.REFID,
# MAGIC ke.KALAID,
# MAGIC ke.KALADAT,
# MAGIC ke.TVERS_SENDER,
# MAGIC ke.`/CWM/LOSGR_BAS`,
# MAGIC ke.`/CWM/LOSGR_BEW`,
# MAGIC ke.PACKNO,
# MAGIC ke.INTROW,
# MAGIC ke.BOSDVERSION,
# MAGIC ke.CLINT,
# MAGIC 'SAP' as DataSource
# MAGIC from ckis_tmp as ck
# MAGIC left join 
# MAGIC keko_tmp as ke
# MAGIC on 
# MAGIC ck.BZOBJ = ke.BZOBJ and
# MAGIC ck.KALNR = ke.KALNR and
# MAGIC ck.KALKA = ke.KALKA and
# MAGIC ck.KADKY = ke.KADKY and
# MAGIC ck.TVERS = ke.TVERS and
# MAGIC ck.BWVAR = ke.BWVAR 
# MAGIC --and ck.POSNR = ke.POSNR

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view part_cost_detail
# MAGIC as
# MAGIC select
# MAGIC S.LEDNR as LedgerForControllingObjects,
# MAGIC S.BZOBJ as ReferenceObject,
# MAGIC S.KALNR as CostEstimateNumber,
# MAGIC S.KALKA as CostingType,
# MAGIC S.KADKY as CostingDate,
# MAGIC S.TVERS as CostingVersion,
# MAGIC S.BWVAR as ValuationVariantInCosting,
# MAGIC S.POSNR as UnitCostingLineItemNumber,
# MAGIC S.KKZMA as AdditiveCostEst,
# MAGIC S.TYPPS as ItemCategory,
# MAGIC S.KSTAR as CostElement,
# MAGIC S.ELEMT as MainCostComponentSplit,
# MAGIC S.ELEMTNS as AuxiliaryCostComponentSplit,
# MAGIC S.KOKRS_HRK as OriginControllingAreaItem,
# MAGIC S.GPREIS as PriceInCOCurrency,
# MAGIC S.FPREIS as FixedPriceInCOCurrency,
# MAGIC S.PEINH as PriceUnitOfPricesInCOCurrency,
# MAGIC S.PMEHT as PriceQuantityUnit,
# MAGIC S.WERTB as ValueInCOCurrencyB,
# MAGIC S.WERTN as ValueInCOCurrencyN,
# MAGIC S.WRTFX as FixedValueInCOCurrency,
# MAGIC S.WRTFW_KPF as ValueInObjectCurrency,
# MAGIC S.WRTFW_KFX as FixedValueInObjectCurrency,
# MAGIC S.FWAER_KPF as ObjectCurrency,
# MAGIC S.WRTFW_POS as ValueInTransactionCurrency,
# MAGIC S.WRTFW_PFX as FixedValueInTransactionCurrency,
# MAGIC S.FWAER as TransactionCurrency,
# MAGIC S.MKURS as AverageRate,
# MAGIC S.FWEHT as CurrencyUnit,
# MAGIC S.MENGE as Quantity,
# MAGIC S.MEEHT as BaseUnitOfMeasure,
# MAGIC S.SUMM1 as QuantitiesTotalForItemCategoryS,
# MAGIC S.SUMM2 as CapitalSpendingTotalForItemCatS,
# MAGIC S.SUMM3 as TotalOfTwoOrMoreItems,
# MAGIC S.DPREIS as AveragePriceForPeriods,
# MAGIC S.PREIS1 as ValueOfDeltaProfitInCoAreaCurrencyLegalView,
# MAGIC S.PREIS2 as ValueOfDeltaProfitInObjectCurrencyLegalView,
# MAGIC S.PREIS3 as ValueOfDeltaProfitInCoAreaCurrencyProfitCtrView,
# MAGIC S.PREIS4 as ValueOfDeltaProfitInObjectCurrencyProfitCtrView,
# MAGIC S.PREIS5 as PriceComponent5,
# MAGIC S.ZUABS as OverheadAsAbsoluteFigure,
# MAGIC S.ZUFKT as OverheadFactor,
# MAGIC S.ARBID as ObjectId,
# MAGIC S.ESOKZ as PurchasingInfoRecordCat,
# MAGIC S.EBELP as ItemNumberOfPurchDoc,
# MAGIC S.STPOS as EquivalenceNumberWith4Places,
# MAGIC S.UMREZ as NumeratorForConvOfOrderUnitToBaseUnit,
# MAGIC S.UMREN as DenominatorForConvOfOrderUnitToBaseUnit,
# MAGIC S.AUSMG as ScrapQuantity,
# MAGIC S.AUSMGKO as ComponentScrapQuantity,
# MAGIC S.AUSPROZ as ScrapFactor,
# MAGIC S.VORNR as ActivityNumber,
# MAGIC S.STEAS as TargetStartDateOfOperation,
# MAGIC S.POSNR_EXT as ItemNumberForExternalLayout,
# MAGIC --S.POINTER1 as PointerNOnItemNum,
# MAGIC --S.POINTER2 as PointerNOnItemNum,
# MAGIC --S.POINTER3 as PointerNOnItemNum,
# MAGIC S.OPREIS as PriceInObjectCurrency,
# MAGIC S.OPREIFX as FixedPriceInObjectCurrency,
# MAGIC S.TPREIS as PriceInTransactionCurrency,
# MAGIC S.TPREIFX as FixedPriceInTransactionCurrency,
# MAGIC S.PATNR as PartnerNumberForCosting,
# MAGIC S.VERWS as ReferenceOfKalktabItemsToAContext,
# MAGIC S.STRAT as ValuationStrategyUsedForCostingItem,
# MAGIC S.TKURS as ExchangeRateItemForeignCurrency,
# MAGIC S.SELKZ as RelevancyToCostingIndicator,
# MAGIC S.SSEDD as LatestScheduledFinishExecutionDate,
# MAGIC S.UKALN as CostEstimateNumOfTransferredCostEstimate,
# MAGIC S.UKALKA as CostingTypeOfTransferredCostEstimate,
# MAGIC S.UKADKY as DateOfTransferredCostEstimate,
# MAGIC S.UTVERS as CostingVersionForTransfer,
# MAGIC S.UBWVAR as ValuationVariantOfTransferredCostEstimate,
# MAGIC S.HKMAT as MaterialRelatedOrigin,
# MAGIC S.SPOSN as BomItemNumber,
# MAGIC S.USTRAT as TransferStrategySearchTerm1,
# MAGIC S.PEINH_2 as PriceUnit2,
# MAGIC S.PEINH_3 as PriceUnit3,
# MAGIC S.`/CWM/MENGE_BEW` as InputQuantityInValuationUOM,
# MAGIC S.`/CWM/AUSMG_BEW` as ScrapQuantityInValuationUOM,
# MAGIC S.`/CWM/AUSMGKO_BEW` as ComponentScrapQuantityInValuationUOM,
# MAGIC S.WERKS as Plant,
# MAGIC S.MATNR as MaterialNumber,
# MAGIC S.BWTAR as ValuationType,
# MAGIC S.PRCTR as ProfitCenter,
# MAGIC S.PSPNR as WbsElement,
# MAGIC S.DISST as LowLevelCode,
# MAGIC S.CUOBJ as ObjectNumberForConfObjects,
# MAGIC S.BWKEY as ValuationArea,
# MAGIC S.KOKRS as ControllingArea,
# MAGIC S.KADAT as CostingDateFrom,
# MAGIC S.BIDAT as CostingDateTo,
# MAGIC S.BWDAT as ValuationDateOfACostEstimate,
# MAGIC S.ALDAT as QuantityStructureDateForCosting,
# MAGIC --S.STCNT as InternalCounter,
# MAGIC --S.PLNCT as InternalCounter,
# MAGIC S.LOSGR as CostingLotSize,
# MAGIC S.MEINS as ValuationUOM,
# MAGIC S.ERFNM as CostedByUser,
# MAGIC S.CPUDT as DateOnWhichCostEstimateWasCreated,
# MAGIC S.CPUTIME as SystemTime,
# MAGIC S.FEH_STA as CostingStatus,
# MAGIC S.FREIG as ReleaseOfStandardCostEstimate,
# MAGIC S.PLSCN as PlanningScenarioInLongTermPlanning,
# MAGIC S.PLMNG as ProductionPlanQtyTransferredToCostEstimate,
# MAGIC S.KALSM as CostingSheetForCalculatingOverhead,
# MAGIC S.KLVAR as CostingVariant,
# MAGIC S.KOSGR as CostingOverheadGroup,
# MAGIC S.ZSCHL as OverheadKey,
# MAGIC S.POPER as PostingPeriod,
# MAGIC S.BDATJ as PostingDateYyyy,
# MAGIC --S.STKOZ as InternalCounter,
# MAGIC --S.ZAEHL as InternalCounter,
# MAGIC S.CMF_NR as ErrorManagementNumber,
# MAGIC S.OCS_COUNT as NumberOfOcsMessagesSent,
# MAGIC S.ERZKA as CostEstimateCreatedWithProdCosting,
# MAGIC S.LOSAU as LotSizeTakingScrapIntoAccount,
# MAGIC S.AUSSS as AssemblyScrapInPercent,
# MAGIC S.SAPRL as ReleaseTheDocumentAsItWasLastSaved,
# MAGIC S.KZROH as MaterialComponent,
# MAGIC S.AUFPL as RoutingNoOperationsWithNonOrdRelatedProd,
# MAGIC S.VORMDAT as DateOnWhichCostEstimateWasMarked,
# MAGIC S.ZIFFR as EquivalenceNumber,
# MAGIC S.MLMAA as MaterialLedgerActivatedAtMaterialLevel,
# MAGIC S.BESKZ as ProcurementType,
# MAGIC S.KALST as CostingLevel,
# MAGIC S.PART_VRSN as PartnerVersion,
# MAGIC S.VOCNT as CounterForMarkingACostEstimate,
# MAGIC S.KURST as ExchangeRateType,
# MAGIC S.HWAER as LocalCurrency,
# MAGIC S.REFID as ReferenceVariant,
# MAGIC S.KALAID as NameOfCostingRun,
# MAGIC S.KALADAT as CostingRunDate,
# MAGIC S.TVERS_SENDER as SenderCostingVersion,
# MAGIC S.`/CWM/LOSGR_BAS` as CostingLotSizeInBaseUOM,
# MAGIC S.`/CWM/LOSGR_BEW` as CostingLotSizeInValuationUOM,
# MAGIC S.PACKNO as PackageNumber,
# MAGIC S.INTROW as InternalLineNumberForLimits,
# MAGIC S.BOSDVERSION as Version,
# MAGIC S.CLINT as InternalClassNumber,
# MAGIC '' as NewUnitBurdenCost,
# MAGIC '' as NewUnitLaborCost,
# MAGIC '' as NewUnitServiceCost,
# MAGIC '' as OldUnitBurdenCost,
# MAGIC '' as OldUnitLaborCost,
# MAGIC '' as OldUnitMaterialCost,
# MAGIC '' as OldUnitServiceCost,
# MAGIC '' as ProductCode,
# MAGIC '' as QtyOnHand,
# MAGIC S.DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from tmp_ckis_keko as S
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC select
# MAGIC '00' as LedgerForControllingObjects,
# MAGIC V.DataSource as ReferenceObject,
# MAGIC '01' as CostEstimateNumber,
# MAGIC V.CostingType as CostingType,
# MAGIC V.DATE_POSTED as CostingDate,
# MAGIC '' as CostingVersion,
# MAGIC V.ValuationVariantInCosting as ValuationVariantInCosting,
# MAGIC V.UnitCostingLineItemNumber as UnitCostingLineItemNumber,
# MAGIC '' as AdditiveCostEst,
# MAGIC '' as ItemCategory,
# MAGIC '' as CostElement,
# MAGIC '' as MainCostComponentSplit,
# MAGIC '' as AuxiliaryCostComponentSplit,
# MAGIC '' as OriginControllingAreaItem,
# MAGIC V.N_UNIT_MATERIAL_COST as PriceInCOCurrency,
# MAGIC '' as FixedPriceInCOCurrency,
# MAGIC V.PriceUnitOfPricesInCOCurrency as PriceUnitOfPricesInCOCurrency,
# MAGIC '' as PriceQuantityUnit,
# MAGIC '' as ValueInCOCurrencyB,
# MAGIC V.N_UNIT_MATERIAL_COST as ValueInCOCurrencyN,
# MAGIC '' as FixedValueInCOCurrency,
# MAGIC '' as ValueInObjectCurrency,
# MAGIC '' as FixedValueInObjectCurrency,
# MAGIC 'USD' as ObjectCurrency,
# MAGIC '' as ValueInTransactionCurrency,
# MAGIC '' as FixedValueInTransactionCurrency,
# MAGIC 'USD' as TransactionCurrency,
# MAGIC '' as AverageRate,
# MAGIC '' as CurrencyUnit,
# MAGIC V.Quantity as Quantity,
# MAGIC '' as BaseUnitOfMeasure,
# MAGIC '' as QuantitiesTotalForItemCategoryS,
# MAGIC '' as CapitalSpendingTotalForItemCatS,
# MAGIC '' as TotalOfTwoOrMoreItems,
# MAGIC '' as AveragePriceForPeriods,
# MAGIC '' as ValueOfDeltaProfitInCoAreaCurrencyLegalView,
# MAGIC '' as ValueOfDeltaProfitInObjectCurrencyLegalView,
# MAGIC '' as ValueOfDeltaProfitInCoAreaCurrencyProfitCtrView,
# MAGIC '' as ValueOfDeltaProfitInObjectCurrencyProfitCtrView,
# MAGIC '' as PriceComponent5,
# MAGIC '' as OverheadAsAbsoluteFigure,
# MAGIC '' as OverheadFactor,
# MAGIC '' as ObjectId,
# MAGIC '' as PurchasingInfoRecordCat,
# MAGIC '' as ItemNumberOfPurchDoc,
# MAGIC '' as EquivalenceNumberWith4Places,
# MAGIC '' as NumeratorForConvOfOrderUnitToBaseUnit,
# MAGIC '' as DenominatorForConvOfOrderUnitToBaseUnit,
# MAGIC '' as ScrapQuantity,
# MAGIC '' as ComponentScrapQuantity,
# MAGIC '' as ScrapFactor,
# MAGIC '' as ActivityNumber,
# MAGIC '' as TargetStartDateOfOperation,
# MAGIC '' as ItemNumberForExternalLayout,
# MAGIC --'' as PointerNOnItemNum,
# MAGIC --'' as PointerNOnItemNum,
# MAGIC --'' as PointerNOnItemNum,
# MAGIC '' as PriceInObjectCurrency,
# MAGIC '' as FixedPriceInObjectCurrency,
# MAGIC '' as PriceInTransactionCurrency,
# MAGIC '' as FixedPriceInTransactionCurrency,
# MAGIC '' as PartnerNumberForCosting,
# MAGIC '' as ReferenceOfKalktabItemsToAContext,
# MAGIC '' as ValuationStrategyUsedForCostingItem,
# MAGIC '' as ExchangeRateItemForeignCurrency,
# MAGIC '' as RelevancyToCostingIndicator,
# MAGIC '' as LatestScheduledFinishExecutionDate,
# MAGIC '' as CostEstimateNumOfTransferredCostEstimate,
# MAGIC '' as CostingTypeOfTransferredCostEstimate,
# MAGIC '' as DateOfTransferredCostEstimate,
# MAGIC '' as CostingVersionForTransfer,
# MAGIC '' as ValuationVariantOfTransferredCostEstimate,
# MAGIC '' as MaterialRelatedOrigin,
# MAGIC '' as BomItemNumber,
# MAGIC '' as TransferStrategySearchTerm1,
# MAGIC '' as PriceUnit2,
# MAGIC '' as PriceUnit3,
# MAGIC '' as InputQuantityInValuationUOM,
# MAGIC '' as ScrapQuantityInValuationUOM,
# MAGIC '' as ComponentScrapQuantityInValuationUOM,
# MAGIC V.Plant as Plant,
# MAGIC V.PART_ID as MaterialNumber,
# MAGIC '' as ValuationType,
# MAGIC '' as ProfitCenter,
# MAGIC '' as WbsElement,
# MAGIC '' as LowLevelCode,
# MAGIC '' as ObjectNumberForConfObjects,
# MAGIC V.ValuationArea as ValuationArea,
# MAGIC '' as ControllingArea,
# MAGIC V.DATE_POSTED as CostingDateFrom,
# MAGIC '' as CostingDateTo,
# MAGIC '' as ValuationDateOfACostEstimate,
# MAGIC '' as QuantityStructureDateForCosting,
# MAGIC --'' as InternalCounter,
# MAGIC --'' as InternalCounter,
# MAGIC '' as CostingLotSize,
# MAGIC V.ValuationUOM as ValuationUOM,
# MAGIC '' as CostedByUser,
# MAGIC '' as DateOnWhichCostEstimateWasCreated,
# MAGIC '' as SystemTime,
# MAGIC '' as CostingStatus,
# MAGIC '' as ReleaseOfStandardCostEstimate,
# MAGIC '' as PlanningScenarioInLongTermPlanning,
# MAGIC '' as ProductionPlanQtyTransferredToCostEstimate,
# MAGIC '' as CostingSheetForCalculatingOverhead,
# MAGIC '' as CostingVariant,
# MAGIC '' as CostingOverheadGroup,
# MAGIC '' as OverheadKey,
# MAGIC Month(V.DATE_POSTED) as PostingPeriod,
# MAGIC '' as PostingDateYyyy,
# MAGIC --'' as InternalCounter,
# MAGIC --'' as InternalCounter,
# MAGIC '' as ErrorManagementNumber,
# MAGIC '' as NumberOfOcsMessagesSent,
# MAGIC '' as CostEstimateCreatedWithProdCosting,
# MAGIC '' as LotSizeTakingScrapIntoAccount,
# MAGIC '' as AssemblyScrapInPercent,
# MAGIC '' as ReleaseTheDocumentAsItWasLastSaved,
# MAGIC '' as MaterialComponent,
# MAGIC '' as RoutingNoOperationsWithNonOrdRelatedProd,
# MAGIC '' as DateOnWhichCostEstimateWasMarked,
# MAGIC '' as EquivalenceNumber,
# MAGIC '' as MaterialLedgerActivatedAtMaterialLevel,
# MAGIC '' as ProcurementType,
# MAGIC '' as CostingLevel,
# MAGIC '' as PartnerVersion,
# MAGIC '' as CounterForMarkingACostEstimate,
# MAGIC '' as ExchangeRateType,
# MAGIC '' as LocalCurrency,
# MAGIC '' as ReferenceVariant,
# MAGIC '' as NameOfCostingRun,
# MAGIC '' as CostingRunDate,
# MAGIC '' as SenderCostingVersion,
# MAGIC '' as CostingLotSizeInBaseUOM,
# MAGIC '' as CostingLotSizeInValuationUOM,
# MAGIC '' as PackageNumber,
# MAGIC '' as InternalLineNumberForLimits,
# MAGIC '' as Version,
# MAGIC '' as InternalClassNumber,
# MAGIC V.N_UNIT_BURDEN_COST as NewUnitBurdenCost,
# MAGIC V.N_UNIT_LABOR_COST as NewUnitLaborCost,
# MAGIC V.N_UNIT_SERVICE_COST as NewUnitServiceCost,
# MAGIC V.O_UNIT_BURDEN_COST as OldUnitBurdenCost,
# MAGIC V.O_UNIT_LABOR_COST as OldUnitLaborCost,
# MAGIC V.O_UNIT_MATERIAL_COST as OldUnitMaterialCost,
# MAGIC V.O_UNIT_SERVICE_COST as OldUnitServiceCost,
# MAGIC V.PRODUCT_CODE as ProductCode,
# MAGIC V.QTY_ON_HAND as QtyOnHand,
# MAGIC V.DataSource,
# MAGIC now() as UpdatedOn
# MAGIC from
# MAGIC tmp_uct_part_cost_change as V

# COMMAND ----------

df_part_cost_detail = spark.sql("select * from part_cost_detail")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_part_cost_detail.write.format(write_format).mode("overwrite").partitionBy("DataSource").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_part_cost_detail.createOrReplaceTempView('tmp_part_cost_detail')

# COMMAND ----------

spark.sql("delete from fedw.part_cost_detail where datasource = 'VE70'")
spark.sql("delete from fedw.part_cost_detail where datasource = 'VE72'")

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into fedw.part_cost_detail as T
# MAGIC Using tmp_part_cost_detail as S
# MAGIC on T.LedgerForControllingObjects = S.LedgerForControllingObjects 
# MAGIC and T.ReferenceObject = S.ReferenceObject 
# MAGIC and T.CostEstimateNumber = S.CostEstimateNumber 
# MAGIC and T.CostingType = S.CostingType 
# MAGIC and T.CostingDate = S.CostingDate 
# MAGIC and T.CostingVersion = S.CostingVersion 
# MAGIC and T.ValuationVariantInCosting = S.ValuationVariantInCosting 
# MAGIC and T.UnitCostingLineItemNumber = S.UnitCostingLineItemNumber 
# MAGIC and T.DataSource = 'SAP'
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.LedgerForControllingObjects =  S.LedgerForControllingObjects,
# MAGIC T.ReferenceObject =  S.ReferenceObject,
# MAGIC T.CostEstimateNumber =  S.CostEstimateNumber,
# MAGIC T.CostingType =  S.CostingType,
# MAGIC T.CostingDate =  S.CostingDate,
# MAGIC T.CostingVersion =  S.CostingVersion,
# MAGIC T.ValuationVariantInCosting =  S.ValuationVariantInCosting,
# MAGIC T.UnitCostingLineItemNumber =  S.UnitCostingLineItemNumber,
# MAGIC T.AdditiveCostEst =  S.AdditiveCostEst,
# MAGIC T.ItemCategory =  S.ItemCategory,
# MAGIC T.CostElement =  S.CostElement,
# MAGIC T.MainCostComponentSplit =  S.MainCostComponentSplit,
# MAGIC T.AuxiliaryCostComponentSplit =  S.AuxiliaryCostComponentSplit,
# MAGIC T.OriginControllingAreaItem =  S.OriginControllingAreaItem,
# MAGIC T.PriceInCOCurrency =  S.PriceInCOCurrency,
# MAGIC T.FixedPriceInCOCurrency =  S.FixedPriceInCOCurrency,
# MAGIC T.PriceUnitOfPricesInCOCurrency =  S.PriceUnitOfPricesInCOCurrency,
# MAGIC T.PriceQuantityUnit =  S.PriceQuantityUnit,
# MAGIC T.ValueInCOCurrencyB =  S.ValueInCOCurrencyB,
# MAGIC T.ValueInCOCurrencyN =  S.ValueInCOCurrencyN,
# MAGIC T.FixedValueInCOCurrency =  S.FixedValueInCOCurrency,
# MAGIC T.ValueInObjectCurrency =  S.ValueInObjectCurrency,
# MAGIC T.FixedValueInObjectCurrency =  S.FixedValueInObjectCurrency,
# MAGIC T.ObjectCurrency =  S.ObjectCurrency,
# MAGIC T.ValueInTransactionCurrency =  S.ValueInTransactionCurrency,
# MAGIC T.FixedValueInTransactionCurrency =  S.FixedValueInTransactionCurrency,
# MAGIC T.TransactionCurrency =  S.TransactionCurrency,
# MAGIC T.AverageRate =  S.AverageRate,
# MAGIC T.CurrencyUnit =  S.CurrencyUnit,
# MAGIC T.Quantity =  S.Quantity,
# MAGIC T.BaseUnitOfMeasure =  S.BaseUnitOfMeasure,
# MAGIC T.QuantitiesTotalForItemCategoryS =  S.QuantitiesTotalForItemCategoryS,
# MAGIC T.CapitalSpendingTotalForItemCatS =  S.CapitalSpendingTotalForItemCatS,
# MAGIC T.TotalOfTwoOrMoreItems =  S.TotalOfTwoOrMoreItems,
# MAGIC T.AveragePriceForPeriods =  S.AveragePriceForPeriods,
# MAGIC T.ValueOfDeltaProfitInCoAreaCurrencyLegalView =  S.ValueOfDeltaProfitInCoAreaCurrencyLegalView,
# MAGIC T.ValueOfDeltaProfitInObjectCurrencyLegalView =  S.ValueOfDeltaProfitInObjectCurrencyLegalView,
# MAGIC T.ValueOfDeltaProfitInCoAreaCurrencyProfitCtrView =  S.ValueOfDeltaProfitInCoAreaCurrencyProfitCtrView,
# MAGIC T.ValueOfDeltaProfitInObjectCurrencyProfitCtrView =  S.ValueOfDeltaProfitInObjectCurrencyProfitCtrView,
# MAGIC T.PriceComponent5 =  S.PriceComponent5,
# MAGIC T.OverheadAsAbsoluteFigure =  S.OverheadAsAbsoluteFigure,
# MAGIC T.OverheadFactor =  S.OverheadFactor,
# MAGIC T.ObjectId =  S.ObjectId,
# MAGIC T.PurchasingInfoRecordCat =  S.PurchasingInfoRecordCat,
# MAGIC T.ItemNumberOfPurchDoc =  S.ItemNumberOfPurchDoc,
# MAGIC T.EquivalenceNumberWith4Places =  S.EquivalenceNumberWith4Places,
# MAGIC T.NumeratorForConvOfOrderUnitToBaseUnit =  S.NumeratorForConvOfOrderUnitToBaseUnit,
# MAGIC T.DenominatorForConvOfOrderUnitToBaseUnit =  S.DenominatorForConvOfOrderUnitToBaseUnit,
# MAGIC T.ScrapQuantity =  S.ScrapQuantity,
# MAGIC T.ComponentScrapQuantity =  S.ComponentScrapQuantity,
# MAGIC T.ScrapFactor =  S.ScrapFactor,
# MAGIC T.ActivityNumber =  S.ActivityNumber,
# MAGIC T.TargetStartDateOfOperation =  S.TargetStartDateOfOperation,
# MAGIC T.ItemNumberForExternalLayout =  S.ItemNumberForExternalLayout,
# MAGIC T.PriceInObjectCurrency =  S.PriceInObjectCurrency,
# MAGIC T.FixedPriceInObjectCurrency =  S.FixedPriceInObjectCurrency,
# MAGIC T.PriceInTransactionCurrency =  S.PriceInTransactionCurrency,
# MAGIC T.FixedPriceInTransactionCurrency =  S.FixedPriceInTransactionCurrency,
# MAGIC T.PartnerNumberForCosting =  S.PartnerNumberForCosting,
# MAGIC T.ReferenceOfKalktabItemsToAContext =  S.ReferenceOfKalktabItemsToAContext,
# MAGIC T.ValuationStrategyUsedForCostingItem =  S.ValuationStrategyUsedForCostingItem,
# MAGIC T.ExchangeRateItemForeignCurrency =  S.ExchangeRateItemForeignCurrency,
# MAGIC T.RelevancyToCostingIndicator =  S.RelevancyToCostingIndicator,
# MAGIC T.LatestScheduledFinishExecutionDate =  S.LatestScheduledFinishExecutionDate,
# MAGIC T.CostEstimateNumOfTransferredCostEstimate =  S.CostEstimateNumOfTransferredCostEstimate,
# MAGIC T.CostingTypeOfTransferredCostEstimate =  S.CostingTypeOfTransferredCostEstimate,
# MAGIC T.DateOfTransferredCostEstimate =  S.DateOfTransferredCostEstimate,
# MAGIC T.CostingVersionForTransfer =  S.CostingVersionForTransfer,
# MAGIC T.ValuationVariantOfTransferredCostEstimate =  S.ValuationVariantOfTransferredCostEstimate,
# MAGIC T.MaterialRelatedOrigin =  S.MaterialRelatedOrigin,
# MAGIC T.BomItemNumber =  S.BomItemNumber,
# MAGIC T.TransferStrategySearchTerm1 =  S.TransferStrategySearchTerm1,
# MAGIC T.PriceUnit2 =  S.PriceUnit2,
# MAGIC T.PriceUnit3 =  S.PriceUnit3,
# MAGIC T.InputQuantityInValuationUOM =  S.InputQuantityInValuationUOM,
# MAGIC T.ScrapQuantityInValuationUOM =  S.ScrapQuantityInValuationUOM,
# MAGIC T.ComponentScrapQuantityInValuationUOM =  S.ComponentScrapQuantityInValuationUOM,
# MAGIC T.Plant =  S.Plant,
# MAGIC T.MaterialNumber =  S.MaterialNumber,
# MAGIC T.ValuationType =  S.ValuationType,
# MAGIC T.ProfitCenter =  S.ProfitCenter,
# MAGIC T.WbsElement =  S.WbsElement,
# MAGIC T.LowLevelCode =  S.LowLevelCode,
# MAGIC T.ObjectNumberForConfObjects =  S.ObjectNumberForConfObjects,
# MAGIC T.ValuationArea =  S.ValuationArea,
# MAGIC T.ControllingArea =  S.ControllingArea,
# MAGIC T.CostingDateFrom =  S.CostingDateFrom,
# MAGIC T.CostingDateTo =  S.CostingDateTo,
# MAGIC T.ValuationDateOfACostEstimate =  S.ValuationDateOfACostEstimate,
# MAGIC T.QuantityStructureDateForCosting =  S.QuantityStructureDateForCosting,
# MAGIC T.CostingLotSize =  S.CostingLotSize,
# MAGIC T.ValuationUOM =  S.ValuationUOM,
# MAGIC T.CostedByUser =  S.CostedByUser,
# MAGIC T.DateOnWhichCostEstimateWasCreated =  S.DateOnWhichCostEstimateWasCreated,
# MAGIC T.SystemTime =  S.SystemTime,
# MAGIC T.CostingStatus =  S.CostingStatus,
# MAGIC T.ReleaseOfStandardCostEstimate =  S.ReleaseOfStandardCostEstimate,
# MAGIC T.PlanningScenarioInLongTermPlanning =  S.PlanningScenarioInLongTermPlanning,
# MAGIC T.ProductionPlanQtyTransferredToCostEstimate =  S.ProductionPlanQtyTransferredToCostEstimate,
# MAGIC T.CostingSheetForCalculatingOverhead =  S.CostingSheetForCalculatingOverhead,
# MAGIC T.CostingVariant =  S.CostingVariant,
# MAGIC T.CostingOverheadGroup =  S.CostingOverheadGroup,
# MAGIC T.OverheadKey =  S.OverheadKey,
# MAGIC T.PostingPeriod =  S.PostingPeriod,
# MAGIC T.PostingDateYyyy =  S.PostingDateYyyy,
# MAGIC T.ErrorManagementNumber =  S.ErrorManagementNumber,
# MAGIC T.NumberOfOcsMessagesSent =  S.NumberOfOcsMessagesSent,
# MAGIC T.CostEstimateCreatedWithProdCosting =  S.CostEstimateCreatedWithProdCosting,
# MAGIC T.LotSizeTakingScrapIntoAccount =  S.LotSizeTakingScrapIntoAccount,
# MAGIC T.AssemblyScrapInPercent =  S.AssemblyScrapInPercent,
# MAGIC T.ReleaseTheDocumentAsItWasLastSaved =  S.ReleaseTheDocumentAsItWasLastSaved,
# MAGIC T.MaterialComponent =  S.MaterialComponent,
# MAGIC T.RoutingNoOperationsWithNonOrdRelatedProd =  S.RoutingNoOperationsWithNonOrdRelatedProd,
# MAGIC T.DateOnWhichCostEstimateWasMarked =  S.DateOnWhichCostEstimateWasMarked,
# MAGIC T.EquivalenceNumber =  S.EquivalenceNumber,
# MAGIC T.MaterialLedgerActivatedAtMaterialLevel =  S.MaterialLedgerActivatedAtMaterialLevel,
# MAGIC T.ProcurementType =  S.ProcurementType,
# MAGIC T.CostingLevel =  S.CostingLevel,
# MAGIC T.PartnerVersion =  S.PartnerVersion,
# MAGIC T.CounterForMarkingACostEstimate =  S.CounterForMarkingACostEstimate,
# MAGIC T.ExchangeRateType =  S.ExchangeRateType,
# MAGIC T.LocalCurrency =  S.LocalCurrency,
# MAGIC T.ReferenceVariant =  S.ReferenceVariant,
# MAGIC T.NameOfCostingRun =  S.NameOfCostingRun,
# MAGIC T.CostingRunDate =  S.CostingRunDate,
# MAGIC T.SenderCostingVersion =  S.SenderCostingVersion,
# MAGIC T.CostingLotSizeInBaseUOM =  S.CostingLotSizeInBaseUOM,
# MAGIC T.CostingLotSizeInValuationUOM =  S.CostingLotSizeInValuationUOM,
# MAGIC T.PackageNumber =  S.PackageNumber,
# MAGIC T.InternalLineNumberForLimits =  S.InternalLineNumberForLimits,
# MAGIC T.Version =  S.Version,
# MAGIC T.InternalClassNumber =  S.InternalClassNumber,
# MAGIC T.NewUnitBurdenCost =  S.NewUnitBurdenCost,
# MAGIC T.NewUnitLaborCost =  S.NewUnitLaborCost,
# MAGIC T.NewUnitServiceCost =  S.NewUnitServiceCost,
# MAGIC T.OldUnitBurdenCost =  S.OldUnitBurdenCost,
# MAGIC T.OldUnitLaborCost =  S.OldUnitLaborCost,
# MAGIC T.OldUnitMaterialCost =  S.OldUnitMaterialCost,
# MAGIC T.OldUnitServiceCost =  S.OldUnitServiceCost,
# MAGIC T.ProductCode =  S.ProductCode,
# MAGIC T.QtyOnHand =  S.QtyOnHand,
# MAGIC T.DataSource =  S.DataSource,
# MAGIC T.UpdatedOn = now()
# MAGIC 
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT
# MAGIC (
# MAGIC LedgerForControllingObjects,
# MAGIC ReferenceObject,
# MAGIC CostEstimateNumber,
# MAGIC CostingType,
# MAGIC CostingDate,
# MAGIC CostingVersion,
# MAGIC ValuationVariantInCosting,
# MAGIC UnitCostingLineItemNumber,
# MAGIC AdditiveCostEst,
# MAGIC ItemCategory,
# MAGIC CostElement,
# MAGIC MainCostComponentSplit,
# MAGIC AuxiliaryCostComponentSplit,
# MAGIC OriginControllingAreaItem,
# MAGIC PriceInCOCurrency,
# MAGIC FixedPriceInCOCurrency,
# MAGIC PriceUnitOfPricesInCOCurrency,
# MAGIC PriceQuantityUnit,
# MAGIC ValueInCOCurrencyB,
# MAGIC ValueInCOCurrencyN,
# MAGIC FixedValueInCOCurrency,
# MAGIC ValueInObjectCurrency,
# MAGIC FixedValueInObjectCurrency,
# MAGIC ObjectCurrency,
# MAGIC ValueInTransactionCurrency,
# MAGIC FixedValueInTransactionCurrency,
# MAGIC TransactionCurrency,
# MAGIC AverageRate,
# MAGIC CurrencyUnit,
# MAGIC Quantity,
# MAGIC BaseUnitOfMeasure,
# MAGIC QuantitiesTotalForItemCategoryS,
# MAGIC CapitalSpendingTotalForItemCatS,
# MAGIC TotalOfTwoOrMoreItems,
# MAGIC AveragePriceForPeriods,
# MAGIC ValueOfDeltaProfitInCoAreaCurrencyLegalView,
# MAGIC ValueOfDeltaProfitInObjectCurrencyLegalView,
# MAGIC ValueOfDeltaProfitInCoAreaCurrencyProfitCtrView,
# MAGIC ValueOfDeltaProfitInObjectCurrencyProfitCtrView,
# MAGIC PriceComponent5,
# MAGIC OverheadAsAbsoluteFigure,
# MAGIC OverheadFactor,
# MAGIC ObjectId,
# MAGIC PurchasingInfoRecordCat,
# MAGIC ItemNumberOfPurchDoc,
# MAGIC EquivalenceNumberWith4Places,
# MAGIC NumeratorForConvOfOrderUnitToBaseUnit,
# MAGIC DenominatorForConvOfOrderUnitToBaseUnit,
# MAGIC ScrapQuantity,
# MAGIC ComponentScrapQuantity,
# MAGIC ScrapFactor,
# MAGIC ActivityNumber,
# MAGIC TargetStartDateOfOperation,
# MAGIC ItemNumberForExternalLayout,
# MAGIC PriceInObjectCurrency,
# MAGIC FixedPriceInObjectCurrency,
# MAGIC PriceInTransactionCurrency,
# MAGIC FixedPriceInTransactionCurrency,
# MAGIC PartnerNumberForCosting,
# MAGIC ReferenceOfKalktabItemsToAContext,
# MAGIC ValuationStrategyUsedForCostingItem,
# MAGIC ExchangeRateItemForeignCurrency,
# MAGIC RelevancyToCostingIndicator,
# MAGIC LatestScheduledFinishExecutionDate,
# MAGIC CostEstimateNumOfTransferredCostEstimate,
# MAGIC CostingTypeOfTransferredCostEstimate,
# MAGIC DateOfTransferredCostEstimate,
# MAGIC CostingVersionForTransfer,
# MAGIC ValuationVariantOfTransferredCostEstimate,
# MAGIC MaterialRelatedOrigin,
# MAGIC BomItemNumber,
# MAGIC TransferStrategySearchTerm1,
# MAGIC PriceUnit2,
# MAGIC PriceUnit3,
# MAGIC InputQuantityInValuationUOM,
# MAGIC ScrapQuantityInValuationUOM,
# MAGIC ComponentScrapQuantityInValuationUOM,
# MAGIC Plant,
# MAGIC MaterialNumber,
# MAGIC ValuationType,
# MAGIC ProfitCenter,
# MAGIC WbsElement,
# MAGIC LowLevelCode,
# MAGIC ObjectNumberForConfObjects,
# MAGIC ValuationArea,
# MAGIC ControllingArea,
# MAGIC CostingDateFrom,
# MAGIC CostingDateTo,
# MAGIC ValuationDateOfACostEstimate,
# MAGIC QuantityStructureDateForCosting,
# MAGIC CostingLotSize,
# MAGIC ValuationUOM,
# MAGIC CostedByUser,
# MAGIC DateOnWhichCostEstimateWasCreated,
# MAGIC SystemTime,
# MAGIC CostingStatus,
# MAGIC ReleaseOfStandardCostEstimate,
# MAGIC PlanningScenarioInLongTermPlanning,
# MAGIC ProductionPlanQtyTransferredToCostEstimate,
# MAGIC CostingSheetForCalculatingOverhead,
# MAGIC CostingVariant,
# MAGIC CostingOverheadGroup,
# MAGIC OverheadKey,
# MAGIC PostingPeriod,
# MAGIC PostingDateYyyy,
# MAGIC ErrorManagementNumber,
# MAGIC NumberOfOcsMessagesSent,
# MAGIC CostEstimateCreatedWithProdCosting,
# MAGIC LotSizeTakingScrapIntoAccount,
# MAGIC AssemblyScrapInPercent,
# MAGIC ReleaseTheDocumentAsItWasLastSaved,
# MAGIC MaterialComponent,
# MAGIC RoutingNoOperationsWithNonOrdRelatedProd,
# MAGIC DateOnWhichCostEstimateWasMarked,
# MAGIC EquivalenceNumber,
# MAGIC MaterialLedgerActivatedAtMaterialLevel,
# MAGIC ProcurementType,
# MAGIC CostingLevel,
# MAGIC PartnerVersion,
# MAGIC CounterForMarkingACostEstimate,
# MAGIC ExchangeRateType,
# MAGIC LocalCurrency,
# MAGIC ReferenceVariant,
# MAGIC NameOfCostingRun,
# MAGIC CostingRunDate,
# MAGIC SenderCostingVersion,
# MAGIC CostingLotSizeInBaseUOM,
# MAGIC CostingLotSizeInValuationUOM,
# MAGIC PackageNumber,
# MAGIC InternalLineNumberForLimits,
# MAGIC Version,
# MAGIC InternalClassNumber,
# MAGIC NewUnitBurdenCost,
# MAGIC NewUnitLaborCost,
# MAGIC NewUnitServiceCost,
# MAGIC OldUnitBurdenCost,
# MAGIC OldUnitLaborCost,
# MAGIC OldUnitMaterialCost,
# MAGIC OldUnitServiceCost,
# MAGIC ProductCode,
# MAGIC QtyOnHand,
# MAGIC DataSource,
# MAGIC UpdatedOn
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.LedgerForControllingObjects,
# MAGIC S.ReferenceObject,
# MAGIC S.CostEstimateNumber,
# MAGIC S.CostingType,
# MAGIC S.CostingDate,
# MAGIC S.CostingVersion,
# MAGIC S.ValuationVariantInCosting,
# MAGIC S.UnitCostingLineItemNumber,
# MAGIC S.AdditiveCostEst,
# MAGIC S.ItemCategory,
# MAGIC S.CostElement,
# MAGIC S.MainCostComponentSplit,
# MAGIC S.AuxiliaryCostComponentSplit,
# MAGIC S.OriginControllingAreaItem,
# MAGIC S.PriceInCOCurrency,
# MAGIC S.FixedPriceInCOCurrency,
# MAGIC S.PriceUnitOfPricesInCOCurrency,
# MAGIC S.PriceQuantityUnit,
# MAGIC S.ValueInCOCurrencyB,
# MAGIC S.ValueInCOCurrencyN,
# MAGIC S.FixedValueInCOCurrency,
# MAGIC S.ValueInObjectCurrency,
# MAGIC S.FixedValueInObjectCurrency,
# MAGIC S.ObjectCurrency,
# MAGIC S.ValueInTransactionCurrency,
# MAGIC S.FixedValueInTransactionCurrency,
# MAGIC S.TransactionCurrency,
# MAGIC S.AverageRate,
# MAGIC S.CurrencyUnit,
# MAGIC S.Quantity,
# MAGIC S.BaseUnitOfMeasure,
# MAGIC S.QuantitiesTotalForItemCategoryS,
# MAGIC S.CapitalSpendingTotalForItemCatS,
# MAGIC S.TotalOfTwoOrMoreItems,
# MAGIC S.AveragePriceForPeriods,
# MAGIC S.ValueOfDeltaProfitInCoAreaCurrencyLegalView,
# MAGIC S.ValueOfDeltaProfitInObjectCurrencyLegalView,
# MAGIC S.ValueOfDeltaProfitInCoAreaCurrencyProfitCtrView,
# MAGIC S.ValueOfDeltaProfitInObjectCurrencyProfitCtrView,
# MAGIC S.PriceComponent5,
# MAGIC S.OverheadAsAbsoluteFigure,
# MAGIC S.OverheadFactor,
# MAGIC S.ObjectId,
# MAGIC S.PurchasingInfoRecordCat,
# MAGIC S.ItemNumberOfPurchDoc,
# MAGIC S.EquivalenceNumberWith4Places,
# MAGIC S.NumeratorForConvOfOrderUnitToBaseUnit,
# MAGIC S.DenominatorForConvOfOrderUnitToBaseUnit,
# MAGIC S.ScrapQuantity,
# MAGIC S.ComponentScrapQuantity,
# MAGIC S.ScrapFactor,
# MAGIC S.ActivityNumber,
# MAGIC S.TargetStartDateOfOperation,
# MAGIC S.ItemNumberForExternalLayout,
# MAGIC S.PriceInObjectCurrency,
# MAGIC S.FixedPriceInObjectCurrency,
# MAGIC S.PriceInTransactionCurrency,
# MAGIC S.FixedPriceInTransactionCurrency,
# MAGIC S.PartnerNumberForCosting,
# MAGIC S.ReferenceOfKalktabItemsToAContext,
# MAGIC S.ValuationStrategyUsedForCostingItem,
# MAGIC S.ExchangeRateItemForeignCurrency,
# MAGIC S.RelevancyToCostingIndicator,
# MAGIC S.LatestScheduledFinishExecutionDate,
# MAGIC S.CostEstimateNumOfTransferredCostEstimate,
# MAGIC S.CostingTypeOfTransferredCostEstimate,
# MAGIC S.DateOfTransferredCostEstimate,
# MAGIC S.CostingVersionForTransfer,
# MAGIC S.ValuationVariantOfTransferredCostEstimate,
# MAGIC S.MaterialRelatedOrigin,
# MAGIC S.BomItemNumber,
# MAGIC S.TransferStrategySearchTerm1,
# MAGIC S.PriceUnit2,
# MAGIC S.PriceUnit3,
# MAGIC S.InputQuantityInValuationUOM,
# MAGIC S.ScrapQuantityInValuationUOM,
# MAGIC S.ComponentScrapQuantityInValuationUOM,
# MAGIC S.Plant,
# MAGIC S.MaterialNumber,
# MAGIC S.ValuationType,
# MAGIC S.ProfitCenter,
# MAGIC S.WbsElement,
# MAGIC S.LowLevelCode,
# MAGIC S.ObjectNumberForConfObjects,
# MAGIC S.ValuationArea,
# MAGIC S.ControllingArea,
# MAGIC S.CostingDateFrom,
# MAGIC S.CostingDateTo,
# MAGIC S.ValuationDateOfACostEstimate,
# MAGIC S.QuantityStructureDateForCosting,
# MAGIC S.CostingLotSize,
# MAGIC S.ValuationUOM,
# MAGIC S.CostedByUser,
# MAGIC S.DateOnWhichCostEstimateWasCreated,
# MAGIC S.SystemTime,
# MAGIC S.CostingStatus,
# MAGIC S.ReleaseOfStandardCostEstimate,
# MAGIC S.PlanningScenarioInLongTermPlanning,
# MAGIC S.ProductionPlanQtyTransferredToCostEstimate,
# MAGIC S.CostingSheetForCalculatingOverhead,
# MAGIC S.CostingVariant,
# MAGIC S.CostingOverheadGroup,
# MAGIC S.OverheadKey,
# MAGIC S.PostingPeriod,
# MAGIC S.PostingDateYyyy,
# MAGIC S.ErrorManagementNumber,
# MAGIC S.NumberOfOcsMessagesSent,
# MAGIC S.CostEstimateCreatedWithProdCosting,
# MAGIC S.LotSizeTakingScrapIntoAccount,
# MAGIC S.AssemblyScrapInPercent,
# MAGIC S.ReleaseTheDocumentAsItWasLastSaved,
# MAGIC S.MaterialComponent,
# MAGIC S.RoutingNoOperationsWithNonOrdRelatedProd,
# MAGIC S.DateOnWhichCostEstimateWasMarked,
# MAGIC S.EquivalenceNumber,
# MAGIC S.MaterialLedgerActivatedAtMaterialLevel,
# MAGIC S.ProcurementType,
# MAGIC S.CostingLevel,
# MAGIC S.PartnerVersion,
# MAGIC S.CounterForMarkingACostEstimate,
# MAGIC S.ExchangeRateType,
# MAGIC S.LocalCurrency,
# MAGIC S.ReferenceVariant,
# MAGIC S.NameOfCostingRun,
# MAGIC S.CostingRunDate,
# MAGIC S.SenderCostingVersion,
# MAGIC S.CostingLotSizeInBaseUOM,
# MAGIC S.CostingLotSizeInValuationUOM,
# MAGIC S.PackageNumber,
# MAGIC S.InternalLineNumberForLimits,
# MAGIC S.Version,
# MAGIC S.InternalClassNumber,
# MAGIC S.NewUnitBurdenCost,
# MAGIC S.NewUnitLaborCost,
# MAGIC S.NewUnitServiceCost,
# MAGIC S.OldUnitBurdenCost,
# MAGIC S.OldUnitLaborCost,
# MAGIC S.OldUnitMaterialCost,
# MAGIC S.OldUnitServiceCost,
# MAGIC S.ProductCode,
# MAGIC S.QtyOnHand,
# MAGIC S.DataSource,
# MAGIC now()
# MAGIC )

# COMMAND ----------

if(ts_keko != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'KEKO' and DatabaseName = 'S42'".format(ts_keko))

if(ts_ckis != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'CKIS' and DatabaseName = 'S42'".format(ts_ckis))
    
if(ts_ve70_uct_part_cost_change != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_PART_COST_CHANGE' and DatabaseName = 'VE70'".format(ts_ve70_uct_part_cost_change))

if(ts_ve72_uct_part_cost_change != None):
    spark.sql ("update config.INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'UCT_PART_COST_CHANGE' and DatabaseName = 'VE72'".format(ts_ve72_uct_part_cost_change))


# COMMAND ----------

df_sf = spark.sql("select * from FEDW.part_cost_detail where UpdatedOn > (select LastUpdatedOn from config.INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Part_Cost_Detail' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_part_cost_details_details = ts_sf["max(UpdatedOn)"]
print(ts_part_cost_details_details)

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
  .option("dbtable", "Part_Cost_Detail") \
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
# MAGIC Utils.runQuery(options, """call sp_load_part_cost_change()""")

# COMMAND ----------

if(ts_part_cost_details_details != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Part_Cost_Detail' and DatabaseName = 'FEDW'".format(ts_part_cost_details_details))

# COMMAND ----------


