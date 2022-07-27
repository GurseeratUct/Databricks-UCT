# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Material_Plant'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_marc = [
    "MATNR",
"WERKS",
  "ZZHTS",
"ZZPRODCODE",
"ZZPLNTGRP",
"DISPO",
"PRCTR",
"SOBSL",
"DISMM",
"DISGR",
"EKGRP",
"MMSTA",
"DISLS",
"STRGR",
"PSTAT",
"PLIFZ",
"WEBAZ",
"PERKZ",
"AUSSS",
"MINBE",
"EISBE",
"BSTMI",
"BSTMA",
"BSTFE",
"BSTRF",
"MABST",
"LOSFX",
"BEARZ",
"RUEZT",
"TRANZ",
"BASMG",
"DZEIT",
"MAXLZ",
"UEETO",
"UNETO",
"WZEIT",
"VZUSL",
"PRFRQ",
"UMLMC",
"LGRAD",
"OBJID",
"VRVEZ",
"VBAMG",
"VBEAZ",
"TRAME",
"FXHOR",
"VINT1",
"VINT2",
"LOSGR",
"KAUSF",
"TAKZT",
"CUOBJ",
"VRBFK",
"CUOBV",
"RESVP",
"ABFAC",
"SHZET",
"VKUMC",
"VKTRW",
"GLGMG",
"VKGLG",
"DPLHO",
"MINLS",
"MAXLS",
"FIXLS",
"LTINC",
"COMPL",
"MCRUE",
"LFMON",
"LFGJA",
"EISLO",
"BWESB",
"PPSKZ",
"GI_PR_TIME",
"MIN_TROC",
"MAX_TROC",
"TARGET_STOCK",
"/CWM/UMLMC",
"/CWM/TRAME",
"/CWM/BWESB",
"SCM_GRPRT",
"SCM_GIPRT",
"SCM_SCOST",
"SCM_RELDT",
"SCM_SSPEN",
"SCM_CONHAP",
"SCM_CONHAP_OUT",
"SCM_SHELF_LIFE_DUR",
"SCM_MATURITY_DUR",
"SCM_SHLF_LFE_REQ_MIN",
"SCM_SHLF_LFE_REQ_MAX",
"SCM_REORD_DUR",
"SCM_TARGET_DUR",
"SCM_PEG_PAST_ALERT",
"SCM_PEG_FUTURE_ALERT",
"SCM_PEG_STRATEGY",
"SCM_PRIO",
"SCM_MIN_PASS_AMOUNT",
"ESPPFLG",
"SCM_THRUPUT_TIME",
"SCM_SAFTY_V",
"SCM_PPSAFTYSTK",
"SCM_PPSAFTYSTK_V",
"SCM_REPSAFTY",
"SCM_REPSAFTY_V",
"SCM_REORD_V",
"SCM_MAXSTOCK_V",
"SCM_SCOST_PRCNT",
"SCM_PROC_COST",
"SCM_NDCOSTWE",
"SCM_NDCOSTWA",
"SCM_CONINP",
"/SAPMP/TOLPRPL",
"/SAPMP/TOLPRMI",
"ZZECCN",
"ZZECCN_YR",
"BESKZ",
"UpdatedOn"
]

# COMMAND ----------

col_names_part = [
    "ID",
"PURCHASED",
"PRODUCT_CODE",
"COMMODITY_CODE",
"PREF_VENDOR_ID",
"BUYER_USER_ID",
"ABC_CODE",
"INVENTORY_LOCKED",
"STATUS",
"REVISION_ID"
]

# COMMAND ----------

df = spark.sql("select * from S42.MARC where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'MARC' and DatabaseName = 'S42')")
df_marc = df.select(col_names_marc)
df_marc.createOrReplaceTempView("marc_tmp")
df_ts_marc = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_marc = df_ts_marc["max(UpdatedOn)"]
print(ts_marc)

# COMMAND ----------

df = spark.sql("select * from VE70.PART where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PART' and TargetLayerTableName ='Material_Plant' and DatabaseName = 'VE70')")
df_ve70_part = df.select(col_names_part).withColumn('Plant',lit('6311'))
df_ve70_part.createOrReplaceTempView("ve70_part_tmp")
df_ts_ve70_part = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve70_part = df_ts_ve70_part["max(UpdatedOn)"]
print(ts_ve70_part)

# COMMAND ----------

df = spark.sql("select * from VE72.PART where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'PART' and TargetLayerTableName ='Material_Plant' and DatabaseName = 'VE72')")
df_ve72_part = df.select(col_names_part).withColumn('Plant',lit('6101'))
df_ve72_part.createOrReplaceTempView("ve72_part_tmp")
df_ts_ve72_part = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_ve72_part = df_ts_ve70_part["max(UpdatedOn)"]
print(ts_ve72_part)

# COMMAND ----------

df_part =  spark.sql("select *,'VE70' as DataSource from ve70_part_tmp union select *,'VE72' as DataSource from ve72_part_tmp")
df_part.createOrReplaceTempView("part_tmp")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_material_plant as 
# MAGIC select 
# MAGIC case when S.`MATNR` IS NULL then V.ID else S.`MATNR` end as MaterialNumber,
# MAGIC case when S.`MATNR` IS NULL then V.PLANT else S.`WERKS` end as Plant,
# MAGIC ZZHTS as HTS,
# MAGIC ZZPRODCODE as SAPPRODUCTCODE,
# MAGIC ZZPLNTGRP as PLANTGROUP,
# MAGIC S.`DISPO` as MRPController,
# MAGIC S.`PRCTR` as ProfitCenter,
# MAGIC S.`SOBSL` as SpecialProcurementType	,
# MAGIC S.`DISMM` as MRPType,
# MAGIC S.`DISGR` as MRPGroup,
# MAGIC S.`EKGRP` as PurchasingGroup	,
# MAGIC S.`MMSTA` as PlantSpecificMaterialStatus,
# MAGIC S.`DISLS` as LotSize,
# MAGIC S.`STRGR` as PlanningStrategyGroup	,
# MAGIC S.`PSTAT` as MaintenanceStatus,
# MAGIC S.`PLIFZ` as PlannedDeliveryTimeInDays,
# MAGIC S.`WEBAZ` as GoodsReceiptProcessingTimeInDays,
# MAGIC S.`PERKZ` as PeriodIndicator,
# MAGIC S.`AUSSS` as AssemblyScrapInPercent,
# MAGIC S.`MINBE` as ReorderPoint,
# MAGIC S.`EISBE` as SafetyStock,
# MAGIC S.`BSTMI` as MinimumLotSize,
# MAGIC S.`BSTMA` as MaximumLotSize,
# MAGIC S.`BSTFE` as FixedLotSize,
# MAGIC S.`BSTRF` as RoundingValueForPurchaseOrderQuantity,
# MAGIC S.`MABST` as MaximumStockLevel,
# MAGIC S.`LOSFX` as LotSizeIndependentCosts,
# MAGIC S.`BEARZ` as ProcessingTime,
# MAGIC S.`RUEZT` as SetupAndTeardownTime,
# MAGIC S.`TRANZ` as InteroperationTime,
# MAGIC S.`BASMG` as BaseQuantity,
# MAGIC S.`DZEIT` as InHouseProductionTime,
# MAGIC S.`MAXLZ` as MaximumStoragePeriod,
# MAGIC S.`UEETO` as OverdeliveryToleranceLimit,
# MAGIC S.`UNETO` as UnderdeliveryToleranceLimit,
# MAGIC S.`WZEIT` as TotalReplenishmentLeadTimInWorkdays,
# MAGIC S.`VZUSL` as SurchargeFactorForCostInPercent,
# MAGIC S.`PRFRQ` as IntervalUntilNextRecurringInspection,
# MAGIC S.`UMLMC` as StockInTransferPlantToPlant,
# MAGIC S.`LGRAD` as ServiceLevel,
# MAGIC S.`OBJID` as ObjectId,
# MAGIC S.`VRVEZ` as ShippingSetupTime,
# MAGIC S.`VBAMG` as BaseQuantityForCapacityPlanningInShipping,
# MAGIC S.`VBEAZ` as ShippingProcessingTime,
# MAGIC S.`TRAME` as StockInTransit,
# MAGIC S.`FXHOR` as PlanningTimeFence,
# MAGIC S.`VINT1` as ConsumptionPeriodBackward,
# MAGIC S.`VINT2` as ConsumptionPeriodForward,
# MAGIC S.`LOSGR` as LotSizeForProductCosting,
# MAGIC S.`KAUSF` as ComponentScrapInPercent,
# MAGIC S.`TAKZT` as TaktTime,
# MAGIC S.`CUOBJ` as InternalObjectNumber,
# MAGIC S.`VRBFK` as MultiplierForReferenceMaterialForConsumption,
# MAGIC S.`CUOBV` as InternalObjectNumberOfConfigurableMaterialForPlanning,
# MAGIC S.`RESVP` as PeriodOfAdjustmentForPlannedIndependentRequirements,
# MAGIC S.`ABFAC` as AirBouyancyFactor,
# MAGIC S.`SHZET` as SafetyTimeInWorkdays,
# MAGIC S.`VKUMC` as StockTransferSalesValueForVoMaterial,
# MAGIC S.`VKTRW` as TransitValueAtSalesPriceForValue,
# MAGIC S.`GLGMG` as TiedEmptiesStock,
# MAGIC S.`VKGLG` as SalesValueOfTiedEmptiesStock,
# MAGIC S.`DPLHO` as DeploymentHorizonInDays,
# MAGIC S.`MINLS` as MinimumLotSizeForSupplyDemandMatch,
# MAGIC S.`MAXLS` as MaximumLotSizeForSupplyDemandMatch,
# MAGIC S.`FIXLS` as FixedLotSizeForSupplyDemandMatch,
# MAGIC S.`LTINC` as LotSizeIncrementForSupplyDemandMatch,
# MAGIC S.`COMPL` as ThisFieldIsNoLongerUsed,
# MAGIC S.`MCRUE` as MardhRecAlreadyExistsForPerBeforeLastOfMardPer,
# MAGIC S.`LFMON` as CurrentPeriod,
# MAGIC S.`LFGJA` as FiscalYearOfCurrentPeriod,
# MAGIC S.`EISLO` as MinimumSafetyStock,
# MAGIC S.`BWESB` as ValuatedGoodsReceiptBlockedStock,
# MAGIC S.`PPSKZ` as IndicatorForAdvancedPlanning,
# MAGIC S.`GI_PR_TIME` as GoodsIssueProcessingTimeInDays,
# MAGIC S.`MIN_TROC` as MinimumTargetRangeOfCoverage,
# MAGIC S.`MAX_TROC` as MaximumTargetRangeOfCoverage,
# MAGIC S.`TARGET_STOCK` as TargetStock,
# MAGIC S.`/CWM/UMLMC` as StockInTransfer,
# MAGIC S.`/CWM/TRAME` as StockIn_Transit,
# MAGIC S.`/CWM/BWESB` as Valuated_GoodsReceiptBlockedStock,
# MAGIC S.`SCM_GRPRT` as GoodsReceiptProcessingTime,
# MAGIC S.`SCM_GIPRT` as GoodsIssueProcessingTime,
# MAGIC S.`SCM_SCOST` as ProductDependentStorageCosts,
# MAGIC S.`SCM_RELDT` as ReplenishmentLeadTimeInCalendarDays,
# MAGIC S.`SCM_SSPEN` as PenaltyCostsForSafetyStockViolation,
# MAGIC S.`SCM_CONHAP` as HandlingCapacityConsumptionInUnitOfMeasure,
# MAGIC S.`SCM_CONHAP_OUT` as HandlingCapacityConsumption,
# MAGIC S.`SCM_SHELF_LIFE_DUR` as LocationDependentShelfLife,
# MAGIC S.`SCM_MATURITY_DUR` as LocationDependentMaturationTime,
# MAGIC S.`SCM_SHLF_LFE_REQ_MIN` as MinimumShelfLifeRequiredLocationDependent,
# MAGIC S.`SCM_SHLF_LFE_REQ_MAX` as MaximumShelfLifeRequiredLocationDependent,
# MAGIC S.`SCM_REORD_DUR` as ReorderDaysSupply,
# MAGIC S.`SCM_TARGET_DUR` as TargetDaysSupplyInWorkdays,
# MAGIC S.`SCM_PEG_PAST_ALERT` as AlertThresholdForDelayedReceipts,
# MAGIC S.`SCM_PEG_FUTURE_ALERT` as AlertThresholdForEarlyReceipts,
# MAGIC S.`SCM_PEG_STRATEGY` as PeggingStrategyForDynamicPegging,
# MAGIC S.`SCM_PRIO` as PriorityOfProduct,
# MAGIC S.`SCM_MIN_PASS_AMOUNT` as MinimumPassingAmountForContinuousIOPegging,
# MAGIC S.`ESPPFLG` as UsageInExtendedServicePartsPlanning,
# MAGIC S.`SCM_THRUPUT_TIME` as ThroughputTime,
# MAGIC S.`SCM_SAFTY_V` as SafetyStockForVirtualChildLocation,
# MAGIC S.`SCM_PPSAFTYSTK` as SafetyStockAtParentLocation,
# MAGIC S.`SCM_PPSAFTYSTK_V` as SafetyStockOfParentLocationVirtualChildLocation,
# MAGIC S.`SCM_REPSAFTY` as RepairSafetyStock,
# MAGIC S.`SCM_REPSAFTY_V` as RepairSafetyStockForVirtualChildLocation,
# MAGIC S.`SCM_REORD_V` as ReorderPointForVirtualChildLocation,
# MAGIC S.`SCM_MAXSTOCK_V` as MaximumStockLevelForVirtualChildLocations,
# MAGIC S.`SCM_SCOST_PRCNT` as CostFactorForStockholdingCosts,
# MAGIC S.`SCM_PROC_COST` as ProcurementCostsForProduct,
# MAGIC S.`SCM_NDCOSTWE` as GoodsReceivingCosts,
# MAGIC S.`SCM_NDCOSTWA` as GoodsIssueCosts,
# MAGIC S.`SCM_CONINP` as ConsumptionOfStorageCapacityPerUnitOfMaterial,
# MAGIC S.`/SAPMP/TOLPRPL` as PercentageTolerancePlus,
# MAGIC S.`/SAPMP/TOLPRMI` as PercentageToleranceMinus,
# MAGIC S.`ZZECCN` as ECCN,
# MAGIC S.`ZZECCN_YR` as Edition,
# MAGIC case when S.`MATNR` IS NULL then case when V.PURCHASED = 'Y' then 'F' else V.PURCHASED end  else S.`BESKZ` end as ProcurementType,
# MAGIC V.PRODUCT_CODE as ProductCode,
# MAGIC V.COMMODITY_CODE as CommodityCode,
# MAGIC V.PREF_VENDOR_ID as PrefVendorID,
# MAGIC V.BUYER_USER_ID as BuyerUserID,
# MAGIC V.ABC_CODE as ABCCode,
# MAGIC V.INVENTORY_LOCKED as InventoryLocked,
# MAGIC V.STATUS as Status,
# MAGIC V.REVISION_ID as RevisionID,
# MAGIC now() as UpdatedOn,
# MAGIC case when S.`MATNR` IS NULL then V.DataSource else 'SAP' end as DataSource
# MAGIC from marc_tmp as S
# MAGIC full join part_tmp as V
# MAGIC on S.MATNR = V.ID
# MAGIC and S.WERKS = V.Plant
# MAGIC and 'SAP' = V.DataSource

# COMMAND ----------

df_merge = spark.sql("select * from merge_material_plant")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_merge.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_material_plant')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.material_plant as T 
# MAGIC USING stg_merge_material_plant as S 
# MAGIC ON T.MaterialNumber = S.MaterialNumber
# MAGIC and T.Plant = S.Plant
# MAGIC and T.DATASOURCE = S.DATASOURCE
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.MaterialNumber = S.MaterialNumber,
# MAGIC T.Plant = S.Plant,
# MAGIC T.HTS = T.HTS,
# MAGIC T.SAPPRODUCTCODE = S.SAPPRODUCTCODE,
# MAGIC T.PLANTGROUP = S.PLANTGROUP,
# MAGIC T.MRPController = S.MRPController,
# MAGIC T.ProfitCenter = S.ProfitCenter,
# MAGIC T.SpecialProcurementType = S.SpecialProcurementType,
# MAGIC T.MRPType = S.MRPType,
# MAGIC T.MRPGroup = S.MRPGroup,
# MAGIC T.PurchasingGroup = S.PurchasingGroup,
# MAGIC T.PlantSpecificMaterialStatus = S.PlantSpecificMaterialStatus,
# MAGIC T.LotSize = S.LotSize,
# MAGIC T.PlanningStrategyGroup = S.PlanningStrategyGroup,
# MAGIC T.MaintenanceStatus = S.MaintenanceStatus,
# MAGIC T.PlannedDeliveryTimeInDays = S.PlannedDeliveryTimeInDays,
# MAGIC T.GoodsReceiptProcessingTimeInDays = S.GoodsReceiptProcessingTimeInDays,
# MAGIC T.PeriodIndicator = S.PeriodIndicator,
# MAGIC T.AssemblyScrapInPercent = S.AssemblyScrapInPercent,
# MAGIC T.ReorderPoint = S.ReorderPoint,
# MAGIC T.SafetyStock = S.SafetyStock,
# MAGIC T.MinimumLotSize = S.MinimumLotSize,
# MAGIC T.MaximumLotSize = S.MaximumLotSize,
# MAGIC T.FixedLotSize = S.FixedLotSize,
# MAGIC T.RoundingValueForPurchaseOrderQuantity = S.RoundingValueForPurchaseOrderQuantity,
# MAGIC T.MaximumStockLevel = S.MaximumStockLevel,
# MAGIC T.LotSizeIndependentCosts = S.LotSizeIndependentCosts,
# MAGIC T.ProcessingTime = S.ProcessingTime,
# MAGIC T.SetupAndTeardownTime = S.SetupAndTeardownTime,
# MAGIC T.InteroperationTime = S.InteroperationTime,
# MAGIC T.BaseQuantity = S.BaseQuantity,
# MAGIC T.InHouseProductionTime = S.InHouseProductionTime,
# MAGIC T.MaximumStoragePeriod = S.MaximumStoragePeriod,
# MAGIC T.OverdeliveryToleranceLimit = S.OverdeliveryToleranceLimit,
# MAGIC T.UnderdeliveryToleranceLimit = S.UnderdeliveryToleranceLimit,
# MAGIC T.TotalReplenishmentLeadTimInWorkdays = S.TotalReplenishmentLeadTimInWorkdays,
# MAGIC T.SurchargeFactorForCostInPercent = S.SurchargeFactorForCostInPercent,
# MAGIC T.IntervalUntilNextRecurringInspection = S.IntervalUntilNextRecurringInspection,
# MAGIC T.StockInTransferPlantToPlant = S.StockInTransferPlantToPlant,
# MAGIC T.ServiceLevel = S.ServiceLevel,
# MAGIC T.ObjectId = S.ObjectId,
# MAGIC T.ShippingSetupTime = S.ShippingSetupTime,
# MAGIC T.BaseQuantityForCapacityPlanningInShipping = S.BaseQuantityForCapacityPlanningInShipping,
# MAGIC T.ShippingProcessingTime = S.ShippingProcessingTime,
# MAGIC T.StockInTransit = S.StockInTransit,
# MAGIC T.PlanningTimeFence = S.PlanningTimeFence,
# MAGIC T.ConsumptionPeriodBackward = S.ConsumptionPeriodBackward,
# MAGIC T.ConsumptionPeriodForward = S.ConsumptionPeriodForward,
# MAGIC T.LotSizeForProductCosting = S.LotSizeForProductCosting,
# MAGIC T.ComponentScrapInPercent = S.ComponentScrapInPercent,
# MAGIC T.TaktTime = S.TaktTime,
# MAGIC T.InternalObjectNumber = S.InternalObjectNumber,
# MAGIC T.MultiplierForReferenceMaterialForConsumption = S.MultiplierForReferenceMaterialForConsumption,
# MAGIC T.InternalObjectNumberOfConfigurableMaterialForPlanning = S.InternalObjectNumberOfConfigurableMaterialForPlanning,
# MAGIC T.PeriodOfAdjustmentForPlannedIndependentRequirements = S.PeriodOfAdjustmentForPlannedIndependentRequirements,
# MAGIC T.AirBouyancyFactor = S.AirBouyancyFactor,
# MAGIC T.SafetyTimeInWorkdays = S.SafetyTimeInWorkdays,
# MAGIC T.StockTransferSalesValueForVoMaterial = S.StockTransferSalesValueForVoMaterial,
# MAGIC T.TransitValueAtSalesPriceForValue = S.TransitValueAtSalesPriceForValue,
# MAGIC T.TiedEmptiesStock = S.TiedEmptiesStock,
# MAGIC T.SalesValueOfTiedEmptiesStock = S.SalesValueOfTiedEmptiesStock,
# MAGIC T.DeploymentHorizonInDays = S.DeploymentHorizonInDays,
# MAGIC T.MinimumLotSizeForSupplyDemandMatch = S.MinimumLotSizeForSupplyDemandMatch,
# MAGIC T.MaximumLotSizeForSupplyDemandMatch = S.MaximumLotSizeForSupplyDemandMatch,
# MAGIC T.FixedLotSizeForSupplyDemandMatch = S.FixedLotSizeForSupplyDemandMatch,
# MAGIC T.LotSizeIncrementForSupplyDemandMatch = S.LotSizeIncrementForSupplyDemandMatch,
# MAGIC T.ThisFieldIsNoLongerUsed = S.ThisFieldIsNoLongerUsed,
# MAGIC T.MardhRecAlreadyExistsForPerBeforeLastOfMardPer = S.MardhRecAlreadyExistsForPerBeforeLastOfMardPer,
# MAGIC T.CurrentPeriod = S.CurrentPeriod,
# MAGIC T.FiscalYearOfCurrentPeriod = S.FiscalYearOfCurrentPeriod,
# MAGIC T.MinimumSafetyStock = S.MinimumSafetyStock,
# MAGIC T.ValuatedGoodsReceiptBlockedStock = S.ValuatedGoodsReceiptBlockedStock,
# MAGIC T.IndicatorForAdvancedPlanning = S.IndicatorForAdvancedPlanning,
# MAGIC T.GoodsIssueProcessingTimeInDays = S.GoodsIssueProcessingTimeInDays,
# MAGIC T.MinimumTargetRangeOfCoverage = S.MinimumTargetRangeOfCoverage,
# MAGIC T.MaximumTargetRangeOfCoverage = S.MaximumTargetRangeOfCoverage,
# MAGIC T.TargetStock = S.TargetStock,
# MAGIC T.StockInTransfer = S.StockInTransfer,
# MAGIC T.StockIn_Transit = S.StockIn_Transit,
# MAGIC T.Valuated_GoodsReceiptBlockedStock = S.Valuated_GoodsReceiptBlockedStock,
# MAGIC T.GoodsReceiptProcessingTime = S.GoodsReceiptProcessingTime,
# MAGIC T.GoodsIssueProcessingTime = S.GoodsIssueProcessingTime,
# MAGIC T.ProductDependentStorageCosts = S.ProductDependentStorageCosts,
# MAGIC T.ReplenishmentLeadTimeInCalendarDays = S.ReplenishmentLeadTimeInCalendarDays,
# MAGIC T.PenaltyCostsForSafetyStockViolation = S.PenaltyCostsForSafetyStockViolation,
# MAGIC T.HandlingCapacityConsumptionInUnitOfMeasure = S.HandlingCapacityConsumptionInUnitOfMeasure,
# MAGIC T.HandlingCapacityConsumption = S.HandlingCapacityConsumption,
# MAGIC T.LocationDependentShelfLife = S.LocationDependentShelfLife,
# MAGIC T.LocationDependentMaturationTime = S.LocationDependentMaturationTime,
# MAGIC T.MinimumShelfLifeRequiredLocationDependent = S.MinimumShelfLifeRequiredLocationDependent,
# MAGIC T.MaximumShelfLifeRequiredLocationDependent = S.MaximumShelfLifeRequiredLocationDependent,
# MAGIC T.ReorderDaysSupply = S.ReorderDaysSupply,
# MAGIC T.TargetDaysSupplyInWorkdays = S.TargetDaysSupplyInWorkdays,
# MAGIC T.AlertThresholdForDelayedReceipts = S.AlertThresholdForDelayedReceipts,
# MAGIC T.AlertThresholdForEarlyReceipts = S.AlertThresholdForEarlyReceipts,
# MAGIC T.PeggingStrategyForDynamicPegging = S.PeggingStrategyForDynamicPegging,
# MAGIC T.PriorityOfProduct = S.PriorityOfProduct,
# MAGIC T.MinimumPassingAmountForContinuousIOPegging = S.MinimumPassingAmountForContinuousIOPegging,
# MAGIC T.UsageInExtendedServicePartsPlanning = S.UsageInExtendedServicePartsPlanning,
# MAGIC T.ThroughputTime = S.ThroughputTime,
# MAGIC T.SafetyStockForVirtualChildLocation = S.SafetyStockForVirtualChildLocation,
# MAGIC T.SafetyStockAtParentLocation = S.SafetyStockAtParentLocation,
# MAGIC T.SafetyStockOfParentLocationVirtualChildLocation = S.SafetyStockOfParentLocationVirtualChildLocation,
# MAGIC T.RepairSafetyStock = S.RepairSafetyStock,
# MAGIC T.RepairSafetyStockForVirtualChildLocation = S.RepairSafetyStockForVirtualChildLocation,
# MAGIC T.ReorderPointForVirtualChildLocation = S.ReorderPointForVirtualChildLocation,
# MAGIC T.MaximumStockLevelForVirtualChildLocations = S.MaximumStockLevelForVirtualChildLocations,
# MAGIC T.CostFactorForStockholdingCosts = S.CostFactorForStockholdingCosts,
# MAGIC T.ProcurementCostsForProduct = S.ProcurementCostsForProduct,
# MAGIC T.GoodsReceivingCosts = S.GoodsReceivingCosts,
# MAGIC T.GoodsIssueCosts = S.GoodsIssueCosts,
# MAGIC T.ConsumptionOfStorageCapacityPerUnitOfMaterial = S.ConsumptionOfStorageCapacityPerUnitOfMaterial,
# MAGIC T.PercentageTolerancePlus = S.PercentageTolerancePlus,
# MAGIC T.PercentageToleranceMinus = S.PercentageToleranceMinus,
# MAGIC T.ECCN = S.ECCN,
# MAGIC T.Edition = S.Edition,
# MAGIC T.ProcurementType = S.ProcurementType,
# MAGIC T.ProductCode = S.ProductCode,
# MAGIC T.CommodityCode = S.CommodityCode,
# MAGIC T.PrefVendorID = S.PrefVendorID,
# MAGIC T.BuyerUserID = S.BuyerUserID,
# MAGIC T.ABCCode = S.ABCCode,
# MAGIC T.InventoryLocked = S.InventoryLocked,
# MAGIC T.Status = S.Status,
# MAGIC T.RevisionID = S.RevisionID,
# MAGIC T.UpdatedOn = now(),
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC MaterialNumber,
# MAGIC Plant,
# MAGIC HTS,
# MAGIC SAPPRODUCTCODE,
# MAGIC PLANTGROUP,
# MAGIC MRPController,
# MAGIC ProfitCenter,
# MAGIC SpecialProcurementType,
# MAGIC MRPType,
# MAGIC MRPGroup,
# MAGIC PurchasingGroup,
# MAGIC PlantSpecificMaterialStatus,
# MAGIC LotSize,
# MAGIC PlanningStrategyGroup,
# MAGIC MaintenanceStatus,
# MAGIC PlannedDeliveryTimeInDays,
# MAGIC GoodsReceiptProcessingTimeInDays,
# MAGIC PeriodIndicator,
# MAGIC AssemblyScrapInPercent,
# MAGIC ReorderPoint,
# MAGIC SafetyStock,
# MAGIC MinimumLotSize,
# MAGIC MaximumLotSize,
# MAGIC FixedLotSize,
# MAGIC RoundingValueForPurchaseOrderQuantity,
# MAGIC MaximumStockLevel,
# MAGIC LotSizeIndependentCosts,
# MAGIC ProcessingTime,
# MAGIC SetupAndTeardownTime,
# MAGIC InteroperationTime,
# MAGIC BaseQuantity,
# MAGIC InHouseProductionTime,
# MAGIC MaximumStoragePeriod,
# MAGIC OverdeliveryToleranceLimit,
# MAGIC UnderdeliveryToleranceLimit,
# MAGIC TotalReplenishmentLeadTimInWorkdays,
# MAGIC SurchargeFactorForCostInPercent,
# MAGIC IntervalUntilNextRecurringInspection,
# MAGIC StockInTransferPlantToPlant,
# MAGIC ServiceLevel,
# MAGIC ObjectId,
# MAGIC ShippingSetupTime,
# MAGIC BaseQuantityForCapacityPlanningInShipping,
# MAGIC ShippingProcessingTime,
# MAGIC StockInTransit,
# MAGIC PlanningTimeFence,
# MAGIC ConsumptionPeriodBackward,
# MAGIC ConsumptionPeriodForward,
# MAGIC LotSizeForProductCosting,
# MAGIC ComponentScrapInPercent,
# MAGIC TaktTime,
# MAGIC InternalObjectNumber,
# MAGIC MultiplierForReferenceMaterialForConsumption,
# MAGIC InternalObjectNumberOfConfigurableMaterialForPlanning,
# MAGIC PeriodOfAdjustmentForPlannedIndependentRequirements,
# MAGIC AirBouyancyFactor,
# MAGIC SafetyTimeInWorkdays,
# MAGIC StockTransferSalesValueForVoMaterial,
# MAGIC TransitValueAtSalesPriceForValue,
# MAGIC TiedEmptiesStock,
# MAGIC SalesValueOfTiedEmptiesStock,
# MAGIC DeploymentHorizonInDays,
# MAGIC MinimumLotSizeForSupplyDemandMatch,
# MAGIC MaximumLotSizeForSupplyDemandMatch,
# MAGIC FixedLotSizeForSupplyDemandMatch,
# MAGIC LotSizeIncrementForSupplyDemandMatch,
# MAGIC ThisFieldIsNoLongerUsed,
# MAGIC MardhRecAlreadyExistsForPerBeforeLastOfMardPer,
# MAGIC CurrentPeriod,
# MAGIC FiscalYearOfCurrentPeriod,
# MAGIC MinimumSafetyStock,
# MAGIC ValuatedGoodsReceiptBlockedStock,
# MAGIC IndicatorForAdvancedPlanning,
# MAGIC GoodsIssueProcessingTimeInDays,
# MAGIC MinimumTargetRangeOfCoverage,
# MAGIC MaximumTargetRangeOfCoverage,
# MAGIC TargetStock,
# MAGIC StockInTransfer,
# MAGIC StockIn_Transit,
# MAGIC Valuated_GoodsReceiptBlockedStock,
# MAGIC GoodsReceiptProcessingTime,
# MAGIC GoodsIssueProcessingTime,
# MAGIC ProductDependentStorageCosts,
# MAGIC ReplenishmentLeadTimeInCalendarDays,
# MAGIC PenaltyCostsForSafetyStockViolation,
# MAGIC HandlingCapacityConsumptionInUnitOfMeasure,
# MAGIC HandlingCapacityConsumption,
# MAGIC LocationDependentShelfLife,
# MAGIC LocationDependentMaturationTime,
# MAGIC MinimumShelfLifeRequiredLocationDependent,
# MAGIC MaximumShelfLifeRequiredLocationDependent,
# MAGIC ReorderDaysSupply,
# MAGIC TargetDaysSupplyInWorkdays,
# MAGIC AlertThresholdForDelayedReceipts,
# MAGIC AlertThresholdForEarlyReceipts,
# MAGIC PeggingStrategyForDynamicPegging,
# MAGIC PriorityOfProduct,
# MAGIC MinimumPassingAmountForContinuousIOPegging,
# MAGIC UsageInExtendedServicePartsPlanning,
# MAGIC ThroughputTime,
# MAGIC SafetyStockForVirtualChildLocation,
# MAGIC SafetyStockAtParentLocation,
# MAGIC SafetyStockOfParentLocationVirtualChildLocation,
# MAGIC RepairSafetyStock,
# MAGIC RepairSafetyStockForVirtualChildLocation,
# MAGIC ReorderPointForVirtualChildLocation,
# MAGIC MaximumStockLevelForVirtualChildLocations,
# MAGIC CostFactorForStockholdingCosts,
# MAGIC ProcurementCostsForProduct,
# MAGIC GoodsReceivingCosts,
# MAGIC GoodsIssueCosts,
# MAGIC ConsumptionOfStorageCapacityPerUnitOfMaterial,
# MAGIC PercentageTolerancePlus,
# MAGIC PercentageToleranceMinus,
# MAGIC ECCN,
# MAGIC Edition,
# MAGIC ProcurementType,
# MAGIC ProductCode,
# MAGIC CommodityCode,
# MAGIC PrefVendorID,
# MAGIC BuyerUserID,
# MAGIC ABCCode,
# MAGIC InventoryLocked,
# MAGIC Status,
# MAGIC RevisionID,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.MaterialNumber,
# MAGIC S.Plant,
# MAGIC S.HTS,
# MAGIC S.SAPPRODUCTCODE,
# MAGIC S.PLANTGROUP,
# MAGIC S.MRPController,
# MAGIC S.ProfitCenter,
# MAGIC S.SpecialProcurementType,
# MAGIC S.MRPType,
# MAGIC S.MRPGroup,
# MAGIC S.PurchasingGroup,
# MAGIC S.PlantSpecificMaterialStatus,
# MAGIC S.LotSize,
# MAGIC S.PlanningStrategyGroup,
# MAGIC S.MaintenanceStatus,
# MAGIC S.PlannedDeliveryTimeInDays,
# MAGIC S.GoodsReceiptProcessingTimeInDays,
# MAGIC S.PeriodIndicator,
# MAGIC S.AssemblyScrapInPercent,
# MAGIC S.ReorderPoint,
# MAGIC S.SafetyStock,
# MAGIC S.MinimumLotSize,
# MAGIC S.MaximumLotSize,
# MAGIC S.FixedLotSize,
# MAGIC S.RoundingValueForPurchaseOrderQuantity,
# MAGIC S.MaximumStockLevel,
# MAGIC S.LotSizeIndependentCosts,
# MAGIC S.ProcessingTime,
# MAGIC S.SetupAndTeardownTime,
# MAGIC S.InteroperationTime,
# MAGIC S.BaseQuantity,
# MAGIC S.InHouseProductionTime,
# MAGIC S.MaximumStoragePeriod,
# MAGIC S.OverdeliveryToleranceLimit,
# MAGIC S.UnderdeliveryToleranceLimit,
# MAGIC S.TotalReplenishmentLeadTimInWorkdays,
# MAGIC S.SurchargeFactorForCostInPercent,
# MAGIC S.IntervalUntilNextRecurringInspection,
# MAGIC S.StockInTransferPlantToPlant,
# MAGIC S.ServiceLevel,
# MAGIC S.ObjectId,
# MAGIC S.ShippingSetupTime,
# MAGIC S.BaseQuantityForCapacityPlanningInShipping,
# MAGIC S.ShippingProcessingTime,
# MAGIC S.StockInTransit,
# MAGIC S.PlanningTimeFence,
# MAGIC S.ConsumptionPeriodBackward,
# MAGIC S.ConsumptionPeriodForward,
# MAGIC S.LotSizeForProductCosting,
# MAGIC S.ComponentScrapInPercent,
# MAGIC S.TaktTime,
# MAGIC S.InternalObjectNumber,
# MAGIC S.MultiplierForReferenceMaterialForConsumption,
# MAGIC S.InternalObjectNumberOfConfigurableMaterialForPlanning,
# MAGIC S.PeriodOfAdjustmentForPlannedIndependentRequirements,
# MAGIC S.AirBouyancyFactor,
# MAGIC S.SafetyTimeInWorkdays,
# MAGIC S.StockTransferSalesValueForVoMaterial,
# MAGIC S.TransitValueAtSalesPriceForValue,
# MAGIC S.TiedEmptiesStock,
# MAGIC S.SalesValueOfTiedEmptiesStock,
# MAGIC S.DeploymentHorizonInDays,
# MAGIC S.MinimumLotSizeForSupplyDemandMatch,
# MAGIC S.MaximumLotSizeForSupplyDemandMatch,
# MAGIC S.FixedLotSizeForSupplyDemandMatch,
# MAGIC S.LotSizeIncrementForSupplyDemandMatch,
# MAGIC S.ThisFieldIsNoLongerUsed,
# MAGIC S.MardhRecAlreadyExistsForPerBeforeLastOfMardPer,
# MAGIC S.CurrentPeriod,
# MAGIC S.FiscalYearOfCurrentPeriod,
# MAGIC S.MinimumSafetyStock,
# MAGIC S.ValuatedGoodsReceiptBlockedStock,
# MAGIC S.IndicatorForAdvancedPlanning,
# MAGIC S.GoodsIssueProcessingTimeInDays,
# MAGIC S.MinimumTargetRangeOfCoverage,
# MAGIC S.MaximumTargetRangeOfCoverage,
# MAGIC S.TargetStock,
# MAGIC S.StockInTransfer,
# MAGIC S.StockIn_Transit,
# MAGIC S.Valuated_GoodsReceiptBlockedStock,
# MAGIC S.GoodsReceiptProcessingTime,
# MAGIC S.GoodsIssueProcessingTime,
# MAGIC S.ProductDependentStorageCosts,
# MAGIC S.ReplenishmentLeadTimeInCalendarDays,
# MAGIC S.PenaltyCostsForSafetyStockViolation,
# MAGIC S.HandlingCapacityConsumptionInUnitOfMeasure,
# MAGIC S.HandlingCapacityConsumption,
# MAGIC S.LocationDependentShelfLife,
# MAGIC S.LocationDependentMaturationTime,
# MAGIC S.MinimumShelfLifeRequiredLocationDependent,
# MAGIC S.MaximumShelfLifeRequiredLocationDependent,
# MAGIC S.ReorderDaysSupply,
# MAGIC S.TargetDaysSupplyInWorkdays,
# MAGIC S.AlertThresholdForDelayedReceipts,
# MAGIC S.AlertThresholdForEarlyReceipts,
# MAGIC S.PeggingStrategyForDynamicPegging,
# MAGIC S.PriorityOfProduct,
# MAGIC S.MinimumPassingAmountForContinuousIOPegging,
# MAGIC S.UsageInExtendedServicePartsPlanning,
# MAGIC S.ThroughputTime,
# MAGIC S.SafetyStockForVirtualChildLocation,
# MAGIC S.SafetyStockAtParentLocation,
# MAGIC S.SafetyStockOfParentLocationVirtualChildLocation,
# MAGIC S.RepairSafetyStock,
# MAGIC S.RepairSafetyStockForVirtualChildLocation,
# MAGIC S.ReorderPointForVirtualChildLocation,
# MAGIC S.MaximumStockLevelForVirtualChildLocations,
# MAGIC S.CostFactorForStockholdingCosts,
# MAGIC S.ProcurementCostsForProduct,
# MAGIC S.GoodsReceivingCosts,
# MAGIC S.GoodsIssueCosts,
# MAGIC S.ConsumptionOfStorageCapacityPerUnitOfMaterial,
# MAGIC S.PercentageTolerancePlus,
# MAGIC S.PercentageToleranceMinus,
# MAGIC S.ECCN,
# MAGIC S.Edition,
# MAGIC S.ProcurementType,
# MAGIC S.ProductCode,
# MAGIC S.CommodityCode,
# MAGIC S.PrefVendorID,
# MAGIC S.BuyerUserID,
# MAGIC S.ABCCode,
# MAGIC S.InventoryLocked,
# MAGIC S.Status,
# MAGIC S.RevisionID,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_marc != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'MARC' and TargetLayerTableName ='Material_Plant' and DatabaseName = 'S42'".format(ts_marc))
    
if(ts_ve70_part != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PART' and TargetLayerTableName ='Material_Plant' and DatabaseName = 'VE70'".format(ts_ve70_part))
    
if(ts_ve72_part != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'PART' and TargetLayerTableName ='Material_Plant' and DatabaseName = 'VE72'".format(ts_ve72_part))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.material_plant where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Plant' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_material_plant = ts_sf["max(UpdatedOn)"]
print(ts_material_plant)

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
  .option("dbtable", "material_plant") \
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
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC  
# MAGIC Utils.runQuery(options, """call SP_LOAD_MATERIALPLANT()""")

# COMMAND ----------

if(ts_material_plant != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Material_Plant' and DatabaseName = 'FEDW'".format(ts_material_plant))

# COMMAND ----------

|
