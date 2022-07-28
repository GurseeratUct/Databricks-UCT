# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Country'
table_name = 'T005' 
read_format = 'delta'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
stage_table = 'stg_'+table_name
database_name = 'FEDW'

# COMMAND ----------

col_names = ['MANDT',
'LAND1',
'LANDK',
'LNPLZ',
'PRPLZ',
'ADDRS',
'XPLZS',
'XPLPF',
'SPRAS',
'XLAND',
'XADDR',
'NMFMT',
'XREGS',
'XPLST',
'INTCA',
'INTCA3',
'INTCN3',
'XEGLD',
'XSKFN',
'XMWSN',
'LNBKN',
'PRBKN',
'LNBLZ',
'PRBLZ',
'LNPSK',
'PRPSK',
'XPRBK',
'BNKEY',
'LNBKS',
'PRBKS',
'XPRSO',
'PRUIN',
'UINLN',
'LNST1',
'PRST1',
'LNST2',
'PRST2',
'LNST3',
'PRST3',
'LNST4',
'PRST4',
'LNST5',
'PRST5',
'LANDD',
'KALSM',
'LANDA',
'WECHF',
'LKVRZ',
'INTCN',
'XDEZP',
'DATFM',
'CURIN',
'CURHA',
'WAERS',
'KURST',
'AFAPL',
'GWGWRT',
'UMRWRT',
'KZRBWB',
'XANZUM',
'CTNCONCEPT',
'KZSRV',
'XXINVE',
'XGCCV',
'SUREG',
'LandingFileTimeStamp',
'UpdatedOn',
'DataSource']

# COMMAND ----------

df = spark.sql("select * from S42.T005 where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'T005')")

# COMMAND ----------

df_ts = df.agg({"UpdatedOn": "max"}).collect()[0]
ts = df_ts["max(UpdatedOn)"]
print(ts)

# COMMAND ----------

df_select_col = df.select(col_names)

# COMMAND ----------

df_rename =    df_select_col.withColumnRenamed('MANDT','Client') \
                            .withColumnRenamed('LAND1','CountryKey') \
                            .withColumnRenamed('LANDK','VehicleCountryKey') \
                            .withColumnRenamed('LNPLZ','PostalCodeLength') \
                            .withColumnRenamed('PRPLZ','RuleForThePostalCodeFieldCheck') \
                            .withColumnRenamed('ADDRS','FormattingRoutineKeyForPrintingAddresses') \
                            .withColumnRenamed('XPLZS','StreetAddressPostalCodeReqd') \
                            .withColumnRenamed('XPLPF','POBoxPostalCodeReqd') \
                            .withColumnRenamed('SPRAS','LanguageKey') \
                            .withColumnRenamed('XLAND','CountryVersionFlag') \
                            .withColumnRenamed('XADDR','CountryNameInForeignAddresses') \
                            .withColumnRenamed('NMFMT','StandardNameFormat') \
                            .withColumnRenamed('XREGS','CityFileAddressCheck') \
                            .withColumnRenamed('XPLST','StreetSpecificPostalCode') \
                            .withColumnRenamed('INTCA','CountryISOcode') \
                            .withColumnRenamed('INTCA3','ISOCountryCode3char') \
                            .withColumnRenamed('INTCN3','ISOCountryCodeNumeric3Char') \
                            .withColumnRenamed('XEGLD','EuropeanUnionMember') \
                            .withColumnRenamed('XSKFN','DiscountBaseAmount') \
                            .withColumnRenamed('XMWSN','BaseAmountForTax') \
                            .withColumnRenamed('LNBKN','BankAccountNumberLength') \
                            .withColumnRenamed('PRBKN','RuleForCheckingBankAccountNo') \
                            .withColumnRenamed('LNBLZ','BankNumberLength') \
                            .withColumnRenamed('PRBLZ','RuleForCheckingBankNumber') \
                            .withColumnRenamed('LNPSK','PostOfficeBankCurrentAccountNumber') \
                            .withColumnRenamed('PRPSK','RuleForCheckingPostalCheckAccountNumber') \
                            .withColumnRenamed('XPRBK','UseCheckModuleForBankField') \
                            .withColumnRenamed('BNKEY','NameOfTheBankKey') \
                            .withColumnRenamed('LNBKS','LengthOfBankKey') \
                            .withColumnRenamed('PRBKS','RuleForCheckingBankKeyField') \
                            .withColumnRenamed('XPRSO','UseCheckModuleForTaxFields') \
                            .withColumnRenamed('PRUIN','RuleForCheckingVATRegistrationNumberField') \
                            .withColumnRenamed('UINLN','VATRegistrationNumberLength') \
                            .withColumnRenamed('LNST1','PermittedInputLengthForTaxNumber1') \
                            .withColumnRenamed('PRST1','RuleForCheckingTaxCode1') \
                            .withColumnRenamed('LNST2','PermittedInputLengthForTaxNumber2') \
                            .withColumnRenamed('PRST2','RuleForCheckingTaxCode2') \
                            .withColumnRenamed('LNST3','PermittedInputLengthForTaxNumber3') \
                            .withColumnRenamed('PRST3','RuleForCheckingTaxCode3') \
                            .withColumnRenamed('LNST4','PermittedInputLengthForTaxNumber4') \
                            .withColumnRenamed('PRST4','RuleForCheckingTaxCode4') \
                            .withColumnRenamed('LNST5','PermittedInputLengthForTaxNumber5') \
                            .withColumnRenamed('PRST5','RuleForCheckingTaxCode5') \
                            .withColumnRenamed('LANDD','Nationality') \
                            .withColumnRenamed('KALSM','Procedure') \
                            .withColumnRenamed('LANDA','AlternativeCountryKey') \
                            .withColumnRenamed('WECHF','BillOfExchangePaymentPeriod') \
                            .withColumnRenamed('LKVRZ','ShortNameForForeignTradeStatistics') \
                            .withColumnRenamed('INTCN','IntrastatCode') \
                            .withColumnRenamed('XDEZP','PointFormat') \
                            .withColumnRenamed('DATFM','DateFormat') \
                            .withColumnRenamed('CURIN','CurrencyKeyOfTheIndexBasedCurrency') \
                            .withColumnRenamed('CURHA','CurrencyKeyOfTheHardCurrency') \
                            .withColumnRenamed('WAERS','CountryCurrency') \
                            .withColumnRenamed('KURST','ExchangeRateTypeForTranslationIntoCountryCurrency') \
                            .withColumnRenamed('AFAPL','ChartOfDepreciatonForAssetValuation') \
                            .withColumnRenamed('GWGWRT','MaximumLowValueAssetAmount') \
                            .withColumnRenamed('UMRWRT','NetBookValueForChangeoverOfDepreciationMethod') \
                            .withColumnRenamed('KZRBWB','IndicatorPostNetBookValueForRetirement') \
                            .withColumnRenamed('XANZUM','IndicatorTransferDownPayments') \
                            .withColumnRenamed('CTNCONCEPT','WithholdingTaxCertificateNumbering') \
                            .withColumnRenamed('KZSRV','TaxesAtIndividualServiceLevel') \
                            .withColumnRenamed('XXINVE','DisplayCapitalGoodsIndicator') \
                            .withColumnRenamed('XGCCV','GCCCountriesMember') \
                            .withColumnRenamed('SUREG','SuperRegionPerCountry') \
                            .withColumnRenamed('LandingFileTimeStamp','LandingFileTimeStamp') \
                            .withColumnRenamed('UpdatedOn','UpdatedOn') \
                            .withColumn('DataSource',lit('SAP'))

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df_rename.write.format(write_format).mode("overwrite").save(write_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_rename.createOrReplaceTempView('stg_country')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO fedw.country  S
# MAGIC USING (select * from (select row_number() OVER (PARTITION BY Client,CountryKey ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_country)A where A.rn = 1 ) as T 
# MAGIC ON 
# MAGIC S.Client = T.Client 
# MAGIC and 
# MAGIC S.CountryKey = T.CountryKey
# MAGIC WHEN  MATCHED 
# MAGIC THEN UPDATE SET
# MAGIC S.Client = T.Client,
# MAGIC S.CountryKey = T.CountryKey,
# MAGIC S.VehicleCountryKey = T.VehicleCountryKey,
# MAGIC S.PostalCodeLength = T.PostalCodeLength,
# MAGIC S.RuleForThePostalCodeFieldCheck = T.RuleForThePostalCodeFieldCheck,
# MAGIC S.FormattingRoutineKeyForPrintingAddresses = T.FormattingRoutineKeyForPrintingAddresses,
# MAGIC S.StreetAddressPostalCodeReqd = T.StreetAddressPostalCodeReqd,
# MAGIC S.POBoxPostalCodeReqd = T.POBoxPostalCodeReqd,
# MAGIC S.LanguageKey = T.LanguageKey,
# MAGIC S.CountryVersionFlag = T.CountryVersionFlag,
# MAGIC S.CountryNameInForeignAddresses = T.CountryNameInForeignAddresses,
# MAGIC S.StandardNameFormat = T.StandardNameFormat,
# MAGIC S.CityFileAddressCheck = T.CityFileAddressCheck,
# MAGIC S.StreetSpecificPostalCode = T.StreetSpecificPostalCode,
# MAGIC S.CountryISOcode = T.CountryISOcode,
# MAGIC S.ISOCountryCode3char = T.ISOCountryCode3char,
# MAGIC S.ISOCountryCodeNumeric3Char = T.ISOCountryCodeNumeric3Char,
# MAGIC S.EuropeanUnionMember = T.EuropeanUnionMember,
# MAGIC S.DiscountBaseAmount = T.DiscountBaseAmount,
# MAGIC S.BaseAmountForTax = T.BaseAmountForTax,
# MAGIC S.BankAccountNumberLength = T.BankAccountNumberLength,
# MAGIC S.RuleForCheckingBankAccountNo = T.RuleForCheckingBankAccountNo,
# MAGIC S.BankNumberLength = T.BankNumberLength,
# MAGIC S.RuleForCheckingBankNumber = T.RuleForCheckingBankNumber,
# MAGIC S.PostOfficeBankCurrentAccountNumber = T.PostOfficeBankCurrentAccountNumber,
# MAGIC S.RuleForCheckingPostalCheckAccountNumber = T.RuleForCheckingPostalCheckAccountNumber,
# MAGIC S.UseCheckModuleForBankField = T.UseCheckModuleForBankField,
# MAGIC S.NameOfTheBankKey = T.NameOfTheBankKey,
# MAGIC S.LengthOfBankKey = T.LengthOfBankKey,
# MAGIC S.RuleForCheckingBankKeyField = T.RuleForCheckingBankKeyField,
# MAGIC S.UseCheckModuleForTaxFields = T.UseCheckModuleForTaxFields,
# MAGIC S.RuleForCheckingVATRegistrationNumberField = T.RuleForCheckingVATRegistrationNumberField,
# MAGIC S.VATRegistrationNumberLength = T.VATRegistrationNumberLength,
# MAGIC S.PermittedInputLengthForTaxNumber1 = T.PermittedInputLengthForTaxNumber1,
# MAGIC S.RuleForCheckingTaxCode1 = T.RuleForCheckingTaxCode1,
# MAGIC S.PermittedInputLengthForTaxNumber2 = T.PermittedInputLengthForTaxNumber2,
# MAGIC S.RuleForCheckingTaxCode2 = T.RuleForCheckingTaxCode2,
# MAGIC S.PermittedInputLengthForTaxNumber3 = T.PermittedInputLengthForTaxNumber3,
# MAGIC S.RuleForCheckingTaxCode3 = T.RuleForCheckingTaxCode3,
# MAGIC S.PermittedInputLengthForTaxNumber4 = T.PermittedInputLengthForTaxNumber4,
# MAGIC S.RuleForCheckingTaxCode4 = T.RuleForCheckingTaxCode4,
# MAGIC S.PermittedInputLengthForTaxNumber5 = T.PermittedInputLengthForTaxNumber5,
# MAGIC S.RuleForCheckingTaxCode5 = T.RuleForCheckingTaxCode5,
# MAGIC S.Nationality = T.Nationality,
# MAGIC S.Procedure = T.Procedure,
# MAGIC S.AlternativeCountryKey = T.AlternativeCountryKey,
# MAGIC S.BillOfExchangePaymentPeriod = T.BillOfExchangePaymentPeriod,
# MAGIC S.ShortNameForForeignTradeStatistics = T.ShortNameForForeignTradeStatistics,
# MAGIC S.IntrastatCode = T.IntrastatCode,
# MAGIC S.PointFormat = T.PointFormat,
# MAGIC S.DateFormat = T.DateFormat,
# MAGIC S.CurrencyKeyOfTheIndexBasedCurrency = T.CurrencyKeyOfTheIndexBasedCurrency,
# MAGIC S.CurrencyKeyOfTheHardCurrency = T.CurrencyKeyOfTheHardCurrency,
# MAGIC S.CountryCurrency = T.CountryCurrency,
# MAGIC S.ExchangeRateTypeForTranslationIntoCountryCurrency = T.ExchangeRateTypeForTranslationIntoCountryCurrency,
# MAGIC S.ChartOfDepreciatonForAssetValuation = T.ChartOfDepreciatonForAssetValuation,
# MAGIC S.MaximumLowValueAssetAmount = T.MaximumLowValueAssetAmount,
# MAGIC S.NetBookValueForChangeoverOfDepreciationMethod = T.NetBookValueForChangeoverOfDepreciationMethod,
# MAGIC S.IndicatorPostNetBookValueForRetirement = T.IndicatorPostNetBookValueForRetirement,
# MAGIC S.IndicatorTransferDownPayments = T.IndicatorTransferDownPayments,
# MAGIC S.WithholdingTaxCertificateNumbering = T.WithholdingTaxCertificateNumbering,
# MAGIC S.TaxesAtIndividualServiceLevel = T.TaxesAtIndividualServiceLevel,
# MAGIC S.DisplayCapitalGoodsIndicator = T.DisplayCapitalGoodsIndicator,
# MAGIC S.GCCCountriesMember = T.GCCCountriesMember,
# MAGIC S.SuperRegionPerCountry = T.SuperRegionPerCountry,
# MAGIC S.LandingFileTimeStamp = T.LandingFileTimeStamp,
# MAGIC S.UpdatedOn = T.UpdatedOn,
# MAGIC S.DataSource = T.DataSource
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC Client,
# MAGIC CountryKey,
# MAGIC VehicleCountryKey,
# MAGIC PostalCodeLength,
# MAGIC RuleForThePostalCodeFieldCheck,
# MAGIC FormattingRoutineKeyForPrintingAddresses,
# MAGIC StreetAddressPostalCodeReqd,
# MAGIC POBoxPostalCodeReqd,
# MAGIC LanguageKey,
# MAGIC CountryVersionFlag,
# MAGIC CountryNameInForeignAddresses,
# MAGIC StandardNameFormat,
# MAGIC CityFileAddressCheck,
# MAGIC StreetSpecificPostalCode,
# MAGIC CountryISOcode,
# MAGIC ISOCountryCode3char,
# MAGIC ISOCountryCodeNumeric3Char,
# MAGIC EuropeanUnionMember,
# MAGIC DiscountBaseAmount,
# MAGIC BaseAmountForTax,
# MAGIC BankAccountNumberLength,
# MAGIC RuleForCheckingBankAccountNo,
# MAGIC BankNumberLength,
# MAGIC RuleForCheckingBankNumber,
# MAGIC PostOfficeBankCurrentAccountNumber,
# MAGIC RuleForCheckingPostalCheckAccountNumber,
# MAGIC UseCheckModuleForBankField,
# MAGIC NameOfTheBankKey,
# MAGIC LengthOfBankKey,
# MAGIC RuleForCheckingBankKeyField,
# MAGIC UseCheckModuleForTaxFields,
# MAGIC RuleForCheckingVATRegistrationNumberField,
# MAGIC VATRegistrationNumberLength,
# MAGIC PermittedInputLengthForTaxNumber1,
# MAGIC RuleForCheckingTaxCode1,
# MAGIC PermittedInputLengthForTaxNumber2,
# MAGIC RuleForCheckingTaxCode2,
# MAGIC PermittedInputLengthForTaxNumber3,
# MAGIC RuleForCheckingTaxCode3,
# MAGIC PermittedInputLengthForTaxNumber4,
# MAGIC RuleForCheckingTaxCode4,
# MAGIC PermittedInputLengthForTaxNumber5,
# MAGIC RuleForCheckingTaxCode5,
# MAGIC Nationality,
# MAGIC Procedure,
# MAGIC AlternativeCountryKey,
# MAGIC BillOfExchangePaymentPeriod,
# MAGIC ShortNameForForeignTradeStatistics,
# MAGIC IntrastatCode,
# MAGIC PointFormat,
# MAGIC DateFormat,
# MAGIC CurrencyKeyOfTheIndexBasedCurrency,
# MAGIC CurrencyKeyOfTheHardCurrency,
# MAGIC CountryCurrency,
# MAGIC ExchangeRateTypeForTranslationIntoCountryCurrency,
# MAGIC ChartOfDepreciatonForAssetValuation,
# MAGIC MaximumLowValueAssetAmount,
# MAGIC NetBookValueForChangeoverOfDepreciationMethod,
# MAGIC IndicatorPostNetBookValueForRetirement,
# MAGIC IndicatorTransferDownPayments,
# MAGIC WithholdingTaxCertificateNumbering,
# MAGIC TaxesAtIndividualServiceLevel,
# MAGIC DisplayCapitalGoodsIndicator,
# MAGIC GCCCountriesMember,
# MAGIC SuperRegionPerCountry,
# MAGIC LandingFileTimeStamp,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC T.Client ,
# MAGIC T.CountryKey ,
# MAGIC T.VehicleCountryKey ,
# MAGIC T.PostalCodeLength ,
# MAGIC T.RuleForThePostalCodeFieldCheck ,
# MAGIC T.FormattingRoutineKeyForPrintingAddresses ,
# MAGIC T.StreetAddressPostalCodeReqd ,
# MAGIC T.POBoxPostalCodeReqd ,
# MAGIC T.LanguageKey ,
# MAGIC T.CountryVersionFlag ,
# MAGIC T.CountryNameInForeignAddresses ,
# MAGIC T.StandardNameFormat ,
# MAGIC T.CityFileAddressCheck ,
# MAGIC T.StreetSpecificPostalCode ,
# MAGIC T.CountryISOcode ,
# MAGIC T.ISOCountryCode3char ,
# MAGIC T.ISOCountryCodeNumeric3Char ,
# MAGIC T.EuropeanUnionMember ,
# MAGIC T.DiscountBaseAmount ,
# MAGIC T.BaseAmountForTax ,
# MAGIC T.BankAccountNumberLength ,
# MAGIC T.RuleForCheckingBankAccountNo ,
# MAGIC T.BankNumberLength ,
# MAGIC T.RuleForCheckingBankNumber ,
# MAGIC T.PostOfficeBankCurrentAccountNumber ,
# MAGIC T.RuleForCheckingPostalCheckAccountNumber ,
# MAGIC T.UseCheckModuleForBankField ,
# MAGIC T.NameOfTheBankKey ,
# MAGIC T.LengthOfBankKey ,
# MAGIC T.RuleForCheckingBankKeyField ,
# MAGIC T.UseCheckModuleForTaxFields ,
# MAGIC T.RuleForCheckingVATRegistrationNumberField ,
# MAGIC T.VATRegistrationNumberLength ,
# MAGIC T.PermittedInputLengthForTaxNumber1 ,
# MAGIC T.RuleForCheckingTaxCode1 ,
# MAGIC T.PermittedInputLengthForTaxNumber2 ,
# MAGIC T.RuleForCheckingTaxCode2 ,
# MAGIC T.PermittedInputLengthForTaxNumber3 ,
# MAGIC T.RuleForCheckingTaxCode3 ,
# MAGIC T.PermittedInputLengthForTaxNumber4 ,
# MAGIC T.RuleForCheckingTaxCode4 ,
# MAGIC T.PermittedInputLengthForTaxNumber5 ,
# MAGIC T.RuleForCheckingTaxCode5 ,
# MAGIC T.Nationality ,
# MAGIC T.Procedure ,
# MAGIC T.AlternativeCountryKey ,
# MAGIC T.BillOfExchangePaymentPeriod ,
# MAGIC T.ShortNameForForeignTradeStatistics ,
# MAGIC T.IntrastatCode ,
# MAGIC T.PointFormat ,
# MAGIC T.DateFormat ,
# MAGIC T.CurrencyKeyOfTheIndexBasedCurrency ,
# MAGIC T.CurrencyKeyOfTheHardCurrency ,
# MAGIC T.CountryCurrency ,
# MAGIC T.ExchangeRateTypeForTranslationIntoCountryCurrency ,
# MAGIC T.ChartOfDepreciatonForAssetValuation ,
# MAGIC T.MaximumLowValueAssetAmount ,
# MAGIC T.NetBookValueForChangeoverOfDepreciationMethod ,
# MAGIC T.IndicatorPostNetBookValueForRetirement ,
# MAGIC T.IndicatorTransferDownPayments ,
# MAGIC T.WithholdingTaxCertificateNumbering ,
# MAGIC T.TaxesAtIndividualServiceLevel ,
# MAGIC T.DisplayCapitalGoodsIndicator ,
# MAGIC T.GCCCountriesMember ,
# MAGIC T.SuperRegionPerCountry ,
# MAGIC T.LandingFileTimeStamp ,
# MAGIC now(),
# MAGIC T.DataSource
# MAGIC )

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'T005'".format(ts))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Country where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Country')")

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
  .option("dbtable", "Country") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_Country()""")

# COMMAND ----------

if(ts != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Country'".format(ts))

# COMMAND ----------


