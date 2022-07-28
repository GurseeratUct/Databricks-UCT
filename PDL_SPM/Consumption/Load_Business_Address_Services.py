# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp
from delta.tables import *

# COMMAND ----------

consumption_table_name = 'Business_Address_Services'
write_format = 'delta'
write_path = '/mnt/uct-consumption-gen-dev/Dimensions/'+consumption_table_name+'/' #write_path
database_name = 'FEDW'

# COMMAND ----------

col_names_adrc = [
    "CLIENT",
"ADDRNUMBER",
"DATE_FROM",
"NATION",
"DATE_TO",
"TITLE",
"NAME1",
"NAME2",
"NAME3",
"NAME4",
"NAME_TEXT",
"NAME_CO",
"CITY1",
"CITY2",
"CITY_CODE",
"CITYP_CODE",
"HOME_CITY",
"CITYH_CODE",
"CHCKSTATUS",
"REGIOGROUP",
"POST_CODE1",
"POST_CODE2",
"POST_CODE3",
"PCODE1_EXT",
"PCODE2_EXT",
"PCODE3_EXT",
"PO_BOX",
"DONT_USE_P",
"PO_BOX_NUM",
"PO_BOX_LOC",
"CITY_CODE2",
"PO_BOX_REG",
"PO_BOX_CTY",
"POSTALAREA",
"TRANSPZONE",
"STREET",
"DONT_USE_S",
"STREETCODE",
"STREETABBR",
"HOUSE_NUM1",
"HOUSE_NUM2",
"HOUSE_NUM3",
"STR_SUPPL1",
"STR_SUPPL2",
"STR_SUPPL3",
"LOCATION",
"BUILDING",
"FLOOR",
"ROOMNUMBER",
"COUNTRY",
"LANGU",
"REGION",
"ADDR_GROUP",
"FLAGGROUPS",
"PERS_ADDR",
"SORT1",
"SORT2",
"SORT_PHN",
"DEFLT_COMM",
"TEL_NUMBER",
"TEL_EXTENS",
"FAX_NUMBER",
"FAX_EXTENS",
"FLAGCOMM2",
"FLAGCOMM3",
"FLAGCOMM4",
"FLAGCOMM5",
"FLAGCOMM6",
"FLAGCOMM7",
"FLAGCOMM8",
"FLAGCOMM9",
"FLAGCOMM10",
"FLAGCOMM11",
"FLAGCOMM12",
"FLAGCOMM13",
"ADDRORIGIN",
"MC_NAME1",
"MC_CITY1",
"MC_STREET",
"EXTENSION1",
"EXTENSION2",
"TIME_ZONE",
"TAXJURCODE",
"ADDRESS_ID",
"LANGU_CREA",
"ADRC_UUID",
"UUID_BELATED",
"ID_CATEGORY",
"ADRC_ERR_STATUS",
"PO_BOX_LOBBY",
"DELI_SERV_TYPE",
"DELI_SERV_NUMBER",
"COUNTY_CODE",
"COUNTY",
"TOWNSHIP_CODE",
"TOWNSHIP",
"MC_COUNTY",
"MC_TOWNSHIP",
"XPCPT",
"_DATAAGING",
"DUNS",
"DUNSP4"
]

# COMMAND ----------

df = spark.sql("select * from S42.ADRC where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Transform' and SourceLayerTableName = 'ADRC' and DatabaseName = 'S42')")
df_adrc = df.select(col_names_adrc)
df_adrc.createOrReplaceTempView("tmp_adrc")
df_ts_adrc = df.agg({"UpdatedOn": "max"}).collect()[0]
ts_adrc = df_ts_adrc["max(UpdatedOn)"]
print(ts_adrc)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view merge_business_address_services as 
# MAGIC select 
# MAGIC S.CLIENT as Client,
# MAGIC S.ADDRNUMBER as AddressNumber,
# MAGIC S.DATE_FROM as Validfromdate,
# MAGIC S.NATION as VersionIDforInterAdd,
# MAGIC S.DATE_TO as Validtodate,
# MAGIC S.TITLE as FormofAddressKey,
# MAGIC S.NAME1 as Name1,
# MAGIC S.NAME2 as Name2,
# MAGIC S.NAME3 as Name3,
# MAGIC S.NAME4 as Name4,
# MAGIC S.NAME_TEXT as Convertednamefield,
# MAGIC S.NAME_CO as coname,
# MAGIC S.CITY1 as City,
# MAGIC S.CITY2 as District,
# MAGIC S.CITY_CODE as Citycodeforcity,
# MAGIC S.CITYP_CODE as DistrictcodeforCityandStreetfile,
# MAGIC S.HOME_CITY as Citydifferentfrompostalcity,
# MAGIC S.CITYH_CODE as Differentcityforcity,
# MAGIC S.CHCKSTATUS as Cityfileteststatus,
# MAGIC S.REGIOGROUP as Regionalstructuregrouping,
# MAGIC S.POST_CODE1 as Citypostalcode,
# MAGIC S.POST_CODE2 as POBoxPostalCode,
# MAGIC S.POST_CODE3 as CompanyPostalCode,
# MAGIC S.PCODE1_EXT as CityPostalCodeExtension,
# MAGIC S.PCODE2_EXT as POBoxPostalCodeExtension,
# MAGIC S.PCODE3_EXT as MajorCustomerPostalCodeExtension,
# MAGIC S.PO_BOX as POBox,
# MAGIC S.DONT_USE_P as POBoxAddressUndeliverableFlag,
# MAGIC S.PO_BOX_NUM as FlagPOBoxWithoutNumber,
# MAGIC S.PO_BOX_LOC as POBoxcity,
# MAGIC S.CITY_CODE2 as CityPOboxcode,
# MAGIC S.PO_BOX_REG as RegionforPOBox,
# MAGIC S.PO_BOX_CTY as POboxcountry,
# MAGIC S.POSTALAREA as PostDeliveryDistrict,
# MAGIC S.TRANSPZONE as Transportationzone,
# MAGIC S.STREET as Street,
# MAGIC S.DONT_USE_S as StreetAddressUndeliverableFlag,
# MAGIC S.STREETCODE as StreetNumberforCity,
# MAGIC S.STREETABBR as AbbreviationofStreetName,
# MAGIC S.HOUSE_NUM1 as HouseNumber,
# MAGIC S.HOUSE_NUM2 as Housenumbersupplement,
# MAGIC S.HOUSE_NUM3 as HouseNumberRange,
# MAGIC S.STR_SUPPL1 as Street2,
# MAGIC S.STR_SUPPL2 as Street3,
# MAGIC S.STR_SUPPL3 as Street4,
# MAGIC S.LOCATION as Street5,
# MAGIC S.BUILDING as Building,
# MAGIC S.FLOOR as Floorinbuilding,
# MAGIC S.ROOMNUMBER as RoomorApartmentNumber,
# MAGIC S.COUNTRY as CountryKey,
# MAGIC S.LANGU as LanguageKey,
# MAGIC S.REGION as Region,
# MAGIC S.ADDR_GROUP as AddressGroup,
# MAGIC S.FLAGGROUPS as Therearemoreaddressgroupassignments,
# MAGIC S.PERS_ADDR as Thisisapersonaladdress,
# MAGIC S.SORT1 as SearchTerm1,
# MAGIC S.SORT2 as SearchTerm2,
# MAGIC S.SORT_PHN as PhoneticSearchSortField,
# MAGIC S.DEFLT_COMM as CommunicationMethod,
# MAGIC S.TEL_NUMBER as diallingcodenumber,
# MAGIC S.TEL_EXTENS as Extension,
# MAGIC S.FAX_NUMBER as AreaCodeNumber,
# MAGIC S.FAX_EXTENS as Firstfaxno,
# MAGIC S.FLAGCOMM2 as Telephonenumber,
# MAGIC S.FLAGCOMM3 as Faxnumber,
# MAGIC S.FLAGCOMM4 as Teletexnumber,
# MAGIC S.FLAGCOMM5 as Telexnumber,
# MAGIC S.FLAGCOMM6 as Emailaddress,
# MAGIC S.FLAGCOMM7 as RML,
# MAGIC S.FLAGCOMM8 as addresse,
# MAGIC S.FLAGCOMM9 as RFCdestination,
# MAGIC S.FLAGCOMM10 as Printerdefined,
# MAGIC S.FLAGCOMM11 as SSFdefined,
# MAGIC S.FLAGCOMM12 as FTPaddressdefined,
# MAGIC S.FLAGCOMM13 as Pageraddressdefined,
# MAGIC S.ADDRORIGIN as AddressDataSource,
# MAGIC S.MC_NAME1 as NameinUppercase,
# MAGIC S.MC_CITY1 as CitynameinUppercase,
# MAGIC S.MC_STREET as StreetNameinUppercase,
# MAGIC S.EXTENSION1 as Extensiondataline,
# MAGIC S.EXTENSION2 as Extensiontelebox,
# MAGIC S.TIME_ZONE as Addresstimezone,
# MAGIC S.TAXJURCODE as TaxJurisdiction,
# MAGIC S.ADDRESS_ID as PhysicaladdressID,
# MAGIC S.LANGU_CREA as Addressrecordcreationoriginallanguage,
# MAGIC S.ADRC_UUID as UUIDUsedintheAddress,
# MAGIC S.UUID_BELATED as UUIDcreatedlater,
# MAGIC S.ID_CATEGORY as CategoryofanAddressID,
# MAGIC S.ADRC_ERR_STATUS as ErrorStatusofAddress,
# MAGIC S.PO_BOX_LOBBY as POBoxLobby,
# MAGIC S.DELI_SERV_TYPE as TypeofDeliveryService,
# MAGIC S.DELI_SERV_NUMBER as NumberofDeliveryService,
# MAGIC S.COUNTY_CODE as Countycodeforcounty,
# MAGIC S.COUNTY as County,
# MAGIC S.TOWNSHIP_CODE as TownshipcodeforTownship,
# MAGIC S.TOWNSHIP as Township,
# MAGIC S.MC_COUNTY as Countynameinuppercase,
# MAGIC S.MC_TOWNSHIP as Townshipnameinuppercase,
# MAGIC S.XPCPT as BusinessPurposeCompletedFlag,
# MAGIC S._DATAAGING as DataFilterValueforDataAging,
# MAGIC S.DUNS as DunBradstreetnumber,
# MAGIC S.DUNSP4 as DUNS4number,
# MAGIC now() as UpdatedOn,
# MAGIC 'SAP' as DataSource
# MAGIC from tmp_adrc as S

# COMMAND ----------

df_merge = spark.sql("select * from merge_business_address_services")

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False): 
        df_accounting.write.format(write_format).mode("overwrite").save(write_path) 
        spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ consumption_table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_merge.createOrReplaceTempView('stg_merge_business_address_services')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO FEDW.business_address_services as T 
# MAGIC USING stg_merge_business_address_services as S 
# MAGIC ON T.Client = S.Client
# MAGIC and T.AddressNumber = S.AddressNumber
# MAGIC and T.Validfromdate = S.Validfromdate
# MAGIC and T.VersionIDforInterAdd = S.VersionIDforInterAdd
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC T.Client =  S.Client,
# MAGIC T.AddressNumber =  S.AddressNumber,
# MAGIC T.Validfromdate =  S.Validfromdate,
# MAGIC T.VersionIDforInterAdd =  S.VersionIDforInterAdd,
# MAGIC T.Validtodate =  S.Validtodate,
# MAGIC T.FormofAddressKey =  S.FormofAddressKey,
# MAGIC T.Name1 =  S.Name1,
# MAGIC T.Name2 =  S.Name2,
# MAGIC T.Name3 =  S.Name3,
# MAGIC T.Name4 =  S.Name4,
# MAGIC T.Convertednamefield =  S.Convertednamefield,
# MAGIC T.coname =  S.coname,
# MAGIC T.City =  S.City,
# MAGIC T.District =  S.District,
# MAGIC T.Citycodeforcity =  S.Citycodeforcity,
# MAGIC T.DistrictcodeforCityandStreetfile =  S.DistrictcodeforCityandStreetfile,
# MAGIC T.Citydifferentfrompostalcity =  S.Citydifferentfrompostalcity,
# MAGIC T.Differentcityforcity =  S.Differentcityforcity,
# MAGIC T.Cityfileteststatus =  S.Cityfileteststatus,
# MAGIC T.Regionalstructuregrouping =  S.Regionalstructuregrouping,
# MAGIC T.Citypostalcode =  S.Citypostalcode,
# MAGIC T.POBoxPostalCode =  S.POBoxPostalCode,
# MAGIC T.CompanyPostalCode =  S.CompanyPostalCode,
# MAGIC T.CityPostalCodeExtension =  S.CityPostalCodeExtension,
# MAGIC T.POBoxPostalCodeExtension =  S.POBoxPostalCodeExtension,
# MAGIC T.MajorCustomerPostalCodeExtension =  S.MajorCustomerPostalCodeExtension,
# MAGIC T.POBox =  S.POBox,
# MAGIC T.POBoxAddressUndeliverableFlag =  S.POBoxAddressUndeliverableFlag,
# MAGIC T.FlagPOBoxWithoutNumber =  S.FlagPOBoxWithoutNumber,
# MAGIC T.POBoxcity =  S.POBoxcity,
# MAGIC T.CityPOboxcode =  S.CityPOboxcode,
# MAGIC T.RegionforPOBox =  S.RegionforPOBox,
# MAGIC T.POboxcountry =  S.POboxcountry,
# MAGIC T.PostDeliveryDistrict =  S.PostDeliveryDistrict,
# MAGIC T.Transportationzone =  S.Transportationzone,
# MAGIC T.Street =  S.Street,
# MAGIC T.StreetAddressUndeliverableFlag =  S.StreetAddressUndeliverableFlag,
# MAGIC T.StreetNumberforCity =  S.StreetNumberforCity,
# MAGIC T.AbbreviationofStreetName =  S.AbbreviationofStreetName,
# MAGIC T.HouseNumber =  S.HouseNumber,
# MAGIC T.Housenumbersupplement =  S.Housenumbersupplement,
# MAGIC T.HouseNumberRange =  S.HouseNumberRange,
# MAGIC T.Street2 =  S.Street2,
# MAGIC T.Street3 =  S.Street3,
# MAGIC T.Street4 =  S.Street4,
# MAGIC T.Street5 =  S.Street5,
# MAGIC T.Building =  S.Building,
# MAGIC T.Floorinbuilding =  S.Floorinbuilding,
# MAGIC T.RoomorApartmentNumber =  S.RoomorApartmentNumber,
# MAGIC T.CountryKey =  S.CountryKey,
# MAGIC T.LanguageKey =  S.LanguageKey,
# MAGIC T.Region =  S.Region,
# MAGIC T.AddressGroup =  S.AddressGroup,
# MAGIC T.Therearemoreaddressgroupassignments =  S.Therearemoreaddressgroupassignments,
# MAGIC T.Thisisapersonaladdress =  S.Thisisapersonaladdress,
# MAGIC T.SearchTerm1 =  S.SearchTerm1,
# MAGIC T.SearchTerm2 =  S.SearchTerm2,
# MAGIC T.PhoneticSearchSortField =  S.PhoneticSearchSortField,
# MAGIC T.CommunicationMethod =  S.CommunicationMethod,
# MAGIC T.diallingcodenumber =  S.diallingcodenumber,
# MAGIC T.Extension =  S.Extension,
# MAGIC T.AreaCodeNumber =  S.AreaCodeNumber,
# MAGIC T.Firstfaxno =  S.Firstfaxno,
# MAGIC T.Telephonenumber =  S.Telephonenumber,
# MAGIC T.Faxnumber =  S.Faxnumber,
# MAGIC T.Teletexnumber =  S.Teletexnumber,
# MAGIC T.Telexnumber =  S.Telexnumber,
# MAGIC T.Emailaddress =  S.Emailaddress,
# MAGIC T.RML =  S.RML,
# MAGIC T.addresse =  S.addresse,
# MAGIC T.RFCdestination =  S.RFCdestination,
# MAGIC T.Printerdefined =  S.Printerdefined,
# MAGIC T.SSFdefined =  S.SSFdefined,
# MAGIC T.FTPaddressdefined =  S.FTPaddressdefined,
# MAGIC T.Pageraddressdefined =  S.Pageraddressdefined,
# MAGIC T.AddressDataSource =  S.AddressDataSource,
# MAGIC T.NameinUppercase =  S.NameinUppercase,
# MAGIC T.CitynameinUppercase =  S.CitynameinUppercase,
# MAGIC T.StreetNameinUppercase =  S.StreetNameinUppercase,
# MAGIC T.Extensiondataline =  S.Extensiondataline,
# MAGIC T.Extensiontelebox =  S.Extensiontelebox,
# MAGIC T.Addresstimezone =  S.Addresstimezone,
# MAGIC T.TaxJurisdiction =  S.TaxJurisdiction,
# MAGIC T.PhysicaladdressID =  S.PhysicaladdressID,
# MAGIC T.Addressrecordcreationoriginallanguage =  S.Addressrecordcreationoriginallanguage,
# MAGIC T.UUIDUsedintheAddress =  S.UUIDUsedintheAddress,
# MAGIC T.UUIDcreatedlater =  S.UUIDcreatedlater,
# MAGIC T.CategoryofanAddressID =  S.CategoryofanAddressID,
# MAGIC T.ErrorStatusofAddress =  S.ErrorStatusofAddress,
# MAGIC T.POBoxLobby =  S.POBoxLobby,
# MAGIC T.TypeofDeliveryService =  S.TypeofDeliveryService,
# MAGIC T.NumberofDeliveryService =  S.NumberofDeliveryService,
# MAGIC T.Countycodeforcounty =  S.Countycodeforcounty,
# MAGIC T.County =  S.County,
# MAGIC T.TownshipcodeforTownship =  S.TownshipcodeforTownship,
# MAGIC T.Township =  S.Township,
# MAGIC T.Countynameinuppercase =  S.Countynameinuppercase,
# MAGIC T.Townshipnameinuppercase =  S.Townshipnameinuppercase,
# MAGIC T.BusinessPurposeCompletedFlag =  S.BusinessPurposeCompletedFlag,
# MAGIC T.DataFilterValueforDataAging =  S.DataFilterValueforDataAging,
# MAGIC T.DunBradstreetnumber =  S.DunBradstreetnumber,
# MAGIC T.DUNS4number =  S.DUNS4number,
# MAGIC T.UpdatedOn = S.UpdatedOn,
# MAGIC T.DataSource = S.DataSource
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC Client,
# MAGIC AddressNumber,
# MAGIC Validfromdate,
# MAGIC VersionIDforInterAdd,
# MAGIC Validtodate,
# MAGIC FormofAddressKey,
# MAGIC Name1,
# MAGIC Name2,
# MAGIC Name3,
# MAGIC Name4,
# MAGIC Convertednamefield,
# MAGIC coname,
# MAGIC City,
# MAGIC District,
# MAGIC Citycodeforcity,
# MAGIC DistrictcodeforCityandStreetfile,
# MAGIC Citydifferentfrompostalcity,
# MAGIC Differentcityforcity,
# MAGIC Cityfileteststatus,
# MAGIC Regionalstructuregrouping,
# MAGIC Citypostalcode,
# MAGIC POBoxPostalCode,
# MAGIC CompanyPostalCode,
# MAGIC CityPostalCodeExtension,
# MAGIC POBoxPostalCodeExtension,
# MAGIC MajorCustomerPostalCodeExtension,
# MAGIC POBox,
# MAGIC POBoxAddressUndeliverableFlag,
# MAGIC FlagPOBoxWithoutNumber,
# MAGIC POBoxcity,
# MAGIC CityPOboxcode,
# MAGIC RegionforPOBox,
# MAGIC POboxcountry,
# MAGIC PostDeliveryDistrict,
# MAGIC Transportationzone,
# MAGIC Street,
# MAGIC StreetAddressUndeliverableFlag,
# MAGIC StreetNumberforCity,
# MAGIC AbbreviationofStreetName,
# MAGIC HouseNumber,
# MAGIC Housenumbersupplement,
# MAGIC HouseNumberRange,
# MAGIC Street2,
# MAGIC Street3,
# MAGIC Street4,
# MAGIC Street5,
# MAGIC Building,
# MAGIC Floorinbuilding,
# MAGIC RoomorApartmentNumber,
# MAGIC CountryKey,
# MAGIC LanguageKey,
# MAGIC Region,
# MAGIC AddressGroup,
# MAGIC Therearemoreaddressgroupassignments,
# MAGIC Thisisapersonaladdress,
# MAGIC SearchTerm1,
# MAGIC SearchTerm2,
# MAGIC PhoneticSearchSortField,
# MAGIC CommunicationMethod,
# MAGIC diallingcodenumber,
# MAGIC Extension,
# MAGIC AreaCodeNumber,
# MAGIC Firstfaxno,
# MAGIC Telephonenumber,
# MAGIC Faxnumber,
# MAGIC Teletexnumber,
# MAGIC Telexnumber,
# MAGIC Emailaddress,
# MAGIC RML,
# MAGIC addresse,
# MAGIC RFCdestination,
# MAGIC Printerdefined,
# MAGIC SSFdefined,
# MAGIC FTPaddressdefined,
# MAGIC Pageraddressdefined,
# MAGIC AddressDataSource,
# MAGIC NameinUppercase,
# MAGIC CitynameinUppercase,
# MAGIC StreetNameinUppercase,
# MAGIC Extensiondataline,
# MAGIC Extensiontelebox,
# MAGIC Addresstimezone,
# MAGIC TaxJurisdiction,
# MAGIC PhysicaladdressID,
# MAGIC Addressrecordcreationoriginallanguage,
# MAGIC UUIDUsedintheAddress,
# MAGIC UUIDcreatedlater,
# MAGIC CategoryofanAddressID,
# MAGIC ErrorStatusofAddress,
# MAGIC POBoxLobby,
# MAGIC TypeofDeliveryService,
# MAGIC NumberofDeliveryService,
# MAGIC Countycodeforcounty,
# MAGIC County,
# MAGIC TownshipcodeforTownship,
# MAGIC Township,
# MAGIC Countynameinuppercase,
# MAGIC Townshipnameinuppercase,
# MAGIC BusinessPurposeCompletedFlag,
# MAGIC DataFilterValueforDataAging,
# MAGIC DunBradstreetnumber,
# MAGIC DUNS4number,
# MAGIC UpdatedOn,
# MAGIC DataSource
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.Client,
# MAGIC S.AddressNumber,
# MAGIC S.Validfromdate,
# MAGIC S.VersionIDforInterAdd,
# MAGIC S.Validtodate,
# MAGIC S.FormofAddressKey,
# MAGIC S.Name1,
# MAGIC S.Name2,
# MAGIC S.Name3,
# MAGIC S.Name4,
# MAGIC S.Convertednamefield,
# MAGIC S.coname,
# MAGIC S.City,
# MAGIC S.District,
# MAGIC S.Citycodeforcity,
# MAGIC S.DistrictcodeforCityandStreetfile,
# MAGIC S.Citydifferentfrompostalcity,
# MAGIC S.Differentcityforcity,
# MAGIC S.Cityfileteststatus,
# MAGIC S.Regionalstructuregrouping,
# MAGIC S.Citypostalcode,
# MAGIC S.POBoxPostalCode,
# MAGIC S.CompanyPostalCode,
# MAGIC S.CityPostalCodeExtension,
# MAGIC S.POBoxPostalCodeExtension,
# MAGIC S.MajorCustomerPostalCodeExtension,
# MAGIC S.POBox,
# MAGIC S.POBoxAddressUndeliverableFlag,
# MAGIC S.FlagPOBoxWithoutNumber,
# MAGIC S.POBoxcity,
# MAGIC S.CityPOboxcode,
# MAGIC S.RegionforPOBox,
# MAGIC S.POboxcountry,
# MAGIC S.PostDeliveryDistrict,
# MAGIC S.Transportationzone,
# MAGIC S.Street,
# MAGIC S.StreetAddressUndeliverableFlag,
# MAGIC S.StreetNumberforCity,
# MAGIC S.AbbreviationofStreetName,
# MAGIC S.HouseNumber,
# MAGIC S.Housenumbersupplement,
# MAGIC S.HouseNumberRange,
# MAGIC S.Street2,
# MAGIC S.Street3,
# MAGIC S.Street4,
# MAGIC S.Street5,
# MAGIC S.Building,
# MAGIC S.Floorinbuilding,
# MAGIC S.RoomorApartmentNumber,
# MAGIC S.CountryKey,
# MAGIC S.LanguageKey,
# MAGIC S.Region,
# MAGIC S.AddressGroup,
# MAGIC S.Therearemoreaddressgroupassignments,
# MAGIC S.Thisisapersonaladdress,
# MAGIC S.SearchTerm1,
# MAGIC S.SearchTerm2,
# MAGIC S.PhoneticSearchSortField,
# MAGIC S.CommunicationMethod,
# MAGIC S.diallingcodenumber,
# MAGIC S.Extension,
# MAGIC S.AreaCodeNumber,
# MAGIC S.Firstfaxno,
# MAGIC S.Telephonenumber,
# MAGIC S.Faxnumber,
# MAGIC S.Teletexnumber,
# MAGIC S.Telexnumber,
# MAGIC S.Emailaddress,
# MAGIC S.RML,
# MAGIC S.addresse,
# MAGIC S.RFCdestination,
# MAGIC S.Printerdefined,
# MAGIC S.SSFdefined,
# MAGIC S.FTPaddressdefined,
# MAGIC S.Pageraddressdefined,
# MAGIC S.AddressDataSource,
# MAGIC S.NameinUppercase,
# MAGIC S.CitynameinUppercase,
# MAGIC S.StreetNameinUppercase,
# MAGIC S.Extensiondataline,
# MAGIC S.Extensiontelebox,
# MAGIC S.Addresstimezone,
# MAGIC S.TaxJurisdiction,
# MAGIC S.PhysicaladdressID,
# MAGIC S.Addressrecordcreationoriginallanguage,
# MAGIC S.UUIDUsedintheAddress,
# MAGIC S.UUIDcreatedlater,
# MAGIC S.CategoryofanAddressID,
# MAGIC S.ErrorStatusofAddress,
# MAGIC S.POBoxLobby,
# MAGIC S.TypeofDeliveryService,
# MAGIC S.NumberofDeliveryService,
# MAGIC S.Countycodeforcounty,
# MAGIC S.County,
# MAGIC S.TownshipcodeforTownship,
# MAGIC S.Township,
# MAGIC S.Countynameinuppercase,
# MAGIC S.Townshipnameinuppercase,
# MAGIC S.BusinessPurposeCompletedFlag,
# MAGIC S.DataFilterValueforDataAging,
# MAGIC S.DunBradstreetnumber,
# MAGIC S.DUNS4number,
# MAGIC now(),
# MAGIC S.DataSource
# MAGIC )

# COMMAND ----------

if(ts_adrc != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Transform' and SourceLayerTableName = 'ADRC' and DatabaseName = 'S42'".format(ts_adrc))

# COMMAND ----------

df_sf = spark.sql("select * from FEDW.Business_Address_Services where UpdatedOn > (select LastUpdatedOn from INCR_DataLoadTracking where SourceLayer = 'Consumption' and SourceLayerTableName = 'Business_Address_Services' and DatabaseName = 'FEDW' )")
ts_sf = df_sf.agg({"UpdatedOn": "max"}).collect()[0]
ts_sf = ts_sf["max(UpdatedOn)"]
print(ts_sf)

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
  .option("dbtable", "Business_Address_Services") \
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
# MAGIC Utils.runQuery(options, """call SP_LOAD_BUSINESS_ADDRESS_SERVICES()""")

# COMMAND ----------

if(ts_sf != None):
    spark.sql ("update INCR_DataLoadTracking set LastUpdatedOn = '{0}' where SourceLayer = 'Consumption' and SourceLayerTableName = 'Business_Address_Services' and DatabaseName = 'FEDW'".format(ts_sf))

# COMMAND ----------


