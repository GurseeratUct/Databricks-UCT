-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS config;
CREATE DATABASE IF NOT EXISTS fedw;
CREATE DATABASE IF NOT EXISTS flat_file;
CREATE DATABASE IF NOT EXISTS it_temp;
CREATE DATABASE IF NOT EXISTS s42;
CREATE DATABASE IF NOT EXISTS ve70;
CREATE DATABASE IF NOT EXISTS ve72;

-- COMMAND ----------

CREATE TABLE config.Config_Constant (variable_name String ,variable_value String ,description String, UpdatedOn Timestamp GENERATED ALWAYS AS (now()));

-- COMMAND ----------

insert into config.Config_Constant(variable_name,variable_value,description) values("sfUrl","ultra_clean_holdings_dataplatform.snowflakecomputing.com","sfUrl");
insert into config.Config_Constant(variable_name,variable_value,description) values("plant_code_singapore","6101","Plant Code Singapore");
insert into config.Config_Constant(variable_name,variable_value,description) values("plant_code_shangai","6311","Plant Code Shangai");
insert into config.Config_Constant(variable_name,variable_value,description) values("sfDatabase","UCT_DEVELOPMENT","sfDatabase");
insert into config.Config_Constant(variable_name,variable_value,description) values("sfPassword","PpanKAJ_9826","sfPassword");
insert into config.Config_Constant(variable_name,variable_value,description) values("sfWarehouse","UCT_DEV","sfWarehouse");
insert into config.Config_Constant(variable_name,variable_value,description) values("insecureMode","true","insecureMode");
insert into config.Config_Constant(variable_name,variable_value,description) values("company_code_shangai","8110","Shangai");
insert into config.Config_Constant(variable_name,variable_value,description) values("company_code_singapore","8120","SINGAPORE");
insert into config.Config_Constant(variable_name,variable_value,description) values("sfSchema","LANDING","sfSchema");
insert into config.Config_Constant(variable_name,variable_value,description) values("sfUser","PKUSHWAH","sfUser");
insert into config.Config_Constant(variable_name,variable_value,description) values("sfRole","PUBLIC","sfRole");
