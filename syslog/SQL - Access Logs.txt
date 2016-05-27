-- Databricks notebook source exported at Fri, 27 May 2016 19:24:30 UTC
-- MAGIC %md
-- MAGIC #### SQL Analysis of Our Weblog History
-- MAGIC This notebook will demostrate the SQL notebook functionality. You have access to the SparkSQL tables and through these notebooks, as well as visualizations to analyze the data sets. 

-- COMMAND ----------

show databases

-- COMMAND ----------

select count(*) from amazon

-- COMMAND ----------

show tables;

-- COMMAND ----------

DROP TABLE IF EXISTS accesslog;
CREATE EXTERNAL TABLE accesslog (
  ipaddress STRING,
  clientidentd STRING,
  userid STRING,
  datetime STRING,
  method STRING,
  endpoint STRING,
  protocol STRING,
  responseCode INT,
  contentSize BIGINT,
  referrer STRING,
  agent STRING,
  duration STRING,
  ip1 STRING,
  ip2 STRING,
  ip3 STRING,
  ip4 STRING
)
ROW FORMAT
  SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\]  \\"(\\S+) (\\S+) (\\S+)\\" (\\d{3}) (\\d+) \\"(.*)\\" \\"(.*)\\" (\\S+) \\"(\\S+), (\\S+), (\\S+), (\\S+)\\"'
)
LOCATION 
  "/mnt/mwc/accesslog/"

-- COMMAND ----------

DESC EXTENDED accesslog

-- COMMAND ----------

SELECT ipaddress, datetime, method, endpoint, protocol, responsecode, agent FROM accesslog LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the distinct ip address mappings to find the locations of the users. 

-- COMMAND ----------

select count(*) from accesslog

-- COMMAND ----------

DROP TABLE IF EXISTS distinct_ips;
create table distinct_ips as select distinct ip1 from accesslog where ip1 is not null; 
select count(*) from distinct_ips; 

-- COMMAND ----------

cache table distinct_ips

-- COMMAND ----------

select count(*) from distinct_ips; 

-- COMMAND ----------

desc extended mapIps

-- COMMAND ----------

DROP TABLE IF EXISTS mapIps; 
CREATE TABLE mapIps AS SELECT ip1 AS ip, mapCCA2(ip1) AS cca2 FROM distinct_ips;

-- COMMAND ----------

SELECT * FROM mapIps LIMIT 40

-- COMMAND ----------

SELECT ip, `mapIps`.cca2 as cca2, `countryCodes`.cca3 as cca3, cn FROM mapIps LEFT OUTER JOIN countryCodes where mapIps.cca2 = countryCodes.cca2

-- COMMAND ----------

DROP TABLE IF EXISTS mappedIP;
CREATE TABLE mappedIP AS SELECT ip, `mapIps`.cca2 as cca2, `countryCodes`.cca3 as cca3, cn FROM mapIps LEFT OUTER JOIN countryCodes where mapIps.cca2 = countryCodes.cca2

-- COMMAND ----------

desc mappedIP

-- COMMAND ----------

DROP TABLE IF EXISTS userAgentTable;
DROP TABLE IF EXISTS userAgentInfo; 
CREATE TABLE userAgentTable AS SELECT DISTINCT agent FROM accesslog; 
CREATE TABLE userAgentInfo AS SELECT agent, osFamily(agent) as OSFamily, browserFamily(agent) as browserFamily FROM userAgentTable; 
SELECT * from userAgentInfo limit 10;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Most Popular Browser

-- COMMAND ----------

SELECT browserFamily, count(1) FROM UserAgentInfo group by browserFamily

-- COMMAND ----------

SELECT UserId, cca3, weblog2Time(a.datetime) AS datetime, u.browserFamily, u.OSFamily, a.endpoint, a.referrer, a.method, a.responsecode, a.contentsize 
FROM accesslog a JOIN UserAgentInfo u ON u.agent = a.agent 
JOIN mappedIP m ON m.ip = a.ip1

-- COMMAND ----------

DROP TABLE IF EXISTS accessLogsPrime;

-- COMMAND ----------

CREATE TABLE accessLogsPrime AS SELECT hash(a.ip1, a.agent) AS UserId, cca3, weblog2Time(a.datetime) AS datetime, u.browserFamily, u.OSFamily, a.endpoint, a.referrer, a.method, a.responsecode, a.contentsize 
FROM accesslog a JOIN UserAgentInfo u ON u.agent = a.agent 
JOIN mappedIP m ON m.ip = a.ip1; 

-- COMMAND ----------

CACHE TABLE accessLogsPrime; 

-- COMMAND ----------

select UserId, datetime, cca3, browserFamily, OSFamily, method, responseCode, contentSize from accessLogsPrime limit 10;


-- COMMAND ----------

cache table accessLogsPrime;

-- COMMAND ----------

select browserFamily, count(distinct UserID) as Users, count(1) as Events from accessLogsPrime group by browserFamily order by Users desc limit 10;

-- COMMAND ----------

select hour(datetime) as Hour, count(1) as events from accessLogsPrime group by hour(datetime) order by hour(datetime)

-- COMMAND ----------

select OSFamily, count(distinct UserID) as Users from accessLogsPrime group by OSFamily order by Users desc limit 10;

-- COMMAND ----------

select cca3, count(distinct UserID) as users from accessLogsPrime group by cca3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Saving Sample Datasets
-- MAGIC There's a few ways to do this.  
-- MAGIC * Use the builtin download tool to download samples. It currently limits to 1000 rows in the browser.  
-- MAGIC   * (New Feature with Custom Limit Downloader in the works)
-- MAGIC * Create a tool / notebook that pulls the dataset as a CSV file into a particular S3 bucket. Use client side tools to pull it locally
-- MAGIC * Use the FileStore to save larger samples over 1000 rows. 

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC mkdir /dbfs/FileStore/mwc_sample

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC ls /dbfs/FileStore/mwc_sample

-- COMMAND ----------

CREATE TABLE sample1 (OSFamily string, Users int) USING com.databricks.spark.csv
OPTIONS (path "dbfs:/FileStore/mwc_sample")

-- COMMAND ----------

INSERT OVERWRITE TABLE sample1 select OSFamily, count(distinct UserID) as Users from accessLogsPrime group by OSFamily order by Users desc limit 100

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC ls /dbfs/FileStore/mwc_sample

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now the data is located in your FileStore location you can access via the browser. 