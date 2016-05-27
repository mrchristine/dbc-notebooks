# Databricks notebook source exported at Fri, 27 May 2016 19:20:43 UTC
# MAGIC %md
# MAGIC ### Data Prep
# MAGIC This notebook will cover how to access existing data in S3 in a particular bucket. 

# COMMAND ----------

# MAGIC %run "/Users/mwc@databricks.com/_helper.py"

# COMMAND ----------

ACCESS_KEY = "[REPLACE_WITH_ACCESS_KEY]"
SECRET_KEY = "[REPLACE_WITH_SECRET_KEY]"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = get_bucket_name()
MOUNT_NAME = "mwc"

# Mount S3 bucket 
try:
  dbutils.fs.ls("/mnt/%s" % MOUNT_NAME)
except:
  print "Mount not found. Attempting to mount..."
  dbutils.fs.mount("s3n://%s:%s@%s/" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mwc"))

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/mwc/accesslog/databricks.com-access.log	

# COMMAND ----------

# MAGIC %md
# MAGIC * Create an external table against the access log data where we define a regular expression format as part of the serializer/deserializer (SerDe) definition.  
# MAGIC * Instead of writing ETL logic to do this, our table definition handles this.
# MAGIC * Original Format: %s %s %s [%s] \"%s %s HTTP/1.1\" %s %s
# MAGIC * Example Web Log Row 
# MAGIC  * 10.0.0.213 - 2185662 [14/Aug/2015:00:05:15 -0800] "GET /Hurricane+Ridge/rss.xml HTTP/1.1" 200 288

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS accesslog;
# MAGIC CREATE EXTERNAL TABLE accesslog (
# MAGIC   ipaddress STRING,
# MAGIC   clientidentd STRING,
# MAGIC   userid STRING,
# MAGIC   datetime STRING,
# MAGIC   method STRING,
# MAGIC   endpoint STRING,
# MAGIC   protocol STRING,
# MAGIC   responseCode INT,
# MAGIC   contentSize BIGINT,
# MAGIC   referrer STRING,
# MAGIC   agent STRING,
# MAGIC   duration STRING,
# MAGIC   ip1 STRING,
# MAGIC   ip2 STRING,
# MAGIC   ip3 STRING,
# MAGIC   ip4 STRING
# MAGIC )
# MAGIC ROW FORMAT
# MAGIC   SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
# MAGIC WITH SERDEPROPERTIES (
# MAGIC   "input.regex" = '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\]  \\"(\\S+) (\\S+) (\\S+)\\" (\\d{3}) (\\d+) \\"(.*)\\" \\"(.*)\\" (\\S+) \\"(\\S+), (\\S+), (\\S+), (\\S+)\\"'
# MAGIC )
# MAGIC LOCATION 
# MAGIC   "/mnt/mwc/accesslog/"

# COMMAND ----------

# MAGIC %sql select ipaddress, datetime, method, endpoint, protocol, responsecode, agent from accesslog limit 10;

# COMMAND ----------

# MAGIC %md ## Obtain ISO-3166-1 Three Letter Country Codes from IP address
# MAGIC * Extract out the distinct set of IP addresses from the Apache Access logs
# MAGIC * Make a REST web service call to freegeoip.net to get the two-letter country codes based on the IP address
# MAGIC  * This creates the **mappedIP2** DataFrame where the schema is encoded.
# MAGIC * Create a DataFrame to extract out a mapping between 2-letter code, 3-letter code, and country name
# MAGIC  * This creates the **countryCodesDF** DataFrame where the schema is inferred
# MAGIC * Join these two data frames together and select out only the four columns needed to create the **mappedIP3** DataFrame

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS distinct_ips;
# MAGIC create table distinct_ips as select distinct ip1 from accesslog where ip1 is not null; 
# MAGIC select count(*) from distinct_ips; 

# COMMAND ----------

import urllib2 
import json

api_key = get_ip_loc_api_key()

def getCCA2(ip):
  url = 'http://api.db-ip.com/addrinfo?addr=' + ip + '&api_key=%s' % api_key
  str = json.loads(urllib2.urlopen(url).read())
  return str['country'].encode('utf-8')

sqlContext.udf.register("mapCCA2", getCCA2)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS mapIps;
# MAGIC CREATE TABLE mapIps AS SELECT ip1 AS ip, mapCCA2(ip1) AS cca2 FROM distinct_ips;

# COMMAND ----------

# MAGIC %sql SELECT * FROM mapIps LIMIT 40

# COMMAND ----------

from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *


fields = sc.textFile("/mnt/mwc/countrycodes/").map(lambda l: l.split(","))
countrycodes = fields.map(lambda x: Row(cn=x[0], cca2=x[1], cca3=x[2]))
sqlContext.createDataFrame(countrycodes).registerTempTable("countryCodes")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM countryCodes LIMIT 20

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT ip, `mapIps`.cca2 as cca2, `countryCodes`.cca3 as cca3, cn FROM mapIps LEFT OUTER JOIN countryCodes where mapIps.cca2 = countryCodes.cca2

# COMMAND ----------

# MAGIC %md ## Identity the Browser and OS information 
# MAGIC * Extract out the distinct set of user agents from the Apache Access logs
# MAGIC * Use the Python Package [user-agents](https://pypi.python.org/pypi/user-agents) to extract out Browser and OS information from the User Agent strring
# MAGIC * For more information on installing pypi packages in Databricks, refer to [Databricks Guide > Product Overview > Libraries](https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#02%20Product%20Overview/07%20Libraries.html)

# COMMAND ----------

from user_agents import parse
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# Convert None to Empty String
def xstr(s): 
  if s is None: 
    return '' 
  return str(s)

# Create UDFs to extract out Browser Family and OS Family information
def browserFamily(ua_string) : return xstr(parse(xstr(ua_string)).browser.family)
def osFamily(ua_string) : return xstr(parse(xstr(ua_string)).os.family)

sqlContext.udf.register("browserFamily", browserFamily)
sqlContext.udf.register("osFamily", osFamily)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS userAgentTable;
# MAGIC DROP TABLE IF EXISTS userAgentInfo; 
# MAGIC CREATE TABLE userAgentTable AS SELECT DISTINCT agent FROM accesslog; 
# MAGIC CREATE TABLE userAgentInfo AS SELECT agent, osFamily(agent) as OSFamily, browserFamily(agent) as browserFamily FROM userAgentTable; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT browserFamily, count(1) FROM UserAgentInfo group by browserFamily

# COMMAND ----------

# MAGIC %md ## UserID, Date, and Joins
# MAGIC To make finish basic preparation of these web logs, we will do the following: 
# MAGIC * Convert the Apache web logs date information
# MAGIC * Create a userid based on the IP address and User Agent (these logs do not have a UserID)
# MAGIC  * We are generating the UserID (a way to uniquify web site visitors) by combining these two columns
# MAGIC * Join back to the Browser and OS information as well as Country (based on IP address) information
# MAGIC * Also include call to udfWeblog2Time function to convert the Apache web log date into a Spark SQL / Hive friendly format (for session calculations below)

# COMMAND ----------

from pyspark.sql.types import DateType
from pyspark.sql.functions import udf
import time

# weblog2Time function
#   Input: 04/Nov/2015:08:15:00 +0000
#   Output: 2015-11-04 08:15:00
def weblog2Time(weblog_timestr):
  weblog_time = time.strptime(weblog_timestr, "%d/%b/%Y:%H:%M:%S +0000")
  weblog_t = time.mktime(weblog_time)
  return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(weblog_t))

# Register the UDF
sqlContext.udf.register("weblog2Time", weblog2Time)


# COMMAND ----------

# MAGIC %md
# MAGIC From here I'll use the SQL notebook to continue the SQL analysis, but refer back to this notebook for any user defined functions. 