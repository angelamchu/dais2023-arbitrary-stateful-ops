# Databricks notebook source
# DBTITLE 1,Do this once - make sure the destination directory has been created
# MAGIC %fs
# MAGIC mkdirs s3://my/path/here/autoloaderInput/

# COMMAND ----------

# DBTITLE 1,Reset Environment
# MAGIC %fs
# MAGIC rm -r s3://my/path/here/temp

# COMMAND ----------

# DBTITLE 1,Functions to generate a JSON dataset for the Auto Loader to pick up
import random
import string
from datetime import datetime
import time
import os

# Method to return a random User ID between 1 and 10 (set low for testing some stateful streaming aggregations, higher for more variability)
def returnUserId():
  return random.randint(1, 6)

# Return a random float value for different purposes, rounded to 4 places
def returnValue():
  return round(random.uniform(111.1111, 9999999999.9999), 4)

# Method to return a string of random characters - hard-coded to length of 30
def returnString():
  letters = string.ascii_letters
  return ( ''.join(random.choice(letters) for i in range(30)) )

def returnTransactionTimestamp():
  currentDateTime = datetime.now()
  return currentDateTime.strftime("%Y-%m-%d %H:%M:%S.%f")

# Generate a record
def generateRecord():
  return (returnUserId(), returnString(), returnValue(), returnValue(), returnValue(), returnTransactionTimestamp())
  
# Generate a list of records
def generateRecordSet(recordCount):
  recordSet = []
  for x in range(recordCount):
    recordSet.append(generateRecord())
  return recordSet

# Generate a set of data, convert it to a Dataframe, write it out as one json file in a temp location, 
# move the json file to the desired location that the Auto Loader will be watching and then delete the temp location
def writeJsonFile(recordCount, tempPath, destinationPath):
  recordColumns = ["userId", "stringCode", "value1", "value2", "value3", "transactionTimestamp"]
  recordSet = generateRecordSet(recordCount)
  recordDf = spark.createDataFrame(data=recordSet, schema=recordColumns)
  
  # Write out the json file with Spark in a temp location - this will create a directory with the file we want the Auto Loader to
  # pick up underneath it
  recordDf.coalesce(1).write.format("json").save(tempPath)
  
  # Grab the file from the temp location, write it to the location we want and then delete the temp directory
  tempJson = os.path.join(tempPath, dbutils.fs.ls(tempPath)[3][1])
  dbutils.fs.cp(tempJson, destinationPath)
  dbutils.fs.rm(tempPath, True)
  

# COMMAND ----------

# DBTITLE 1,Define Record Count, Temporary Location, Auto Loader-Monitored Location and Sleep Interval Here
recordCount=5
tempPath = "s3://my/path/here/temp"
destinationPath = "s3://my/path/here/autoloaderInput/"
sleepIntervalSeconds = 10

while True:
  writeJsonFile(recordCount, tempPath, destinationPath)
  time.sleep(sleepIntervalSeconds)


# COMMAND ----------

df = spark.read.format("json").load("s3://my/path/here/autoloaderInput/")
display(df)

# COMMAND ----------


