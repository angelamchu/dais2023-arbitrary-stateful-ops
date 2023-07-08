# Databricks notebook source
# MAGIC %md
# MAGIC ## This is the Python implementation of the example discussed during the DAIS 2023 presentation: Demystifying Arbitrary Stateful Operations
# MAGIC ### The use case description - transaction count within the last five minutes:
# MAGIC * When a transaction record is received for a user, the count of transactions for that user that occurred within the last 5 minutes of the transaction time is calculated and written to a table.
# MAGIC * Only the current count for a given user is kept. If no transactions are received for a user, the count should automatically go down.  An ML model uses the count to determine if too many have occurred within the last 5 minutes.

# COMMAND ----------

# These can also be in the cluster settings - they will automatically compact sets of small files into larger files as the stream writes to Delta for more optimal read performance
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# This setting will automatically allow schema evolution of the target Delta table with merge statements
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define paths for input and checkpoint

# COMMAND ----------

# Path that the autoloader is pointed to
autoloaderIngest = "s3://my/path/here/autoloaderInput"

# Checkpoint location for the transaction count Delta table
aggregationCheckpointPath = "s3://my/path/here/checkpoints/aggregationtablePython"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the output table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This is creating the database and table in the metastore if they don't already exist 
# MAGIC create database if not exists streamtest;
# MAGIC create table if not exists streamtest.aggregationtablePython (userId Long, purchaseCount Int, eventTimestamp Timestamp, isTimeout Boolean, stateList String)
# MAGIC using delta 
# MAGIC location 's3://my/path/here/aggregationtablePython'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define applyInPandasWithState logic
# MAGIC * This is the Python version of arbitrary stateful operations
# MAGIC * This logic will keep track of all the transactions that occurred in the previous 5 minutes for a given user and update the count every time new transactions are received for that user  
# MAGIC * If no transactions have been received after 1 minute for a given user, the logic will still emit a count and will remove records from state that are more than 5 minutes old
# MAGIC * If the stream has no data coming through at all then nothing will be updated.  Something must be coming through the stream for this logic to be executed
# MAGIC * The tranactionCountMinutes and maxRecordIntervalMinutes variables can be updated below to change how far back in time to count records and how often a new count will be emitted if no new records for a user are received
# MAGIC * Check https://www.databricks.com/blog/2022/10/18/python-arbitrary-stateful-processing-structured-streaming.html

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta
from collections import namedtuple
from typing import Tuple, Iterator, List
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, ArrayType, TimestampType, BooleanType
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

# How far back in minutes to count transactions.  This will be used to calculate when a record should be removed from the state and no longer counted
transactionCountMinutes = 5

# The maximum amount of minutes to wait before emitting a record
maxRecordIntervalMinutes = 1

# Documented for completeness - the expected structure of the input
#inputSchema = "userId LONG, transactionTimestamp TIMESTAMP"

# The schema for the state - this is what the stream is storing so that it can count the number of transactions for a user within the last 5 minutes
# The fields are the latest timestamp that was received for this key (user) and a list of transaction timestamps received for this key
# Examples of both StructType syntax and DDL
#stateSchema =  StructType([StructField("latestTimestamp", TimestampType()), StructField("currentPurchases", ArrayType(TimestampType()))])
stateSchema = "latestTimestamp TIMESTAMP, currentPurchases ARRAY<TIMESTAMP>"

# The schema in DDL form for the values being emitted - the key (user), the count of purchases in the last 5 minutes, the event datetime that triggered this update, a boolean 
# indicating whether the update was triggered by a timeout, meaning a record wasn't received for the user within a minute, and a list of all the timestamps in state for this key
# in the form of a string for debugging purposes
outputSchema = "userId LONG, purchaseCount INT, eventTimestamp TIMESTAMP, isTimeout BOOLEAN, stateList STRING"

# A named tuple with the structure of the state.  Since the state is referred to across multiple functions in this case, a named tuple will make the rest of the code more readable
State = namedtuple("State", "latestTimestamp currentPurchases")

# A function that will remove records that are more than 5 minutes old from the state
# Parameter types that are expected: datetime, List(datetime)
# Returns the latest timestamp and the list of transaction timestamps remaining in state as a State tuple
def removeExpiredRecords(newLatestTimestamp: datetime, currentPurchases: List[datetime]):
  # Calculate the state expiration timestamp - the latest timestamp minus the transaction count minutes
  expirationTimestamp = newLatestTimestamp - timedelta(minutes=transactionCountMinutes)
  
  # If there are records in state, loop through the list of current purchases and remove any that are less than the expiration timestamp
  newPurchaseList: List[datetime] = []
  if currentPurchases:
    for purchase in currentPurchases: 
      if (purchase >= expirationTimestamp):
        newPurchaseList.append(purchase)
    
  return State(newLatestTimestamp, newPurchaseList)

# A function that adds new records to the state
# Parameter types that are expected: the new records as a list of datetime, the current state object as a State tuple
# Returns the new latest timestamp and the updated list of purchases as a State tuple
def addNewRecords(newRecords: List[datetime], purchaseCountState):
  # Get the latest timestamp in the set of new records
  newLatestTimestamp: datetime = max(newRecords)
  
  # Compare to the latestTimestamp in the purchaseCountState, use whichever is greater
  # This is in case we've received data out of order
  if (newLatestTimestamp < purchaseCountState.latestTimestamp):
    newLatestTimestamp = purchaseCountState.latestTimestamp 
  
  # Return the updated state, with the latest timestamp and the updated list of purchases
  return State(newLatestTimestamp, purchaseCountState.currentPurchases + newRecords)


# This is the function that is called with applyInPandasWithState.  It keeps track of the last 5 minutes of records for each key so that each time new data is received 
# it can count the number of transactions that occurred in the last 5 minuts.
# This function will be called in two ways -
#   If one or more records for a given user are received.  In that case it will add those records to the state, remove any records that are older than 5 minuts from the state and calculate the count
#   If no records are received for a given user within a minute since the last time this function was called.  In that case it will remove any records that are older than 5 minutes from the state and calculate the count
def updateState (
  key: Tuple[int],  # This is the key we are grouping on.  It can be a tuple of one or more fields
  values: Iterator[pd.DataFrame],  # These are the records coming into the function
  state: GroupState # The state we're storing in-between microbatches
) -> Iterator[pd.DataFrame]:   # The records we're outputting

  # If we haven't timed out then there are values for this key
  if not state.hasTimedOut:
    # There can be one or more records for this key.  Iterate through them and put the transaction timestamps into a list
    # Our input in this use case is rows of userId and transactionTimestamp
    transactionList: List[datetime] = []
    for value in values:
      transactionList = transactionList + value["transactionTimestamp"].tolist()

    # Now get the previous state if it exists.  If it doesn't exist (if this is the first time we've received a record for this user the state won't exist yet) then set the initial state to the 
    # maximum transactionTimestamp from the input list and an empty List of datetime
    maxTimestamp: datetime = max(transactionList)
    prevState = State(maxTimestamp, [])
    if state.exists:
      (latestTimestamp, currentPurchases) = state.get
      prevState = State(latestTimestamp, currentPurchases)
    
    # Add the new records to the state
    stateWithNewRecords = addNewRecords(transactionList, prevState)
    
    # Remove expired records from the state
    # After this function only the transactions that occurred within the last five minutes from the latest transaction will be in the state object
    stateWithRecordsRemoved = removeExpiredRecords(stateWithNewRecords.latestTimestamp, stateWithNewRecords.currentPurchases)
    
    # Save the state
    state.update(stateWithRecordsRemoved)
    
    # When no data has been seen for a period of time for a given key, this timeout will trigger the else clause below
    # The timeout will only trigger after the watermark has moved past this timestamp.  So for example if we're allowing data to be up to 30 seconds late,
    # then this timeout will trigger at the configured timestamp plus 30 seconds
    # Since this is our steady-state logic and we have new records for this key, set the timeout to the latest transactionTimestamp that's in state plus 30 seconds.  If no data is seen
    # for this key for 30 seconds past the latest transactionTimestamp in the state plus the watermark time, then this function will be triggered for this key to remove expired records and emit a count
    # Since the watermark is set at 30 seconds then this timeout will trigger approximately once per minute
    # This converts the latest timestamp in state to milliseconds and then adds 30 seconds.  In Python setTimeoutTimestamp takes a millisecond value
    timeoutMs = int((stateWithRecordsRemoved.latestTimestamp.timestamp() *1000) + 30000)
    state.setTimeoutTimestamp(timeoutMs)

    # Create the output record and return - the key (user), count of transactions in the last 5 minutes, the latest timestamp and a boolean indicating this record was not triggered by a timeout
    # That comma isn't a typo - since the key in this case is a tuple with just one value, you need the hanging comma to get the value instead of the object
    (userId,) = key
    purchaseCount = len(stateWithRecordsRemoved.currentPurchases)
    eventTimestamp = stateWithRecordsRemoved.latestTimestamp
    isTimeout = False

    # Get the current list of timestamps in state as a string and return as part of the output for debugging purposes
    stateList = []
    for purchase in stateWithRecordsRemoved.currentPurchases:
      stateList.append(purchase.strftime('%Y-%m-%dT%H:%M:%S.%f'))
    stateListString = ",".join(stateList)

    # Using yield here will return a generator object for this Pandas Dataframe
    yield pd.DataFrame({"userId": [userId], "purchaseCount": [purchaseCount], "eventTimestamp": [eventTimestamp], "isTimeout": [isTimeout], "stateList": [stateListString]})

  else:
    # Since a timeout was triggered that means there was no input for this key
    # Use now as the new maximum timestamp for the state
    (currentLatestTimestamp, currentPurchases) = state.get
    newTimestamp = datetime.now()

    # Remove expired records from the state if there are any
    stateWithRecordsRemoved = State(newTimestamp, [])
    if currentPurchases:
      # After this function only the transactions that occurred within the last five minutes from the latest transaction will be in state
      stateWithRecordsRemoved = removeExpiredRecords(newTimestamp, currentPurchases)
    
    # *** From this point on this is an ineficient implementation - it blindly updates the state and sets up a new timeout even when there are no records left in state, which means it will keep calling this function every minute even if
    # *** no new records are received for a user for a long time.  Instead, you can add a conditional that removes the state completely for the user once it has no transactions left to count
    # *** and then don't set a new timeout.  When a record for that user is received again in the future, it'll go through the normal initialization logic

    # Save the new state
    state.update(stateWithRecordsRemoved)

    # Set the new timeout to now plus 30 seconds.  If no data is seen for this key in the next 30 seconds plus the watermark then this function will be triggered again to remove expired records and emit a count
    # Since the watermark is set at 30 seconds then this timeout will trigger approximately once per minute
    # This converts the current time to milliseconds and then adds 30 seconds.  In Python setTimeoutTimestamp takes a millisecond value
    timeoutMs = int((newTimestamp.timestamp() *1000) + 30000)
    state.setTimeoutTimestamp(timeoutMs)

    # Create the output record and return - the key (user), count of transactions in the last 5 minutes, the latest timestamp and a boolean indicating this record was triggered by a timeout
    # If there were no transactions left in state then the count will be returned as 0
    (userId,) = key
    purchaseCount = len(stateWithRecordsRemoved.currentPurchases)
    eventTimestamp = stateWithRecordsRemoved.latestTimestamp
    isTimeout = True
    
     # Get the current list of timestamps as a string and return as part of the state for debugging purposes
    stateList = []
    for purchase in stateWithRecordsRemoved.currentPurchases:
      stateList.append(purchase.strftime('%Y-%m-%dT%H:%M:%S.%f'))
    stateListString = ",".join(stateList)

    # Generate and return the output.  Note it is valid to return an empty Pandas Dataframe here
    yield pd.DataFrame({"userId": [userId], "purchaseCount": [purchaseCount], "eventTimestamp": [eventTimestamp], "isTimeout": [isTimeout], "stateList": [stateListString]})
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest data with Auto Loader - this can be swapped out for other sources such as Kafka

# COMMAND ----------

# The schema for the incomming records.  Only needed if the Auto Loader's schema inference feature is not being used
testSchema = StructType([StructField("stringCode", StringType(), True),
                 StructField("transactionTimestamp", StringType(), True),
                 StructField("userId", LongType(), True),
                 StructField("value1", DoubleType(), True),
                 StructField("value2", DoubleType(), True),
                 StructField("value3", DoubleType(), True)
])

# COMMAND ----------

# Read from cloud storage using the Auto Loader
# There are two ways to use the Auto Loader - Directory listing and Notifications.  Directory listing is the default, and only requires permissions on the cloud bucket that you want to read
# See https://docs.databricks.com/ingestion/auto-loader/index.html for documentation
testInputDf = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("header", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.validateOptions", "true")
    .schema(testSchema)
    .load(autoloaderIngest)
    .selectExpr("userId", "cast(transactionTimestamp as timestamp) transactionTimestamp")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Call applyInPandasWithState to calculate the count of records that occurred in the last 5 minutes for each user

# COMMAND ----------

# Notice the watermark, which is required since we're using an event time-based timeout.  We're allowing incoming data to be 30 seconds late before it is dropped
# The name of the function that was defined above is passed to the applyInPandasWithState call, along with the DDL schemas for the state and the output, the output mode and the type of timeout
# The timeout is optional - if you don't need to use them then specify GroupStateTimeout.NoTimeout as the timeout parameter
applyInPandasWithStateResultDf = (
  testInputDf
    .withWatermark("transactionTimestamp", "30 seconds")
    .groupBy(testInputDf["userId"])
    .applyInPandasWithState(updateState, outputSchema, stateSchema, "append", GroupStateTimeout.EventTimeTimeout)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write out the results - merge into a Delta table with the latest counts per user using the foreachBatch sink

# COMMAND ----------

from delta.tables import *

# Function for foreachBatch to update the counts in the Delta table
def updateCounts(newCountsDf, ephoch_id):
  # Get the target Delta table that is being merged
  aggregationTable = DeltaTable.forName(spark, "streamtest.aggregationtablePython")
  
  # Merge the new records into the target Delta table.  This can be done with SQL syntax as well
  aggregationTable.alias("t").merge(
      newCountsDf.alias("m"), 
      "m.userId = t.userId") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Save the applyInPandasWithState result to a Delta table.  The foreachBatch sink is used so that the Delta merge can be executed as a batch operation
# Always define a checkpoint location, and always name your stream so it shows up with a readable name in the Spark UI.
# Yes I know I forgot to change the queryName from the Scala implementation :) 
applyInPandasWithStateResultDf.writeStream \
  .foreachBatch(updateCounts) \
  .option("checkpointLocation", aggregationCheckpointPath) \
  .trigger(processingTime="5 seconds") \
  .queryName("testFlatMapGroups") \
  .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the data while the stream is running to see the updates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can also do a readStream from the Delta path in Python and call display
# MAGIC select * from streamtest.aggregationtablePython
# MAGIC order by userId

# COMMAND ----------


