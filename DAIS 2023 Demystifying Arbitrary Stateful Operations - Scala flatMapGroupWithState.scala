// Databricks notebook source
// MAGIC %md
// MAGIC ## This is the Scala implementation of the example discussed during the DAIS 2023 presentation: Demystifying Arbitrary Stateful Operations
// MAGIC ### The use case description - transaction count within the last five minutes:
// MAGIC * When a transaction record is received for a user, the count of transactions for that user that occurred within the last 5 minutes of the transaction time is calculated and written to a table.
// MAGIC * Only the current count for a given user is kept. If no transactions are received for a user, the count should automatically go down.  An ML model uses the count to determine if too many have occurred within the last 5 minutes.

// COMMAND ----------

// These can also be in the cluster settings - they will automatically compact sets of small files into larger files as the stream writes to Delta for more optimal read performance
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

// This setting will automatically allow schema evolution of the target Delta table with merge statements
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Define paths for input and checkpoint

// COMMAND ----------

// Path that the autoloader is pointed to
val autoloaderIngest = "s3://my/path/here/autoloaderInput"

// Checkpoint location for the transaction count Delta table
val aggregationCheckpointPath = "s3://my/path/here/checkpoints/aggregationtable"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Define the output table

// COMMAND ----------

// MAGIC %sql
// MAGIC -- This is creating the database and table in the metastore if they don't already exist 
// MAGIC create database if not exists streamtest;
// MAGIC create table if not exists streamtest.aggregationtable (userId Long, purchaseCount Int, eventTimestamp Timestamp, isTimeout Boolean)
// MAGIC using delta 
// MAGIC location 's3://my/path/here/aggregationtable'

// COMMAND ----------

// MAGIC %md
// MAGIC ### Define flatMapGroupsWithState logic
// MAGIC * This is the Python version of arbitrary stateful operations
// MAGIC * This logic will keep track of all the transactions that occurred in the previous 5 minutes for a given user and update the count every time new transactions are received for that user  
// MAGIC * If no transactions have been received after 1 minute for a given user, the logic will still emit a count and will remove records from state that are more than 5 minutes old
// MAGIC * If the stream has no data coming through at all then nothing will be updated.  Something must be coming through the stream for this logic to be executed
// MAGIC * The tranactionCountMinutes and maxRecordIntervalMinutes variables can be updated below to change how far back in time to count records and how often a new count will be emitted if no new records for a user are received
// MAGIC * NOTE - the case class definitions have to be in the same notebook cell as the updateState function

// COMMAND ----------

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}

// How far back in minutes to count transactions.  This will be used to calculate when a record should be removed from the state and no longer counted
val transactionCountMinutes = 5

// The maximum amount of minutes to wait before emitting a record
val maxRecordIntervalMinutes = 1

// The case class for the structure of the input data - a user and a transaction (purchase)
case class InputRow(userId: Long, transactionTimestamp: java.time.Instant)
// The case class for the state - this is what the stream is storing so that it can count the number of transactions for a user within the last 5 minutes
case class PurchaseCountState(latestTimestamp: java.time.Instant, currentPurchases: List[InputRow])
// The case class for the values being emitted - the key (user), the count of purchases in the last 5 minutes, the event datetime that triggered this update and a boolean 
// indicating whether the update was triggered by a timeout, meaning a record wasn't received for the user within a minute
case class PurchaseCount(userId: Long, purchaseCount: Integer, eventTimestamp: java.time.Instant, isTimeout: Boolean) 

// A function that will remove records that are more than 5 minutes old from the state
def removeExpiredRecords(newLatestTimestamp: java.time.Instant, currentPurchases: List[InputRow]): PurchaseCountState = {
  val newPurchaseList = ListBuffer[InputRow]()
  
  // Calculate the state expiration timestamp - the latest timestamp minus the transaction count minutes
  val expirationTimestamp = Instant.ofEpochMilli(newLatestTimestamp.toEpochMilli() - TimeUnit.MINUTES.toMillis(transactionCountMinutes))
  
  // If there are records in state, loop through the list of current purchases and remove any that are less than the expiration timestamp
  if (currentPurchases.size > 0) {
    currentPurchases.foreach { value => 
      if (value.transactionTimestamp.toEpochMilli() >= expirationTimestamp.toEpochMilli())
        newPurchaseList.append(value)
    }
  }
  // Create new PurchaseCountState object and return
  new PurchaseCountState(newLatestTimestamp, newPurchaseList.toList)
}

// A function that adds new records to the state
def addNewRecords(newRecords: List[InputRow], purchaseCountState: PurchaseCountState): PurchaseCountState = {
  // Get the latest timestamp in the set of new records
  val recWithLatestTimestamp = newRecords.maxBy(rec => rec.transactionTimestamp)
  val latestNewTimestamp = recWithLatestTimestamp.transactionTimestamp
  
  // Compare to the latestTimestamp in the purchaseCountState, use whichever is greater
  // This is in case we've received data out of order
  val latestTimestamp = if (latestNewTimestamp.toEpochMilli() > purchaseCountState.latestTimestamp.toEpochMilli()) latestNewTimestamp else purchaseCountState.latestTimestamp
  
  // Create a new PurchaseCountState object with the latest timestamp and combining the two record lists and return
  new PurchaseCountState(latestTimestamp, purchaseCountState.currentPurchases ::: newRecords)
}


// This is the function that is called with flatMapGroupsWithState.  It keeps track of the last 5 minutes of records for each key so that each time new data is received
// it can count the number of transactions that occurred in the last 5 minuts.
// This function will be called in two ways -
//   If one or more records for a given user are received.  In that case it will add those records to the state, remove any records that are older than 5 minuts from the state and calculate the count
//   If no records are received for a given user within a minute since the last time this function was called.  In that case it will remove any records that are older than 5 minutes from the state and calculate the count
def updateState(
  userId: Long,  // This is the key we are grouping on
  values: Iterator[InputRow],  // This is the format of the records coming into the function
  state: GroupState[PurchaseCountState]): Iterator[PurchaseCount] = {   // This declares the format of the state records we're storing and the format of the records we're outputting
  
  // Create a new ListBuffer to store what we're going to return at the end of processing this key
  val purchaseCounts = ListBuffer[PurchaseCount]()
  
  // If we haven't timed out then there are values for this key
  if (!state.hasTimedOut) {
    // There can be one or more records for this key.  Iterate through them and put them in a list 
    val transactionList = new ListBuffer[InputRow]()
    values.foreach { value =>
      transactionList.append(value)
    }

    // Now get the previous state if it exists.  If it doesn't exist (if this is the first time we've received a record for this user the state won't exist yet) then set the initial state to the 
    // transactionTimestamp of the first input record and an empty List of InputRow
    var prevState = state.getOption.getOrElse {
      val firstTransactionTimestamp = transactionList.head.transactionTimestamp
      new PurchaseCountState(firstTransactionTimestamp, List[InputRow]())
    }
    
    // Add the new records to the state
    val stateWithNewRecords = addNewRecords(transactionList.toList, prevState)
    
    // Remove expired records from the state
    // After this function only the transactions that occurred within the last five minutes from the latest transsaction will be in the state
    val stateWithRecordsRemoved = removeExpiredRecords(stateWithNewRecords.latestTimestamp, stateWithNewRecords.currentPurchases)
    
    // Create the output record - the key (user), count of transactions in the last 5 minutes, the latest timestamp and a boolean indicating this record was not triggered by a timeout
    val output = new PurchaseCount(userId, stateWithRecordsRemoved.currentPurchases.size, stateWithRecordsRemoved.latestTimestamp, false)
    purchaseCounts.append(output)
    
    // Save the state
    state.update(stateWithRecordsRemoved)
    
    // When no data has been seen for a period of time for a given key, this timeout will trigger the else clause below
    // The timeout will only trigger after the watermark has moved past this timestamp.  So for example if we're allowing data to be up to 30 seconds late,
    // then this timeout will trigger at the configured timestamp plus 30 seconds
    // Since this is our steady-state logic and we have records for this key, set the timeout to the latest transactionTimestamp that's in state plus 30 seconds.  If no data is seen
    // for this key for 30 seconds past the latest transactionTimestamp in the state plus the watermark time, then this function will be triggered again to remove expired records and emit a count
    // Since the watermark is set at 30 seconds then this timeout will trigger approximately once per minute
    state.setTimeoutTimestamp(stateWithRecordsRemoved.latestTimestamp.toEpochMilli(), "30 seconds")
  } else {
    // Since a timeout was triggered that means there was no input for this key
    // Use now as the new timestamp for the state
    val prevState = state.get
    val newTimestamp = Instant.now
    
    // Remove expired records from the state
    // After this function only the transactions that occurred within the last five minutes from the latest transsaction will be in state
    val stateWithRecordsRemoved = removeExpiredRecords(newTimestamp, prevState.currentPurchases)
    
    // Create the output record - the key (user), count of transactions in the last 5 minutes, the latest timestamp and a boolean indicating this record was triggered by a timeout
    // If all the transactions were removed then the count will be 0
    val output = new PurchaseCount(userId, stateWithRecordsRemoved.currentPurchases.size, stateWithRecordsRemoved.latestTimestamp, true)
    purchaseCounts.append(output)
    
    // *** From this point on this is an ineficient implementation - it blindly updates the state and sets up a new timeout even when there are no records left in state, which means it will keep calling this function every minute even if
    // *** no new records are received for a user for a long time.  Instead, you can add a conditional that removes the state completely for the user once it has no transactions left to count
    // *** and then don't set a new timeout.  When a record for that user is received again in the future, it'll go through the normal initialization logic

    // Save the state
    state.update(stateWithRecordsRemoved)
    
    // Set the timeout to now plus 30 seconds.  If no data is seen for this key in the next 30 seconds plus the watermark then this function will be triggered again to remove expired records and emit a count
    // Since the watermark is set at 30 seconds then this timeout will trigger approximately once per minute
    state.setTimeoutTimestamp(stateWithRecordsRemoved.latestTimestamp.toEpochMilli(), "30 seconds")
  }
  
  // Return an iterator of records from flatMapGroupsWithState.  In this use case the output will contain one record
  // It is valid to return an empty Iterator
  // Important - do NOT use a return statement.  Return functions differently in Scala vs. Python or Java, and you could end up returning an empty iterator
  purchaseCounts.toIterator
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Ingest data with Auto Loader - this can be swapped out for other sources such as Kafka

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, StructField, StructType, LongType, DoubleType, TimestampType}

// The schema for the incomming records.  Only needed if the Auto Loader's schema inference feature is not being used
val testSchema = StructType(Array(StructField("stringCode", StringType, true),
                 StructField("transactionTimestamp", StringType, true),
                 StructField("userId", LongType, true),
                 StructField("value1", DoubleType, true),
                 StructField("value2", DoubleType, true),
                 StructField("value3", DoubleType, true)
                ))

// COMMAND ----------

// Read from cloud storage using the Auto Loader
// There are two ways to use the Auto Loader - Directory listing and Notifications.  Directory listing is the default, and only requires permissions on the cloud bucket that you want to read
// See https://docs.databricks.com/ingestion/auto-loader/index.html for documentation
val testInputDf =
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("header", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.validateOptions", "true")
    .schema(testSchema)
    .load(autoloaderIngest)
    .selectExpr("userId", "cast(transactionTimestamp as timestamp) transactionTimestamp")
    .as[InputRow]  // Specifically set the type of the Dataframe to the case class that flatMapGroupsWithState is expecting


// COMMAND ----------

// MAGIC %md
// MAGIC ### Call flatMapGroupsWithState to calculate the count of records that occurred in the last 5 minutes for each user

// COMMAND ----------

// Notice the watermark, which is required since we're using an event time-based timeout.  We're allowing incoming data to be 30 seconds late before it is dropped
// The output mode and type of timeout are passed to the flatMapGroupsWithState call, along with the function that was defined above
// The timeout is optional - if you don't need to use them then specify GroupStateTimeout.NoTimeout as the timeout parameter
val flatMapGroupsWithStateResultDf = 
  testInputDf
    .withWatermark("transactionTimestamp", "30 seconds")
    .groupByKey(_.userId)
    .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout)(updateState)


// COMMAND ----------

// MAGIC %md
// MAGIC ### Write out the results - merge into a Delta table with the latest counts per user using the foreachBatch sink

// COMMAND ----------

import io.delta.tables._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger

// Function for foreachBatch to update the counts in the Delta table
def updateCounts(newCountsDs: Dataset[PurchaseCount], ephoch_id: Long): Unit = {
  // Convert the dataset (which was output by the mapGroupsWithState function) to a dataframe for merging
  val newCountsDf = newCountsDs.toDF
  
  // Get the target Delta table that is being merged
  val aggregationTable = DeltaTable.forName("streamtest.aggregationtable")
  
  // Merge the new records into the target Delta table.  This can be done with SQL syntax as well
  aggregationTable.alias("t")
    .merge(newCountsDf.alias("m"), "m.userId = t.userId")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}

// Save the flatMapGroupsWithState result to a Delta table.  The foreachBatch sink is used so that the Delta merge can be executed as a batch operation
// Always define a checkpoint location, and always name your stream so it shows up with a readable name in the Spark UI
flatMapGroupsWithStateResultDf.writeStream
  .foreachBatch(updateCounts _)
  .option("checkpointLocation", aggregationCheckpointPath)
  .trigger(Trigger.ProcessingTime("5 seconds"))
  .queryName("testFlatMapGroups")
  .start()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Query the data while the stream is running to see the updates

// COMMAND ----------

// MAGIC %sql
// MAGIC -- You can also do a readStream from the Delta path in Scala and call display
// MAGIC select * from streamtest.aggregationtable
// MAGIC order by userId

// COMMAND ----------


