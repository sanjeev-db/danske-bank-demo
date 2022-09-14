# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Ingestion from Kafka topic
TOPIC = "tracker-events"
KAFKA_BROKER = spark.conf.get("KAFKA_SERVER")

# subscribe to TOPIC at KAFKA_BROKER
raw_kafka_events = (spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("startingOffsets", "earliest")
    .load()
    )

# COMMAND ----------

@dlt.table(table_properties={"pipelines.reset.allowed":"false"})
def kafka_bronze():
  return raw_kafka_events

# COMMAND ----------

# DBTITLE 1,Ingestion from Object storage - Autoloader
@dlt.create_table(comment="New raw loan data incrementally ingested from cloud object storage landing zone")
def BZ_raw_txs():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("/demo/danske-bank/dlt_loan/landing"))


# COMMAND ----------

# DBTITLE 1,Autoloader - SQL
#-- INGEST with Auto Loader
#  create or replace streaming live table raw
#  as select * FROM cloud_files("dbfs:/data/twitter", "json")
