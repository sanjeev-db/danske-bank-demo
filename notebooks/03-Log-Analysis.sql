-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DLT pipeline log analysis
-- MAGIC 
-- MAGIC Please Make sure you specify your own Database and Storage location. You'll find this information in the configuration menu of your Delta Live Table Pipeline.
-- MAGIC 
-- MAGIC **NOTE:** Please use Databricks Runtime 9.1 or above when running this notebook
-- MAGIC 
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt%2Fnotebook_dlt_log&dt=DLT">
-- MAGIC <!-- [metadata={"description":"Analyse DLT logs to get expectations metrics",
-- MAGIC  "authors":["dillon.bostwick@databricks.com"],
-- MAGIC  "db_resources":{},
-- MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "dlt"]}}] -->

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text('storage_location', '/pipelines/f159320b-2e51-43a9-95e2-9978920f8579', 'Storage Location')
-- MAGIC dbutils.widgets.text('db', 'danske_demo_db', 'DLT Database')

-- COMMAND ----------

-- MAGIC %python display(dbutils.fs.ls(dbutils.widgets.get('storage_location')))

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS $db;
USE $db;

-- COMMAND ----------

CREATE OR REPLACE VIEW pipeline_logs AS SELECT * FROM delta.`$storage_location/system/events`

-- COMMAND ----------

SELECT * FROM pipeline_logs ORDER BY timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
-- MAGIC * `user_action` Events occur when taking actions like creating the pipeline
-- MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
-- MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
-- MAGIC   * `flow_type` - whether this is a complete or append flow
-- MAGIC   * `explain_text` - the Spark explain plan
-- MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
-- MAGIC   * `metrics` - currently contains `num_output_rows`
-- MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
-- MAGIC     * `dropped_records`
-- MAGIC     * `expectations`
-- MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
-- MAGIC   

-- COMMAND ----------

-- DBTITLE 1,Lineage Information
-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   details:flow_definition.output_dataset,
-- MAGIC   details:flow_definition.input_datasets,
-- MAGIC   details:flow_definition.flow_type,
-- MAGIC   details:flow_definition.schema,
-- MAGIC   details:flow_definition
-- MAGIC FROM pipeline_logs
-- MAGIC WHERE details:flow_definition IS NOT NULL
-- MAGIC ORDER BY timestamp

-- COMMAND ----------

drop table dlt_expectations

-- COMMAND ----------

-- DBTITLE 1,Data Quality Results
CREATE TABLE dlt_expectations AS
SELECT
  id,
  expectations.dataset,
  expectations.name,
  expectations.failed_records,
  expectations.passed_records,
  data_quality.dropped_records,
  data_quality.output_records,
  date_format(timestamp, "yyyy-MM-dd HH:mm:ss.SSSS") as timestamp
FROM(
  SELECT 
    id,
    timestamp,
    details:flow_progress.metrics,
    details:flow_progress.data_quality.dropped_records,
    details:flow_progress.status as status_update,
    details:flow_progress.metrics.num_output_rows as output_records,
    explode(from_json(details:flow_progress:data_quality:expectations
             ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
  FROM pipeline_logs
  WHERE details:flow_progress.metrics IS NOT NULL) data_quality

-- COMMAND ----------

select sum(passed_records) as passed_records,
       sum(failed_records) as failed_records,
       sum(dropped_records) as dropped_records,
       sum(output_records) as output_records,
       sum(failed_records)/sum(output_records)*100 as failure_rate,
       name,
       dataset,
        date(timestamp) as date  from danske_demo_db.dlt_expectations values group by date, dataset, name;

-- COMMAND ----------

-- DBTITLE 1,Cluster performance metrics
SELECT
  timestamp,
  Double(details :cluster_utilization.num_executors) as current_num_executors,
  Double(details :cluster_utilization.avg_num_task_slots) as avg_num_task_slots,
  Double(
    details :cluster_utilization.avg_task_slot_utilization
  ) as avg_task_slot_utilization,
  Double(
    details :cluster_utilization.avg_num_queued_tasks
  ) as queue_size,
  Double(details :flow_progress.metrics.backlog_bytes) as backlog
FROM
  pipeline_logs
WHERE
  event_type IN ('cluster_utilization', 'flow_progress')
  AND origin.update_id = '9f208b04-ae36-48ee-b844-c8540d19226c'; 

-- COMMAND ----------


