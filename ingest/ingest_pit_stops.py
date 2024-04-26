# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text('source_nm','pit_stops')
source_nm = dbutils.widgets.get('source_nm')

# COMMAND ----------

input_schema = StructType([
    StructField("raceId",IntegerType()),
    StructField("driverId",IntegerType()),
    StructField("stop",IntegerType()),
    StructField("lap",IntegerType()),
    StructField("time",StringType()),
    StructField("duration",StringType()),
    StructField("milliseconds",IntegerType())
])

# COMMAND ----------

df = create_multiLine_json_df(f"dbfs:/mnt/formulaone/bronze/{source_nm}.json",input_schema)
df.display()

# COMMAND ----------

pit_stops_df = (
    df.withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("ingest_dt",current_timestamp())
)

pit_stops_df.display()

# COMMAND ----------

pit_stops_df.write.mode("overwrite").parquet(f'dbfs:/mnt/formulaone/silver/{source_nm}')

# COMMAND ----------


