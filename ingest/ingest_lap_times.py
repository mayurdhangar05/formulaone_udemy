# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text('source_nm','lap_times')
source_nm = dbutils.widgets.get('source_nm')


# COMMAND ----------

input_schema = StructType([
    StructField("raceId",IntegerType()),
    StructField("driverId",StringType()),
    StructField("lap",IntegerType()),
    StructField("position",IntegerType()),
    StructField("time",StringType()),
    StructField("milliseconds",IntegerType())
])

# COMMAND ----------

df = create_csv_df("/mnt/formulaone/bronze/lap_times/",input_schema,header_status=False)
df.display()


# COMMAND ----------

lap_times_df = (
    df.withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("ingest_dt", current_timestamp())
)

lap_times_df.display()

# COMMAND ----------

lap_times_df.write.mode("overwrite").parquet(f'dbfs:/mnt/formulaone/silver/{source_nm}')
