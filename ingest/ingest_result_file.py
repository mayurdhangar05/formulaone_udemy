# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text('source_nm','results')
source_nm = dbutils.widgets.get('source_nm')

# COMMAND ----------

input_schema = StructType([
    StructField("resultId",IntegerType()),
    StructField("raceId",IntegerType()),
    StructField("driverId",IntegerType()),
    StructField("constructorId",IntegerType()),
    StructField("number",IntegerType()),
    StructField("grid",IntegerType()),
    StructField("position",IntegerType()),
    StructField("positionText",StringType()),
    StructField("positionOrder",IntegerType()),
    StructField("points",FloatType()),
    StructField("laps",IntegerType()),
    StructField("time",StringType()),
    StructField("milliseconds",IntegerType()),
    StructField("fastestLap",IntegerType()),
    StructField("rank",IntegerType()),
    StructField("fastestLapTime",StringType()),
    StructField("fastestLapSpeed",StringType()),
    StructField("statusId",IntegerType())

])

# COMMAND ----------

df = create_json_df(f"dbfs:/mnt/formulaone/bronze/{source_nm}.json",input_schema)
df.display()

# COMMAND ----------

result_df = (
    df.withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap", "fastest_lap")
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
    .withColumn("ingest_dt", current_timestamp())
)

result_df.display()

# COMMAND ----------

result_df.write.mode("overwrite").partitionBy("race_id").parquet(f'dbfs:/mnt/formulaone/silver/{source_nm}')
