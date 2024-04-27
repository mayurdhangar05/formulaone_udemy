# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest results.json File

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-1 Read The JSON File using spark dataframe reader 

# COMMAND ----------

# MAGIC
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

# Get Schema
results_schema = StructType ([  StructField("constructorId",IntegerType()),\
                                StructField("driverId",IntegerType()),\
                                StructField("fastestLap",IntegerType()),\
                                StructField("fastestLapSpeed",FloatType()),\
                                StructField("fastestLapTime",StringType()),\
                                StructField("grid",IntegerType()),\
                                StructField("laps",IntegerType()),\
                                StructField("milliseconds",IntegerType()),\
                                StructField("number",IntegerType()),\
                                StructField("points",FloatType()),\
                                StructField("position",IntegerType()),\
                                StructField("positionOrder",IntegerType()),\
                                StructField("positionText",StringType()),\
                                StructField("raceId",IntegerType()),\
                                StructField("rank",IntegerType()),\
                                StructField("resultId",IntegerType()),\
                                StructField("statusId",StringType()),\
                                StructField("time",StringType()),\
                                
                ])

# COMMAND ----------

# Read the JSON file with Schema
results_df = spark.read.json("/mnt/bwtformula1project/bronze/Results/20240419/")

# COMMAND ----------

results_df1 = flatten(results_df)

# COMMAND ----------

# results_df1.display()

# COMMAND ----------

results_col_df1 = results_df1.select(
    col("date").alias("race_date"),
    col("season").alias("season_id"),
    col("round").alias("round_id"),
    col("Results_Time_time").alias("race_time"),
    col("Circuit_circuitId").alias("circuit_id"),
    col("Results_Constructor_constructorId").alias("constructor_id"),
    col("Results_Driver_driverId").alias("driver_id"),
    col("Results_FastestLap_Time_time").alias("fastest_lap_detail"),
    col("Results_grid").alias("grid"),
    col("Results_laps").alias("laps"),
    col("Results_position").alias("result_position"),
    col("Results_points").alias("points"),
    col("Results_positionText").alias("position_text")
)

# COMMAND ----------

results_col_df = add_ingestion_date(results_col_df1)

# COMMAND ----------

results_col_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-4 Write the file in parquet 

# COMMAND ----------

#  write the file 
results_col_df.write.mode("overwrite").parquet("/mnt/bwtformula1project/silver/Results")
