# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest racess.json File

# COMMAND ----------

# MAGIC %md
# MAGIC ####step-1 Read The json File Using Spark Datafream Reader

# COMMAND ----------

# MAGIC
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

#Read The JSON File Using Spark Datafream Reader
races_df = spark.read.json("/mnt/bwtformula1project/bronze/Races/20240419")
races_df.printSchema()


# COMMAND ----------

races_df.display()

# COMMAND ----------

races_df = flatten(races_df)

# COMMAND ----------

races_df.display()

# COMMAND ----------

# race column list :['race_date', 'race_name', 'round', 'season', 'race_time', 'circuit_id']
races_selected_column_df = races_df.select(col("date").alias("race_date"),col("raceName").alias("race_name"),col("round"),col("season"),col("time").alias("race_time"),col("Circuit_circuitId").alias("circuit_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Add ingestion date to dataframe

# COMMAND ----------

races_final_df = (
    races_selected_column_df.withColumn("ingestion_date", current_timestamp())   
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 Write data to data lake as parquet file

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet(f"/mnt/bwtformula1project/silver/Races")

# COMMAND ----------

display(spark.read.parquet("/mnt/bwtformula1project/silver/Races"))
