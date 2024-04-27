# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Seasons.json File

# COMMAND ----------

# MAGIC
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-1 Read The JSON File using spark dataframe reader 

# COMMAND ----------

# Read the JSON file with Schema
season_df = spark.read.json("/mnt/bwtformula1project/bronze/Seasons/20240419/")
season_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-4 Write the file in parquet 

# COMMAND ----------

#write the file 
season_df.write.mode("overwrite").parquet(f"/mnt/bwtformula1project/silver/Seasons")

# COMMAND ----------

display(spark.read.parquet("/mnt/bwtformula1project/silver/Seasons"))
