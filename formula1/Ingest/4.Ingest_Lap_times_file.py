# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest lap_times.csv File

# COMMAND ----------

# MAGIC
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-1 Read The JSON File using spark datafram reader 

# COMMAND ----------

# Get Schema
lap_times_schema = StructType ([  StructField("raceId",IntegerType()),\
                                StructField("driverId",IntegerType()),\
                                StructField("lap",IntegerType()),\
                                StructField("positions",IntegerType()),\
                                StructField("time",StringType()),\
                                StructField("milliseconds",IntegerType()),\
                                
                              
])

# COMMAND ----------

# Read the JSON file with Schema
lap_times_df = spark.read.csv(f"/mnt/bwtformula1project/bronze/lap_times/lap_times_split*",schema=lap_times_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Rename columns and new columns
# MAGIC 1. raceid renamed race_id 
# MAGIC 2. driverId renamed to driver_id
# MAGIC 3. ingestion date added
# MAGIC

# COMMAND ----------

#Rename the column and ingestion 
lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", 'race_id')\
                                .withColumnRenamed("driverId","driver_id")\
                             


# COMMAND ----------

lap_times_renamed_df1 = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-4 Write the file in parquect 

# COMMAND ----------

#write the file 
lap_times_renamed_df1.write.mode("overwrite").parquet(f"/mnt/bwtformula1project/silver/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/bwtformula1project/silver/lap_times"))
