# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest pit_stopss.json File

# COMMAND ----------

# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-1 Read The JSON File using spark dataframe reader 

# COMMAND ----------

# Get Schema
pit_stopss_schema = StructType ([  StructField("driverId",IntegerType()),\
                                StructField("duration",StringType()),\
                                StructField("lap",IntegerType()),\
                                StructField("milliseconds",IntegerType()),\
                                StructField("raceId",IntegerType()),\
                                StructField("stop",StringType()),\
                                StructField("time",StringType()),\
                              
])

# COMMAND ----------

# Read the JSON file with Schema
pit_stopss_df = spark.read.json(f"/mnt/bwtformula1project/bronze/Pit_stops/20240419/")

# COMMAND ----------

pit_stopss_df = flatten(pit_stopss_df)

# COMMAND ----------

pit_stopss_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Rename columns and new columns
# MAGIC 1. raceid renamed race_id 
# MAGIC 2. driverId renamed to driver_id
# MAGIC 3. ingestion date added
# MAGIC

# COMMAND ----------

#Rename the column and ingestion 
pit_stopss_renamed_df = pit_stopss_df.withColumnRenamed("MRData_RaceTable_Races_date", 'race_date')\
                                .withColumnRenamed("MRData_RaceTable_Races_PitStops_stop","stops")\
                                    .withColumnRenamed("MRData_RaceTable_season","Season_id")\
                                        .withColumnRenamed("MRData_RaceTable_Races_raceName","race_name")
                                


# COMMAND ----------

pit_stopss_renamed_df1 = add_ingestion_date(pit_stopss_renamed_df)

# COMMAND ----------

df = pit_stopss_renamed_df1.filter((col("Season_id")==2021)&(col("race_name")=="Abu Dhabi Grand Prix"))
df = df.withColumn("MRData_RaceTable_Races_PitStops_duration",col("MRData_RaceTable_Races_PitStops_duration").cast("float"))
from pyspark.sql.window import Window
spec_win = Window.partitionBy("MRData_RaceTable_Races_PitStops_lap").orderBy(col("MRData_RaceTable_Races_PitStops_duration"))
df1 = df.withColumn("DRK",dense_rank().over(spec_win))
df1.filter("DRK==1").display()


# COMMAND ----------

pit_stopss_renamed_df1 = pit_stopss_renamed_df1.select("race_date","Season_id","stops","race_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-4 Write the file in parquet 

# COMMAND ----------

#write the file 
pit_stopss_renamed_df1.write.mode("overwrite").parquet(f"/mnt/bwtformula1project/silver/pit_stops")

# COMMAND ----------

pitstop_df = df.write.format("delta").save(f"/mnt/bwtformula1project/silver/pit_stops_delta_format")

# COMMAND ----------

display(spark.read.parquet("/mnt/bwtformula1project/silver/pit_stops/"))
