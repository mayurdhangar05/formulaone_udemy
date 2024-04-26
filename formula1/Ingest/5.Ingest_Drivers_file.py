# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest drivers.json File

# COMMAND ----------

# MAGIC
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-1 Read The JSON File using spark datafram reader 

# COMMAND ----------

# Read the JSON file with Schema
drivers_df = spark.read.json("/mnt/bwtformula1project/bronze/Drivers/20240419/")
drivers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Rename columns and new columns
# MAGIC 1. driver renamed driver_id 
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname 
# MAGIC

# COMMAND ----------

#Rename the column and ingestion 
drivers_renamed_df = drivers_df.withColumn("name",concat(col("givenName"),lit(" "),"familyName"))\
                                .withColumnRenamed("driverId", 'driver_id')\
                                .withColumnRenamed("driverRef","driver_ref")\
                                .withColumn("ingestion_date",current_timestamp())\
                                         
                                         

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-3 Drop the unwanted columna
# MAGIC 1. name.forename, name.surname , url 

# COMMAND ----------

#select required columns
drivers_final_df = drivers_renamed_df.drop("url","givenName","familyName")


# COMMAND ----------

drivers_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-4 Write the file in parquect 

# COMMAND ----------

#write the file 
drivers_final_df.write.mode("overwrite").parquet(f"/mnt/bwtformula1project/silver/Drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/bwtformula1project/silver/Drivers"))
