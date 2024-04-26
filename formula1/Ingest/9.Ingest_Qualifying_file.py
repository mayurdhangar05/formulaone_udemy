# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying.json File

# COMMAND ----------

# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-1 Read The JSON File using spark datafram reader 

# COMMAND ----------

# Get Schema
qualifying_schema = StructType ([   StructField("qualifyId",IntegerType()),\
                                    StructField("raceId",IntegerType()),\
                                    StructField("driverId",IntegerType()),\
                                    StructField("constructorId",IntegerType()),\
                                    StructField("number",IntegerType()),\
                                    StructField("position",IntegerType()),\
                                    StructField("q1",StringType()),\
                                    StructField("q2",StringType()),\
                                    StructField("q3",StringType()),\
                                                            
    ])

# COMMAND ----------

qualifying_df = spark.read.json("/mnt/bwtformula1project/bronze/qualifying",multiLine=True,schema=qualifying_schema)

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Rename columns and new columns
# MAGIC 1. qualifyId renamed qualify_id 
# MAGIC 2. raceId renamed to race_id
# MAGIC 3. driverId renamed driver_id 
# MAGIC 4. constructorId renamed constructor_id 
# MAGIC 5. ingestion date added
# MAGIC

# COMMAND ----------

#Rename the column and ingestion 
qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyId", 'qualify_id')\
                                .withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId", 'driver_id')\
                                .withColumnRenamed("constructorId","constructor_id")\
                                .withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-4 Write the file in parquect 

# COMMAND ----------

#write the file 
qualifying_renamed_df.write.mode("overwrite").parquet("/mnt/bwtformula1project/silver/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
