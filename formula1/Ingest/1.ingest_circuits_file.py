# Databricks notebook source
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #Ingest Circuits.csv File

# COMMAND ----------

# MAGIC %md
# MAGIC ####step-1 Read The CSV File Using Spark Datafream Reader

# COMMAND ----------

circuit_schema = StructType([StructField("circuitId",IntegerType()),
                            StructField("circuitRef",StringType()),
                            StructField("name",StringType()),
                            StructField("location",StringType()),
                            StructField("country",StringType()),
                            StructField("lat",DoubleType()),
                            StructField("lng",DoubleType()),
                            StructField("alt",IntegerType()),
                            StructField("url",StringType()),
                                     
                            ])

# COMMAND ----------

#Read The CSV File Using Spark Datafream Reader
circuit_df = spark.read.csv("/mnt/bwtformula1project/bronze/circuits/circuits.csv",header=True,schema =circuit_schema)

# COMMAND ----------

circuit_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3 Rename the columns 

# COMMAND ----------

circuits_renamed_df = circuit_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")\



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-4 Add ingestion date to data fream

# COMMAND ----------

circuit_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-5 Write data to data lake as parquet file

# COMMAND ----------

circuit_final_df.write.mode("overwrite").parquet(f"/mnt/bwtformula1project/silver/Circuits")

# COMMAND ----------

display(spark.read.parquet("/mnt/bwtformula1project/silver/Circuits"))
