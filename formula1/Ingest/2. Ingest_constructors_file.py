# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Constructors.json File

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-1 Read The JSON File using spark datafram reader 

# COMMAND ----------

# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

# Get Schema
constructors_schema = StructType ([StructField("constructorId",IntegerType()),\
                    StructField("constructorRef",StringType()),
                    StructField("name",StringType()),\
                    StructField("nationality",StringType()),\
                    StructField("url",StringType())
])

# COMMAND ----------

# Read the JSON file with Schema
constructors_df = spark.read.json(f"/mnt/bwtformula1project/bronze/constructors/constructors.json",schema=constructors_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the column and new column ingestion date

# COMMAND ----------

# Rename the column and ingestion
constructors_renamed_df = (
    constructors_df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorRef", "constructor_ref")
    )

# COMMAND ----------

circuit_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write the file in parquect 

# COMMAND ----------

#write the file 
circuit_final_df.write.mode("overwrite").parquet("/mnt/bwtformula1project/silver/constructors")

# COMMAND ----------

display(spark.read.parquet(f"/mnt/bwtformula1project/silver/constructors"))
