# Databricks notebook source
# MAGIC %md
# MAGIC ####1.Read csv file using spark dataframe reader
# MAGIC ####2.Apply schema over it
# MAGIC ####3.Rename & remove columns based on the requirement

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("source_nm","circuits")
source_nm = dbutils.widgets.get("source_nm")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formulaone/bronze/circuits.csv/"))

# COMMAND ----------

# df.describe().show()

# COMMAND ----------

input_schema = StructType([
    StructField("circuitId",IntegerType()),
    StructField("circuitRef",StringType()),
    StructField("name",StringType()),
    StructField("location",StringType()),
    StructField("country",StringType()),
    StructField("lat",FloatType()),
    StructField("lng",FloatType()),
    StructField("alt",IntegerType()),
    StructField("url",StringType())

])

# COMMAND ----------

df = create_csv_df(f"/mnt/formulaone/bronze/{source_nm}.csv",input_schema)
df.display()

# COMMAND ----------

# rename columns & adding date column
circuits_df = df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumn("ingest_dt",current_timestamp())
circuits_df.display()

# COMMAND ----------

circuits_df.write.mode("overwrite").parquet(f"/mnt/formulaone/silver/{source_nm}")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formulaone/silver"))

# COMMAND ----------

circuit_df = spark.read.parquet("/mnt/formulaone/silver/circuits/")
circuit_df.display()

# COMMAND ----------


