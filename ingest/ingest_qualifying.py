# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("source_nm","qualifying")
source_nm = dbutils.widgets.get("source_nm")

# COMMAND ----------

input_schema = StructType(
    fields=[
        StructField("qualifyID", IntegerType()),
        StructField("raceID", IntegerType()),
        StructField("driverID", IntegerType()),
        StructField("constructorID", IntegerType()),
        StructField("number", IntegerType()),
        StructField("position", IntegerType()),
        StructField("q1", StringType()),
        StructField("q2", StringType()),
        StructField("q3", StringType()),
    ]
)

# COMMAND ----------

qualifying_df = create_multiLine_json_df(f"dbfs:/mnt/formulaone/bronze/{source_nm}",input_schema)
qualifying_df.display()

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed("qualifyID","qualify_id").withColumnRenamed("raceID","race_id").withColumnRenamed("driverID","driver_id").withColumnRenamed("constructorID","constructor_id").withColumn("ingest_dt",current_timestamp())

qualifying_df.display()

# COMMAND ----------

qualifying_df.write.mode("overwrite").parquet(f'dbfs:/mnt/formulaone/silver/{source_nm}')
