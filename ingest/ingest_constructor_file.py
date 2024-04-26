# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("source_nm","constructors")
source_nm = dbutils.widgets.get("source_nm")

# COMMAND ----------

# defining schema
input_schema = StructType([
    StructField("constructorId",IntegerType()),
    StructField("constructorRef",StringType()),
    StructField("name",StringType()),
    StructField("nationality",StringType()),
    StructField("url",StringType())

])

# COMMAND ----------

# input location of the file
display(dbutils.fs.ls('/mnt/formulaone/bronze/constructors.json/'))

# COMMAND ----------

df = create_json_df(f"dbfs:/mnt/formulaone/bronze/{source_nm}.json",input_schema)
df.display()

# COMMAND ----------

constructor_df = df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('constructorRef','constructor_ref')\
    .drop(col('url')).withColumn('ingest_dt',current_timestamp())

constructor_df.display()

# COMMAND ----------

constructor_df.write.mode("overwrite").parquet(f'dbfs:/mnt/formulaone/silver/{source_nm}')

# COMMAND ----------


