# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text('source_nm','drivers')
source_nm = dbutils.widgets.get('source_nm')

# COMMAND ----------

input_schema = StructType(
    [
        StructField("driverId", IntegerType()),
        StructField("driverRef", StringType()),
        StructField("number", IntegerType()),
        StructField("code", StringType()),
        StructField(
            "name",
            StructType(
                [
                    StructField("forename", StringType()),
                    StructField("surname", StringType()),
                ]
            ),
        ),
        StructField("dob", DateType()),
        StructField("nationality", StringType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

df = create_json_df(f"dbfs:/mnt/formulaone/bronze/{source_nm}.json",input_schema)
df.display()

# COMMAND ----------

driver_df = df.withColumnRenamed('driverId','driver_id')\
    .withColumnRenamed('driverRef','driver_ref')\
        .withColumn('ingest_dt',current_timestamp())\
            .withColumn('name',concat(col('name.forename'),lit(' '),col('name.surname')))

driver_df.display()

# COMMAND ----------

driver_df.write.mode("overwrite").parquet(f"/mnt/formulaone/silver/{source_nm}")
