# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("source_nm","races")
source_nm = dbutils.widgets.get("source_nm")

# COMMAND ----------

input_schema = StructType([
    StructField("raceId",IntegerType()),
    StructField("year",IntegerType()),
    StructField("round",IntegerType()),
    StructField("circuitId",IntegerType()),
    StructField("name",StringType()),
    StructField("date",DateType()),
    StructField("time",StringType()),
    StructField("url",StringType()),
    StructField("fp1_date",DateType()),
    StructField("fp1_time",StringType()),
    StructField("fp2_date",DateType()),
    StructField("fp2_time",StringType()),
    StructField("fp3_date",DateType()),
    StructField("fp3_time",StringType()),
    StructField("quali_date",DateType()),
    StructField("quali_time",StringType()),
    StructField("sprint_date",DateType()),
    StructField("sprint_time",StringType()),
])

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formulaone/bronze/races.csv"))

# COMMAND ----------

df = create_csv_df(f"dbfs:/mnt/formulaone/bronze/{source_nm}.csv",input_schema)
df.display()


# COMMAND ----------

races_df = df.withColumnRenamed("raceId","race_id").withColumnRenamed("circuitId","circuit_id").withColumn("ingest_dt",current_timestamp())\
    .withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))
races_df.display()

# COMMAND ----------

races_df = races_df.select(col('race_id'),col('year').alias('race_year'),col('round'),col('circuit_id'),col('name'),col('ingest_dt'),col('race_timestamp'))
races_df.display()

# COMMAND ----------

races_df.write.mode("overwrite").partitionBy('race_year').parquet(f"/mnt/formulaone/silver/{source_nm}")

# COMMAND ----------


