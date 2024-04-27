# Databricks notebook source
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

Final_result_df = spark.read.parquet("/mnt/bwtformula1project/gold/Results/")

# COMMAND ----------

dbutils.widgets.text("Race_Year","2024","Enter the Year to check Driver Standing")
race_year = dbutils.widgets.get("Race_Year")


# COMMAND ----------

Final_result_df = Final_result_df.drop_duplicates(["race_time"]).filter(col("race_yr")==race_year)

# COMMAND ----------

race_results_df = Final_result_df.withColumn("result_position",col("result_position").cast("integer"))

# COMMAND ----------

driver_standings_df = race_results_df \
.groupBy("race_yr", "driver", "nationality")\
.agg(sum("points").alias("total_points"),
     count(when(col("result_position") == 1, True)).alias("wins"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_yr").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

final_df.display()
