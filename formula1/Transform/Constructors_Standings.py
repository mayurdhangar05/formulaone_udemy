# Databricks notebook source
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

dbutils.widgets.text("Race_Year","2021","Enter the Race Year to get Constructor Standing")
race_year = dbutils.widgets.get("Race_Year")

# COMMAND ----------

Final_result_df = spark.read.parquet("/mnt/formulaoneraceadlsgen2/gold/Results/")

# COMMAND ----------

race_results_df = Final_result_df.filter(col("race_yr")==race_year)

# COMMAND ----------

race_results_df = race_results_df.drop_duplicates(["race_time"])

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_yr", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("result_position") == 1, True)).alias("wins"))

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_yr").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

final_df.display()
