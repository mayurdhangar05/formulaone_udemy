# Databricks notebook source
# MAGIC %run ../Utils/Common_functions

# COMMAND ----------

dbutils.widgets.text("Race_name","Abu Dhabi Grand Prix","Enter the Race name to get Race Results")
dbutils.widgets.text("Race_year","2021","Enter the race year to get Race Results")
race_name = dbutils.widgets.get("Race_name")
race_year = dbutils.widgets.get("Race_year")

# COMMAND ----------

df_result = spark.read.parquet("/mnt/formulaoneraceadlsgen2/silver/Results")
df_pitstops = spark.read.parquet("/mnt/formulaoneraceadlsgen2/silver/pit_stops")
df_drivers = spark.read.parquet("/mnt/formulaoneraceadlsgen2/silver/Drivers")
df_constructors = spark.read.parquet("/mnt/formulaoneraceadlsgen2/silver/Constructors")

# COMMAND ----------

df_result = df_result.withColumn('points',col("points").cast("int"))

# COMMAND ----------

df_result = df_result.withColumnRenamed("race_time",'race_time').withColumnRenamed('season',"race_yr").withColumnRenamed("constructor_id","constructor_id1").withColumnRenamed("season_id","season_id1").distinct()

# COMMAND ----------

df_drivers = df_drivers.withColumnRenamed("name","driver_name").withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

df_result1 = df_result.join(df_pitstops,'race_date',"left").join(df_drivers,'driver_id','left').join(df_constructors,df_result.constructor_id1 == df_constructors.constructor_ref,'left')

# COMMAND ----------

# df_result1.display()

# COMMAND ----------

df_result1 = df_result1.select(col("driver_nationality").alias("nationality"),col("driver_name").alias("driver"),col("permanentNumber").alias("number"),col("constructor_ref").alias("Team"),'grid','stops',"fastest_lap_detail",col("race_time"),"points",'race_name',col("season_id1").alias('race_yr'),"result_position").distinct()

# COMMAND ----------

# df_result1.write.parquet("/mnt/formulaoneraceadlsgen2/gold/Results",mode="overwrite")

# COMMAND ----------

df_for_perticular_year_and_race_location = df_result1.filter((col("race_name")== race_name)&(col("race_yr")==race_year)).orderBy(col("points").desc())

# COMMAND ----------

df_for_perticular_year_and_race_location = df_for_perticular_year_and_race_location.dropDuplicates(["driver"])

# COMMAND ----------

df_for_perticular_year_and_race_location = df_for_perticular_year_and_race_location.withColumn("result_position",col("result_position").cast("integer"))

# COMMAND ----------

df_for_perticular_year_and_race_location = df_for_perticular_year_and_race_location.orderBy("result_position").select("nationality","driver","number","Team","grid","stops","fastest_lap_detail","race_time","points")

# COMMAND ----------

df_for_perticular_year_and_race_location.display()

# COMMAND ----------

result = df_for_perticular_year_and_race_location.collect()
result_list = []
for row in result:
    dict_row = row.asDict()
    result_list.append(dict_row)


# COMMAND ----------

dbutils.notebook.exit(result_list)

# COMMAND ----------

# df_for_perticular_year_and_race_location.write.parquet(f"/mnt/formulaoneraceadlsgen2/gold/Results/{race_name}_{race_year}_Results",mode="overwrite")
