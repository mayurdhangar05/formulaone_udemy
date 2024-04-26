# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DateType

from pyspark.sql.functions import lit,col,concat,regexp_replace,current_timestamp,to_timestamp

from datetime import datetime

current_date = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

def create_csv_df(input_location,schema,header_status=True):
    """
    This function is used for creating spark dataframe on csv file location
    :input_location = provide input csv file location
    :schema = provide input schema
    :return spark dataframe
    """
    return spark.read.csv(input_location,header=header_status,schema = schema)


# COMMAND ----------

def create_json_df(input_location,schema):
    """
    This function is used for creating spark dataframe on json file location
    :input_location = provide input json file location
    :schema = provide input schema
    :return spark dataframe
    """
    return spark.read.json(input_location,schema=schema)

# COMMAND ----------


