# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    lit,
    concat,
    regexp_replace,
    explode,
    row_number,
    dense_rank,
    count,
    when
)
from datetime import datetime
from pyspark.sql.functions import (
    current_timestamp,
    to_timestamp,
    max,
    dense_rank,
    desc,
    regexp_replace,
    sum,
    min,
    row_number,
    rank,
    lit,
    trim,
    avg
)
from pyspark.sql.window import Window

# COMMAND ----------

# Define current date value "YYYY-mm-dd"
current_dt = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

def create_csv_dataframe(input_location, schema):
    """
    This function is used for creating a spark dataframe on csv file location
    :input_location: Provide input_file location of csv file
    :schema        : Provide input schema
    :return        : spark dataframe
    """
    return spark.read.csv(path=input_location, schema=schema)

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format(
            "parquet"
        ).saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name).distinct().collect()

    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

def flatten(df):
    # Flatten struct types
    struct_fields = [
        field
        for field in df.schema.fields
        if str(field.dataType).startswith("StructType")
    ]
    for field in struct_fields:
        nested_fields = [
            col(f"{field.name}.{subfield.name}").alias(f"{field.name}_{subfield.name}")
            for subfield in field.dataType.fields
        ]
        df = df.select("*", *nested_fields).drop(field.name)

    # Flatten array types
    array_fields = [
        field
        for field in df.schema.fields
        if str(field.dataType).startswith("ArrayType")
    ]
    for field in array_fields:
        df = df.withColumn(field.name, explode(field.name))

    # Recursively flatten nested structs and arrays
    if struct_fields or array_fields:
        df = flatten(df)

    return df
