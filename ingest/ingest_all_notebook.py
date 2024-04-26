# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run("circuit_ingest",0,{"source_nm":"circuits"})
