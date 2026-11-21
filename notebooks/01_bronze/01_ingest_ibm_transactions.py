# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: IBM transactions

# COMMAND ----------

import os, sys

# Add the repo's src folder to the path so the aml package can be imported
def _repo_root(start: str) -> str:
    path = os.path.abspath(start)
    while path != os.path.dirname(path):
        if os.path.exists(os.path.join(path, "src", "aml", "config.py")):
            return path
        path = os.path.dirname(path)
    raise RuntimeError("Repo root not found")

sys.path.insert(0, os.path.join(_repo_root(os.getcwd()), "src"))

from aml.config import CONFIG

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Raw schema in file column order; the CSV has two columns named "Account",
# so names are set here instead of taken from the header.
# All columns are kept as strings and cast in the silver layer.
raw_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("from_bank", StringType()),
    StructField("from_account", StringType()),
    StructField("to_bank", StringType()),
    StructField("to_account", StringType()),
    StructField("amount_received", StringType()),
    StructField("receiving_currency", StringType()),
    StructField("amount_paid", StringType()),
    StructField("payment_currency", StringType()),
    StructField("payment_format", StringType()),
    StructField("is_laundering", StringType()),
])

target_table = CONFIG.table(CONFIG.bronze_schema, "ibm_transactions")

# COMMAND ----------

# Read the CSV and add source file and load time to each row
df = (
    spark.read
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(raw_schema)
    .csv(CONFIG.ibm_trans_path)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Overwrite so re-runs replace the table instead of duplicating rows
(
    df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

# COMMAND ----------

# Basic checks
bronze = spark.table(target_table)
print(f"{target_table}: {bronze.count():,} rows")

display(bronze.groupBy("is_laundering").count())
display(bronze.limit(5))
