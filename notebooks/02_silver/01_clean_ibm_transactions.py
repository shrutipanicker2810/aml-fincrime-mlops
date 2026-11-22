# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: IBM transactions

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

from pyspark.sql import functions as F, Window

source_table = CONFIG.table(CONFIG.bronze_schema, "ibm_transactions")
target_table = CONFIG.table(CONFIG.silver_schema, "ibm_transactions")

raw_cols = [
    "timestamp", "from_bank", "from_account", "to_bank", "to_account",
    "amount_received", "receiving_currency", "amount_paid",
    "payment_currency", "payment_format", "is_laundering",
]

bronze = spark.table(source_table)

# COMMAND ----------

# Stable transaction id: hash of the raw values plus a counter for exact duplicates
dup_window = Window.partitionBy(*raw_cols).orderBy(F.lit(1))

with_id = (
    bronze
    .withColumn("dup_rank", F.row_number().over(dup_window))
    .withColumn("transaction_id", F.xxhash64(*raw_cols, "dup_rank"))
)

# COMMAND ----------

# Cast types; try_* returns null for bad values so they can be counted below
silver = with_id.select(
    "transaction_id",
    F.expr("try_to_timestamp(timestamp, 'yyyy/MM/dd HH:mm')").alias("timestamp"),
    "from_bank",
    "from_account",
    F.concat_ws("_", "from_bank", "from_account").alias("from_account_id"),
    "to_bank",
    "to_account",
    F.concat_ws("_", "to_bank", "to_account").alias("to_account_id"),
    F.expr("try_cast(amount_paid AS DOUBLE)").alias("amount_paid"),
    "payment_currency",
    F.expr("try_cast(amount_received AS DOUBLE)").alias("amount_received"),
    "receiving_currency",
    "payment_format",
    (F.col("payment_currency") != F.col("receiving_currency")).alias("is_cross_currency"),
    (F.col("from_bank").eqNullSafe(F.col("to_bank"))
     & F.col("from_account").eqNullSafe(F.col("to_account"))).alias("is_self_transfer"),
    F.expr("try_cast(is_laundering AS INT)").alias("is_laundering"),
    "dup_rank",
)

# COMMAND ----------

# Data quality checks
checks = silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("transaction_id").alias("distinct_ids"),
    F.sum(F.col("timestamp").isNull().cast("int")).alias("bad_timestamp"),
    F.sum((F.col("amount_paid").isNull() | F.col("amount_received").isNull()).cast("int")).alias("bad_amount"),
    F.sum(((F.col("amount_paid") < 0) | (F.col("amount_received") < 0)).cast("int")).alias("negative_amount"),
    F.sum((~F.col("is_laundering").isin(0, 1) | F.col("is_laundering").isNull()).cast("int")).alias("bad_label"),
    F.sum((F.col("from_account").isNull() | F.col("to_account").isNull()).cast("int")).alias("missing_account"),
    F.sum((F.col("dup_rank") > 1).cast("int")).alias("exact_duplicates"),
).first().asDict()

for name, value in checks.items():
    print(f"{name}: {value:,}")

failures = [
    name for name in ("bad_timestamp", "bad_amount", "negative_amount", "bad_label", "missing_account")
    if checks[name] > 0
]
if checks["distinct_ids"] != checks["rows"]:
    failures.append("duplicate_transaction_id")

if failures:
    raise ValueError(f"Data quality checks failed: {failures}")

# COMMAND ----------

# Exact duplicates are kept; dup_rank is only needed for the id
(
    silver.drop("dup_rank")
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

# COMMAND ----------

result = spark.table(target_table)
print(f"{target_table}: {result.count():,} rows")
display(result.limit(5))
