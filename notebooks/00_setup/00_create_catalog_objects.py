# Databricks notebook source
# MAGIC %md
# MAGIC # Create catalog objects

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

# Create schemas
for schema in CONFIG.schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONFIG.catalog}.{schema}")
    print(f"{CONFIG.catalog}.{schema}")

# COMMAND ----------

# Create the landing volume and the folder for the IBM files
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"{CONFIG.catalog}.{CONFIG.raw_schema}.{CONFIG.landing_volume}"
)
dbutils.fs.mkdirs(CONFIG.ibm_dir)
print(CONFIG.ibm_dir)

# COMMAND ----------

# Check that the raw files have been uploaded
expected = {
    "IBM transactions": CONFIG.ibm_trans_path,
    "IBM patterns": CONFIG.ibm_patterns_path,
}

for label, path in expected.items():
    try:
        size_mb = dbutils.fs.ls(path)[0].size / 1e6
        print(f"{label}: found ({size_mb:.1f} MB)")
    except Exception:
        print(f"{label}: missing - {path}")
