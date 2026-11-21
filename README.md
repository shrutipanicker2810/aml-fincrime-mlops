# Money Laundering Detection

Detecting money laundering transactions with Databricks.

## Dataset

[IBM Transactions for Anti Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) - Altman et al., *Realistic Synthetic Financial Transactions for Anti-Money Laundering Models*, NeurIPS 2023.

## Setup

1. Clone the repo into a Databricks Git folder.
2. Run `notebooks/00_setup/00_create_catalog_objects.py`.
3. Upload `HI-Small_Trans.csv` and `HI-Small_Patterns.txt` to `/Volumes/workspace/aml_raw/landing/ibm/`.
