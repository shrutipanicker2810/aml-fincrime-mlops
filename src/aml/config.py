from dataclasses import dataclass


@dataclass(frozen=True)
class ProjectConfig:
    catalog: str = "workspace"

    # Schemas for raw files, each medallion layer, and ML outputs
    raw_schema: str = "aml_raw"
    bronze_schema: str = "aml_bronze"
    silver_schema: str = "aml_silver"
    gold_schema: str = "aml_gold"
    ml_schema: str = "aml_ml"

    # Volume holding the uploaded CSV files
    landing_volume: str = "landing"

    # IBM dataset variant (HI-Small, HI-Medium, HI-Large, LI-Small, ...)
    ibm_variant: str = "HI-Small"

    @property
    def schemas(self) -> list[str]:
        return [
            self.raw_schema,
            self.bronze_schema,
            self.silver_schema,
            self.gold_schema,
            self.ml_schema,
        ]

    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.raw_schema}/{self.landing_volume}"

    @property
    def ibm_dir(self) -> str:
        return f"{self.landing_path}/ibm"

    @property
    def ibm_trans_path(self) -> str:
        return f"{self.ibm_dir}/{self.ibm_variant}_Trans.csv"

    @property
    def ibm_patterns_path(self) -> str:
        return f"{self.ibm_dir}/{self.ibm_variant}_Patterns.txt"


CONFIG = ProjectConfig()
