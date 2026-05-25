"""Shared ETL primitives used by both the Airflow and Dagster orchestrators.

The same Python functions back both pipelines so that business logic, schema
definitions, and quality checks stay in lockstep no matter which scheduler runs
the workload.
"""

from shared.config import Settings, get_settings

__all__ = ["Settings", "get_settings"]
