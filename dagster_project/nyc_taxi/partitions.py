"""Monthly partition definition shared across assets."""

from __future__ import annotations

from dagster import MonthlyPartitionsDefinition

# Match the start of the public NYC TLC parquet feed.
monthly_partitions = MonthlyPartitionsDefinition(start_date="2023-01-01")


def partition_year_month(partition_key: str) -> tuple[int, int]:
    year_str, month_str, _ = partition_key.split("-")
    return int(year_str), int(month_str)
