"""
Small helper script to demonstrate how you could call processing functions
outside of Airflow (for example from a notebook or a CLI).
"""

from src.data_processor import (
    init_medallion_schemas,
    load_bronze_placeholder,
    transform_bronze_to_silver,
    aggregate_silver_to_gold,
)


def run_all(object_name: str) -> None:
    init_medallion_schemas()
    load_bronze_placeholder(object_name=object_name)
    transform_bronze_to_silver()
    aggregate_silver_to_gold()


if __name__ == "__main__":
    # In a real script you would parse this from sys.argv
    example_object_name = "manual_sales_example.csv"
    run_all(example_object_name)

