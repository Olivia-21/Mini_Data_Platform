"""
Data generation helper.

Provides ``run_data_generation`` which creates a fake-data CSV and uploads
it to MinIO.  All heavy lifting is done by ``faker_to_minio`` (located in
``airflow/dags/``).

This module can be imported as ``src.data_generator`` from scripts that run
outside the Airflow dags folder.
"""

import logging
import sys
import os

logger = logging.getLogger(__name__)

# Ensure the dags folder is importable so we can reach faker_to_minio
_dags_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)

from faker_to_minio import generate_and_upload  # noqa: E402


def run_data_generation(num_rows: int = 100) -> str:
    """
    Generate fake sales data and upload to MinIO.

    Returns the MinIO object name that was created.
    """
    logger.info("Running data generation for %s rows", num_rows)
    object_name = generate_and_upload(num_rows=num_rows)
    logger.info("Data generation complete. Object name: %s", object_name)
    return object_name
