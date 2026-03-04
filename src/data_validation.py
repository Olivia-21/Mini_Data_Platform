import logging

import psycopg2

from src.data_processor import POSTGRES_CONN


logger = logging.getLogger(__name__)


def validate_row_counts() -> bool:
    """
    Very simple validation:
    - bronze row count should be >= silver row count
    - gold should have at least one row if silver has data
    """
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM bronze.sales_raw;")
    bronze_cnt = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM silver.sales_clean;")
    silver_cnt = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM gold.sales_agg_daily;")
    gold_cnt = cur.fetchone()[0]

    cur.close()
    conn.close()

    logger.info(
        "Validation counts - bronze: %s, silver: %s, gold: %s",
        bronze_cnt,
        silver_cnt,
        gold_cnt,
    )

    if silver_cnt > bronze_cnt:
        logger.error("Validation failed: silver has more rows than bronze.")
        return False

    if silver_cnt > 0 and gold_cnt == 0:
        logger.error("Validation failed: silver has data but gold is empty.")
        return False

    logger.info("Basic validation checks passed.")
    return True

