import logging

import psycopg2


logger = logging.getLogger(__name__)


POSTGRES_CONN = {
    "host": "mdp_postgres",
    "port": 5432,
    "dbname": "mdp_db",
    "user": "mdp_user",
    "password": "mdp_pass",
}


def init_medallion_schemas() -> None:
    """Create bronze/silver/gold schemas and base tables if they don't exist."""
    logger.info("Initializing medallion schemas in PostgreSQL")
    conn = psycopg2.connect(**POSTGRES_CONN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.sales_raw (
            order_id TEXT,
            order_timestamp TIMESTAMPTZ,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            product TEXT,
            quantity INT,
            unit_price NUMERIC
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.sales_clean (
            order_id TEXT,
            order_timestamp TIMESTAMPTZ,
            full_name_hash TEXT,
            email_hash TEXT,
            phone_hash TEXT,
            address_hash TEXT,
            product TEXT,
            quantity INT,
            unit_price NUMERIC,
            total_amount NUMERIC
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.sales_agg_daily (
            sales_date DATE,
            product TEXT,
            total_quantity INT,
            total_revenue NUMERIC
        );
        """
    )

    cur.close()
    conn.close()
    logger.info("Medallion schemas/tables ensured.")


def load_bronze_placeholder(object_name: str) -> None:
    """
    Beginner‑friendly bronze loader.

    In a production setup this would read the real CSV from MinIO and insert
    every row. For now we store one simple row per object so you can focus
    on the medallion pattern and Airflow orchestration.
    """
    logger.info("Loading bronze placeholder row for %s", object_name)
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bronze.sales_raw (
            order_id, order_timestamp, full_name, email, phone, address,
            product, quantity, unit_price
        ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            object_name,
            "Dummy User",
            "dummy@example.com",
            "000-000-0000",
            "Unknown",
            "sample_product",
            1,
            10.0,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Bronze load complete for %s", object_name)


def transform_bronze_to_silver() -> None:
    """Mask PII by hashing and compute total_amount."""
    logger.info("Transforming bronze -> silver with hashing and totals")
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    cur.execute("DELETE FROM silver.sales_clean;")

    cur.execute(
        """
        INSERT INTO silver.sales_clean (
            order_id,
            order_timestamp,
            full_name_hash,
            email_hash,
            phone_hash,
            address_hash,
            product,
            quantity,
            unit_price,
            total_amount
        )
        SELECT
            order_id,
            order_timestamp,
            md5(coalesce(full_name, '')) as full_name_hash,
            md5(coalesce(email, '')) as email_hash,
            md5(coalesce(phone, '')) as phone_hash,
            md5(coalesce(address, '')) as address_hash,
            product,
            quantity,
            unit_price,
            quantity * unit_price as total_amount
        FROM bronze.sales_raw;
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logger.info("bronze -> silver transformation complete")


def aggregate_silver_to_gold() -> None:
    """Aggregate daily metrics for dashboards."""
    logger.info("Aggregating silver -> gold")
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    cur.execute("DELETE FROM gold.sales_agg_daily;")

    cur.execute(
        """
        INSERT INTO gold.sales_agg_daily (sales_date, product, total_quantity, total_revenue)
        SELECT
            date(order_timestamp) as sales_date,
            product,
            SUM(quantity) as total_quantity,
            SUM(total_amount) as total_revenue
        FROM silver.sales_clean
        GROUP BY date(order_timestamp), product;
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logger.info("silver -> gold aggregation complete")

