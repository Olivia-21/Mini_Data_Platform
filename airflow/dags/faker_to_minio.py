import csv
import io
import logging
from datetime import datetime

from faker import Faker
from minio import Minio
from minio.error import S3Error


logger = logging.getLogger(__name__)


def generate_fake_sales_csv(num_rows: int = 100) -> bytes:
    """
    Generate a CSV in-memory with fake sales data including PII.

    PII columns: full_name, email, phone, address.
    """
    fake = Faker()
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(
        [
            "order_id",
            "order_timestamp",
            "full_name",
            "email",
            "phone",
            "address",
            "product",
            "quantity",
            "unit_price",
        ]
    )

    for i in range(num_rows):
        writer.writerow(
            [
                fake.uuid4(),
                datetime.utcnow().isoformat(),
                fake.name(),
                fake.email(),
                fake.phone_number(),
                fake.address().replace("\n", ", "),
                fake.word(),
                fake.random_int(min=1, max=10),
                round(fake.pyfloat(left_digits=2, right_digits=2, positive=True), 2),
            ]
        )

    return output.getvalue().encode("utf-8")


def upload_csv_to_minio(
    csv_bytes: bytes,
    bucket_name: str = "bronze",
    endpoint: str = "mdp_minio:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin123",
) -> str:
    """
    Upload the CSV bytes to MinIO in the given bucket.

    Returns the object name that was created.
    """
    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    if not client.bucket_exists(bucket_name):
        logger.info("Bucket %s does not exist. Creating it.", bucket_name)
        client.make_bucket(bucket_name)

    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    object_name = f"sales_{timestamp}.csv"

    try:
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="text/csv",
        )
        logger.info("Uploaded %s to bucket %s", object_name, bucket_name)
    except S3Error as e:
        logger.exception("Failed to upload CSV to MinIO: %s", e)
        raise

    return object_name


def generate_and_upload(num_rows: int = 100) -> str:
    """
    Helper function for Airflow tasks: generate a new CSV and upload to MinIO.
    Always creates a fresh dataset each run.
    """
    logger.info("Starting fake data generation for %s rows", num_rows)
    csv_bytes = generate_fake_sales_csv(num_rows=num_rows)
    object_name = upload_csv_to_minio(csv_bytes)
    logger.info("Finished generation and upload: %s", object_name)
    return object_name

