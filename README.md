# Mini Data Platform (Medallion Architecture)

This project is a **beginner-friendly mini data platform** that demonstrates how data flows through a **medallion architecture** using Docker, Airflow, PostgreSQL, MinIO, and Metabase.

---

## Components

| Service        | Purpose                                                       | Port(s)          |
|----------------|---------------------------------------------------------------|------------------|
| **PostgreSQL** | Stores bronze, silver, and gold tables                        | `5432`           |
| **MinIO**      | S3-compatible object store for raw CSV files                  | `9000` / `9001`  |
| **Airflow**    | Orchestrates the full pipeline end-to-end                     | `8080`           |
| **Metabase**   | Simple BI tool to build dashboards on top of the gold layer   | `3000`           |

### Medallion schemas (all inside PostgreSQL)

- **`bronze`** — raw data as-is from the source.
- **`silver`** — cleaned data with PII masked (hashed).
- **`gold`** — aggregated data ready for dashboards.

---

## Folder structure

```
DEM12_Lab/
├── .github/workflows/
│   └── ci.yml                      # CI/CD + data-flow validation pipeline
├── airflow/
│   ├── dags/
│   │   ├── medallion_pipeline_dag.py   # Main Airflow DAG
│   │   ├── faker_to_minio.py          # Generates fake CSV and uploads to MinIO
│   │   └── data_generator.py          # Wrapper that calls faker_to_minio
│   ├── logs/                          # Airflow logs (Docker volume mount)
│   └── plugins/
├── src/
│   ├── __init__.py
│   ├── data_generator.py             # Re-export shim for scripts
│   ├── data_processor.py             # Medallion processing (bronze → silver → gold)
│   └── data_validation.py            # Row-count validation & PII masking checks
├── scripts/
│   └── load_to_postgres.py           # Manual helper to run processing outside Airflow
├── docker-compose.yml                # Runs all services together
├── Dockerfile                        # Airflow custom image
├── requirements.txt                  # Python dependencies
├── .gitignore                        # Ignores volumes, logs, caches, secrets
├── metabase.md                       # Metabase setup guide
├── read.md                           # Detailed requirements
└── task.md                           # Lab task description
```

---

## Architecture & Data Flow

```
┌──────────┐      ┌──────────┐      ┌──────────────────────────────────────┐      ┌──────────┐
│  Faker   │─CSV─▸│  MinIO   │─────▸│         Apache Airflow               │─────▸│ Metabase │
│ (data    │      │ (bronze  │      │                                      │      │ (dash-   │
│  gen)    │      │  bucket) │      │  bronze → silver (hash PII) → gold   │      │  boards) │
└──────────┘      └──────────┘      └──────────────────────────────────────┘      └──────────┘
                                              │                                        ▲
                                              ▼                                        │
                                    ┌──────────────────┐                               │
                                    │   PostgreSQL      │───────────────────────────────┘
                                    │  bronze | silver  │
                                    │       gold        │
                                    └──────────────────┘
```

---

## How the pipeline works

### 1. Data generation (bronze files)
- `faker_to_minio.py` uses **Faker** to create a CSV of fake sales data.
- PII fields are included: `full_name`, `email`, `phone`, `address`.
- A new CSV is created on **every DAG run** with a timestamped name and stored in the MinIO `bronze` bucket.

### 2. Data processing (bronze → silver → gold)
- `src/data_processor.py` contains simple, readable functions:
  - `init_medallion_schemas()` — creates `bronze`, `silver`, `gold` schemas and tables.
  - `load_bronze_placeholder(object_name)` — inserts a row into `bronze.sales_raw`.
  - `transform_bronze_to_silver()` — hashes PII using `md5` and calculates `total_amount = quantity × unit_price`.
  - `aggregate_silver_to_gold()` — builds daily product-level aggregates into `gold.sales_agg_daily`.

### 3. Validation
- `src/data_validation.py` checks that:
  - `silver` never has more rows than `bronze`.
  - `gold` is not empty when `silver` has data.
- If validation fails, the Airflow task raises an error (visible in logs).

### 4. Visualization (Metabase)
- Metabase connects to PostgreSQL and reads from `gold.sales_agg_daily`.
- Example dashboards: Total revenue by day, Top products by revenue, Quantity sold by day and product.
- See [metabase.md](metabase.md) for step-by-step setup.

---

## Airflow DAG structure

The DAG `medallion_pipeline` runs every **30 minutes** and orchestrates the full flow:

```
init_medallion_schemas
        │
        ▼
  data_generator          ← generates fake CSV → uploads to MinIO
        │
        ▼
  detect_new_files        ← if no data → emails alert & short-circuits
        │
        ▼
  data_processing_bronze  ← inserts data into bronze.sales_raw
        │
        ▼
  data_processing_silver  ← hashes PII, calculates totals
        │
        ▼
  data_processing_gold    ← daily product aggregations
        │
        ▼
  data_validation         ← row-count checks
```

**Features:**
- Python `logging` module (not `print`)
- Retry logic (`retries: 3`, `retry_delay: 5 min`)
- Email alerts on failure
- Email alert when no new data is detected
- Logs persisted via Docker volume mount (`./airflow/logs`)

---

## Airflow credentials

| Field    | Value         |
|----------|---------------|
| Username | `mdp-airflow` |
| Password | `mdp-pass`    |

These are created automatically by the `airflow-init` service on first startup.

---

## Email alerts

- Airflow sends email alerts to **`dosimeyolivia98@gmail.com`**.
- SMTP settings point to Gmail (`smtp.gmail.com:587`).
- Set your app password via environment variable:

```bash
export SMTP_PASSWORD=your_actual_app_password
docker compose up -d
```

> ⚠️ Do **not** commit a real password to GitHub. Use environment variables or GitHub Secrets.

---

## How to run locally

### 1. Start services

```bash
docker compose up -d
```

### 2. Access UIs

| Service    | URL                          | Credentials                               |
|------------|------------------------------|--------------------------------------------|
| Airflow    | http://localhost:8080        | `mdp-airflow` / `mdp-pass`                |
| MinIO      | http://localhost:9001        | `minioadmin` / `minioadmin123`             |
| Metabase   | http://localhost:3000        | (create on first visit)                    |
| PostgreSQL | `localhost:5432`             | `mdp_db` / `mdp_user` / `mdp_pass`        |

### 3. Trigger the DAG

1. Open Airflow UI → DAGs.
2. Unpause **`medallion_pipeline`**.
3. Click **Trigger DAG** and watch the logs.

### 4. Inspect data

Connect to PostgreSQL and query:

```sql
SELECT * FROM bronze.sales_raw   LIMIT 5;
SELECT * FROM silver.sales_clean LIMIT 5;
SELECT * FROM gold.sales_agg_daily;
```

### 5. Set up Metabase

See [metabase.md](metabase.md) for complete instructions.

---

## CI/CD Pipeline (GitHub Actions)

The pipeline in `.github/workflows/ci.yml` has three jobs:

| Job                       | Trigger          | What it does                                                                 |
|---------------------------|------------------|------------------------------------------------------------------------------|
| **build-and-test** (CI)   | Every commit     | Syntax-checks all Python files, validates Compose config, builds Docker image |
| **deploy-test** (CD)      | Push to `main`   | Starts all containers, waits for health checks, then tears down              |
| **data-flow-validation**  | Push to `main`   | Runs the full ingestion → processing → validation pipeline inside containers |

---

## Notes and next steps

- The bronze loader is intentionally simple (one row per file) to keep the code readable. Extend `load_bronze_placeholder` into a real CSV reader from MinIO when ready.
- For production, move credentials (DB, SMTP) into environment variables or secrets.
- Add more gold-layer aggregations for richer dashboards.
