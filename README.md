# 🏎️ F1 Data Transformation Pipeline

This project automates the ingestion of Formula 1 data from the OpenF1 API, stores it in a **PostgreSQL** raw area, and uses **dbt + DuckDB** for analytical transformations.

## 🏗️ Architecture

1. **Ingestion:** Airflow DAG fetches API data into `Postgres (schema: raw)`.
2. **Interface:** Airflow exports Postgres tables to `.parquet` files.
3. **Transformation:** dbt reads Parquet files and builds models in `DuckDB`.

## 🚀 Getting Started

### Prerequisites

- [uv](https://github.com/astral-sh/uv) (Python package manager)
- Docker & Docker Compose

### Local Setup

1. **Environment:** Create a `.env` file based on `.env.example`.
2. **Create Virtual Environment**
    ```bash
        uv sync
        # Activate the environment
        source .venv/bin/activate
    ```
3. **Infrastructure:** Run the setup script to create Postgres schemas:

    ```bash
    uv run python scripts/setup_database.py
    ```

4. **Containers:** Start Airflow

    ```bash
     docker compose up -d --build
    ```

5. **Profiles:** Create a `profiles.yml` file based on `profiles.yml.example` at conf folder

### Running the Pipeline

- Orchestration: Access Airflow at `localhost:8080` and trigger the `f1_ingest_drivers_staging` DAG.
- Transformation: Once the Parquet file is generated in `data/`, run dbt

    ```bash
     uv run dbt run --profiles-dir conf
    ```

### Add New Package Workflow

Whenever you add a new package locally, follow this ritual:

```bash
    # 1. Add the package to your local environment
    uv add <package_name>

    # 2. Export the lockfile to a standard requirements.txt for Docker
    uv export --format requirements-txt > requirements.txt

    # 3. Rebuild the Docker image to bake in the new package
    docker compose up -d --build
```
