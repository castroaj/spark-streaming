# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A Delta Lake Lakehouse demo running entirely in Docker, using:
- **MinIO** — S3-compatible object storage (bucket: `warehouse`)
- **Hive Metastore** — table metadata (backed by PostgreSQL)
- **Apache Spark 3.5** with `delta-spark==3.1.0`
- **JupyterLab** — notebook interface

## Running the Stack

```bash
# Start all services (builds the Spark/Jupyter image on first run)
docker compose up --build

# JupyterLab: http://localhost:8888  (token in docker logs)
# MinIO Console: http://localhost:9001  (minioadmin / minioadmin)
# Spark UI: http://localhost:4040  (only active during a running job)
```

Notebooks in `./src` are mounted into `/home/jovyan/work` inside the Spark container.

## Architecture

### Service Topology

```
JupyterLab/Spark ──► MinIO (s3a://warehouse/...)        [object storage]
                 ──► Hive Metastore (thrift://hive-metastore:9083)
                           └── PostgreSQL (metastore_db)
```

All services communicate by container name over the `lakehouse-net` Docker bridge network.

### SparkSession Configuration

Both notebooks configure Spark with:
- S3A endpoint: `http://minio:9000`, credentials `minioadmin/minioadmin`
- Delta Lake extensions + Hive catalog pointing at `thrift://hive-metastore:9083`
- `delta-spark` pulled in via `configure_spark_with_delta_pip`

### Notebooks

**`Delta-Lake-Lakehouse.ipynb`** — Core Delta Lake features: table creation, upsert via `DeltaTable` merge, schema evolution, time travel (`versionAsOf`), and liquid clustering (`clusterBy`). Tables land at `s3a://warehouse/<table_name>` and are registered in Hive.

**`Delta-Lake-Streaming-Simulation.ipynb`** — Spark Structured Streaming writing Delta tables to MinIO. Uses the `rate` source for synthetic data, writes ~30 MB Parquet files on a 30-second trigger interval, and supports mid-stream queries. Call `query.stop()` to terminate the background streaming job.
