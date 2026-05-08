# spark-streaming

A hands-on demonstration of a Spark-based streaming lakehouse built entirely with open-source tooling and Docker. The repository is organized into two complementary parts: an interactive notebook environment for exploring Delta Lake features, and a reusable Java library that abstracts the connector surface those notebooks rely on.

---

## Goal

The project demonstrates how to build a production-shaped streaming lakehouse using:

- **Apache Spark 3.5** — distributed compute and structured streaming
- **Delta Lake 3.x** — ACID transactions, schema evolution, and time travel on object storage
- **Apache Hive Metastore** — table catalog backed by PostgreSQL
- **MinIO** — S3-compatible object storage (local stand-in for AWS S3)
- **Apache Kafka** — event streaming source for ingestion pipelines

The notebooks let you explore the concepts interactively. The `streaming-lib` Java library packages those concepts into typed, builder-based APIs that a real application team could depend on.

---

## Repository Layout

```
spark-streaming/
├── ipyworkbook/        # Docker Compose stack + JupyterLab notebooks
│   ├── docker-compose.yml
│   └── src/
│       ├── Delta-Lake-Lakehouse.ipynb           # Core Delta Lake features
│       └── Delta-Lake-Streaming-Simulation.ipynb # Spark Structured Streaming demo
│
└── streaming-lib/      # Java 17 library — connector abstractions for production use
    ├── SPEC.md         # Full API specification
    ├── STANDARDS.md    # Engineering standards
    └── src/
        └── main/java/com/github/castroaj/streaminglib/
            ├── kafka/   # KafkaSource — consume from Kafka topics
            ├── hive/    # HiveMetastoreClient — catalog management
            ├── s3/      # S3StoreClient — object storage operations
            └── delta/   # DeltaStreamWriter — structured streaming writes
```

---

## Part 1: Interactive Notebooks (`ipyworkbook/`)

A self-contained Docker Compose stack that brings up the full lakehouse in one command. The stack includes JupyterLab with Spark, MinIO for S3-compatible object storage, a Hive Metastore backed by PostgreSQL, and exposes Spark UI and MinIO console for observability.

**`Delta-Lake-Lakehouse.ipynb`** covers core Delta Lake features: table creation, upsert via `DeltaTable.merge`, schema evolution, time travel (`versionAsOf`), and liquid clustering (`clusterBy`). Tables are written to MinIO and registered in Hive.

**`Delta-Lake-Streaming-Simulation.ipynb`** demonstrates Spark Structured Streaming writing Delta tables to MinIO using a synthetic `rate` source.

---

## Part 2: Java Connector Library (`streaming-lib/`)

A Java 17 library that wraps the same four connector surfaces the notebooks use into a typed, builder-based API suitable for production streaming jobs.

### Design principles

- **Building blocks, not a framework.** Each connector is independently usable — no mandatory initialization or global state.
- **Caller-provided `SparkSession`.** The library never calls `SparkSession.builder()` internally.
- **Fail fast at construction time.** Config objects are validated via Jakarta Bean Validation before any Spark context is touched.
- **No nulls in the public API.** Absent values use `Optional<T>`.
- **Unchecked exceptions only.** All connector checked exceptions are wrapped as `StreamingLibException` subtypes.

### Connectors

| Package | Interface | Purpose |
|---|---|---|
| `kafka` | `KafkaSource` | Consume records from a Kafka topic (streaming or bounded batch) |
| `hive` | `HiveMetastoreClient` | Database and table catalog operations via Thrift |
| `s3` | `S3StoreClient` | Path-based object storage operations against S3/MinIO |
| `delta` | `DeltaStreamWriter` | Manage `StreamingQuery` lifecycle for Delta format writes |

Each package exposes exactly three public types: an interface, an immutable `*Config` built via an inner builder, and a `*Factory` entry point. See `streaming-lib/SPEC.md` for the full API specification and `streaming-lib/STANDARDS.md` for engineering standards.

---

## Technology Versions

| Component | Version |
|---|---|
| Java | 17 |
| Gradle | 9.5.0 |
| Apache Spark | 3.5.8 (Scala 2.12) |
| Delta Lake | 3.2.1 |
| Apache Kafka | (via `spark-sql-kafka-0-10_2.12:3.5.8`) |
| Hadoop S3A | 3.3.4 |
| Hive Metastore | 4.0.0 |
