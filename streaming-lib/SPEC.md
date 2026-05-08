# streaming-lib Specification

| Property | Value |
|---|---|
| Version | 0.1.0-SNAPSHOT |
| Java | 17 |
| Gradle | 9.5.0 |
| Spark | 3.5.8 (Scala 2.12) |
| Delta Lake | 3.2.1 |

---

## Project Overview

`streaming-lib` is a Java library that provides uniform, developer-friendly abstractions over four connector surfaces required to operate a Spark-based streaming lakehouse:

- **Kafka** — message consumption by topic
- **Hive Metastore** — catalog management via Thrift
- **S3 / MinIO** — object storage via S3A
- **Delta Lake** — structured streaming writes

Rather than requiring application teams to understand the detailed configuration surface of each connector, `streaming-lib` exposes purpose-built builder APIs and typed configuration objects that encapsulate correct defaults and enforce required settings at construction time, failing fast before any Spark context is touched.

The library provides **building blocks, not a framework**. Each abstraction is independently usable — a job that only needs Kafka and Delta does not need to instantiate a Hive client. The `SparkSession` is **always provided by the caller** and injected at factory time. The library never calls `SparkSession.builder()` internally.

---

## Architecture Overview

The library is a single Gradle project with one production source set and one integration test source set. The package layout mirrors a logical module decomposition:

```
com.github.castroaj.streaminglib
├── kafka/          # KafkaSource abstraction
├── hive/           # HiveMetastoreClient abstraction
├── s3/             # S3StoreClient abstraction
├── delta/          # DeltaStreamWriter abstraction
├── config/         # Shared config value-object base types
├── exception/      # Library-specific exception hierarchy
└── util/           # Internal utilities (not part of public API)
```

Each top-level package exposes exactly three public types:
- A primary **interface** (the public API contract)
- An immutable **`*Config`** value object built via an inner `Builder`
- A **`*Factory`** entry-point class (the only public instantiation point)

Implementation classes (e.g., `DefaultKafkaSource`) are package-private.

---

## Module: Kafka

### Purpose

Consume records from a Kafka topic via Spark Structured Streaming's native Kafka source, through a simplified interface that hides subscription options, offset management, and deserialization boilerplate.

### Interface

```java
package com.github.castroaj.streaminglib.kafka;

public interface KafkaSource {

    /**
     * Returns a streaming Dataset subscribed to the configured topic.
     * Schema: key (binary), value (binary), topic (string),
     *         partition (int), offset (long), timestamp (timestamp).
     */
    Dataset<Row> readStream();

    /**
     * Returns a batch Dataset reading from the given offset range.
     * Useful for backfill and replay.
     */
    Dataset<Row> readBatch(KafkaOffsetRange range);

    void close();
}

// Value object for bounded reads
public record KafkaOffsetRange(
        @NotBlank String topic,
        @NotNull @NotEmpty Map<Integer, Long> fromOffsets,
        @NotNull @NotEmpty Map<Integer, Long> untilOffsets) {

    public static KafkaOffsetRange of(String topic,
                                      Map<Integer, Long> fromOffsets,
                                      Map<Integer, Long> untilOffsets) { ... }
}
```

### Configuration

```java
package com.github.castroaj.streaminglib.kafka;

public final class KafkaSourceConfig {
    // Required
    private final String bootstrapServers;      // e.g. "kafka:9092"
    private final String topic;
    private final String groupId;

    // Optional (defaults shown)
    private final StartingOffsets startingOffsets; // EARLIEST
    private final int maxOffsetsPerTrigger;        // 100_000
    private final Duration pollTimeout;            // Duration.ofSeconds(120)
    private final Map<String, String> extraOptions; // empty

    public static Builder builder() { ... }

    public static final class Builder {
        public Builder bootstrapServers(String servers) { ... }
        public Builder topic(String topic) { ... }
        public Builder groupId(String groupId) { ... }
        public Builder startingOffsets(StartingOffsets offsets) { ... }
        public Builder maxOffsetsPerTrigger(int max) { ... }
        public Builder pollTimeout(Duration timeout) { ... }
        public Builder extraOption(String key, String value) { ... }
        public KafkaSourceConfig build() { ... } // validates via Jakarta Bean Validation; throws ConstraintViolationException
    }
}
```

### Factory

```java
public final class KafkaSourceFactory {
    public static KafkaSource create(SparkSession spark, KafkaSourceConfig config) { ... }
}
```

### Operations

| Method | Description |
|---|---|
| `readStream()` | Live streaming `Dataset<Row>` from the subscribed topic |
| `readBatch(KafkaOffsetRange)` | Bounded read for a specific offset range |
| `close()` | Release subscription-related state |

---

## Module: Hive Metastore

### Purpose

Provide typed operations against the Hive Metastore Thrift endpoint for database and table cataloging: checking existence, registering Delta tables, dropping tables, and listing metadata — without requiring raw SQL strings.

### Interface

```java
package com.github.castroaj.streaminglib.hive;

public interface HiveMetastoreClient {

    boolean databaseExists(String databaseName);
    void createDatabase(String databaseName, String location);

    boolean tableExists(String databaseName, String tableName);

    /**
     * Registers a Delta table at the given S3A path.
     * Issues: CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION '...'
     */
    void registerDeltaTable(String databaseName, String tableName, String s3aPath);

    /**
     * Registers a Delta table with an explicit schema (for newly created tables
     * before any data is written).
     */
    void registerDeltaTable(String databaseName, String tableName,
                            String s3aPath, StructType schema);

    void dropTable(String databaseName, String tableName, boolean purge);

    List<String> listTables(String databaseName);
    List<String> listDatabases();

    void close();
}
```

### Configuration

```java
package com.github.castroaj.streaminglib.hive;

public final class HiveMetastoreConfig {
    // Required
    private final String thriftUri;            // e.g. "thrift://hive-metastore:9083"

    // Optional (defaults shown)
    private final Duration connectionTimeout;  // Duration.ofSeconds(30)
    private final int maxRetries;              // 3
    private final Map<String, String> extraSparkConf; // empty; mapped to spark.hadoop.*

    public static Builder builder() { ... }

    public static final class Builder {
        public Builder thriftUri(String uri) { ... }
        public Builder connectionTimeout(Duration timeout) { ... }
        public Builder maxRetries(int retries) { ... }
        public Builder extraSparkConf(String key, String value) { ... }
        public HiveMetastoreConfig build() { ... } // validates via Jakarta Bean Validation; throws ConstraintViolationException
    }
}
```

### Factory

```java
public final class HiveMetastoreClientFactory {
    public static HiveMetastoreClient create(SparkSession spark,
                                             HiveMetastoreConfig config) { ... }
}
```

### Operations

| Method | Description |
|---|---|
| `databaseExists` / `createDatabase` | Database lifecycle management |
| `tableExists` / `registerDeltaTable` | Table registration via SQL DDL |
| `dropTable` | Controlled table removal with optional purge |
| `listTables` / `listDatabases` | Catalog enumeration |

---

## Module: S3 Store

### Purpose

Provide path-based operations against S3-compatible object storage (MinIO or AWS S3) using the Hadoop S3A filesystem: checking existence, reading/writing raw objects, listing prefixes, deleting paths, and resolving canonical `s3a://` URIs from logical table names.

### Interface

```java
package com.github.castroaj.streaminglib.s3;

public interface S3StoreClient {

    boolean pathExists(String s3aPath);

    /** Lists immediate child keys under the given prefix. Non-recursive. */
    List<String> listPrefix(String s3aPrefix);

    /** Writes raw bytes to the given s3a path, overwriting if present. */
    void putObject(String s3aPath, byte[] content);

    /** Reads raw bytes from the given s3a path. */
    byte[] getObject(String s3aPath);

    void deleteRecursive(String s3aPath);

    /**
     * Resolves a logical table name to its canonical warehouse path.
     * e.g. "my_table" -> "s3a://warehouse/my_table"
     */
    String resolveTablePath(String tableName);

    void close();
}
```

### Configuration

```java
package com.github.castroaj.streaminglib.s3;

public final class S3StoreConfig {
    // Required
    private final String endpoint;            // e.g. "http://minio:9000"
    private final String accessKey;
    private final String secretKey;
    private final String bucket;              // e.g. "warehouse"

    // Optional (defaults shown)
    private final boolean pathStyleAccess;    // true  (required for MinIO)
    private final boolean sslEnabled;         // false (for local MinIO)
    private final Map<String, String> extraSparkConf; // empty; mapped to fs.s3a.*

    public static Builder builder() { ... }

    public static final class Builder {
        public Builder endpoint(String endpoint) { ... }
        public Builder accessKey(String key) { ... }
        public Builder secretKey(String secret) { ... }
        public Builder bucket(String bucket) { ... }
        public Builder pathStyleAccess(boolean enabled) { ... }
        public Builder sslEnabled(boolean enabled) { ... }
        public Builder extraSparkConf(String key, String value) { ... }
        public S3StoreConfig build() { ... } // validates via Jakarta Bean Validation; throws ConstraintViolationException
    }
}
```

### Factory

```java
public final class S3StoreClientFactory {
    public static S3StoreClient create(SparkSession spark, S3StoreConfig config) { ... }
}
```

### Operations

| Method | Description |
|---|---|
| `pathExists` | Check for object or prefix existence |
| `listPrefix` | Non-recursive child key enumeration |
| `putObject` / `getObject` | Raw byte-level read/write |
| `deleteRecursive` | Recursive subtree deletion |
| `resolveTablePath` | Logical name to `s3a://` URI resolution |

---

## Module: Delta Lake Streaming Writer

### Purpose

Wrap Spark Structured Streaming's `writeStream` API for Delta format, encapsulating checkpoint path construction, trigger configuration, output mode, clustering column options, and file size targets. Callers provide a streaming `Dataset<Row>` and a destination config; the abstraction manages the `StreamingQuery` lifecycle.

### Interfaces

```java
package com.github.castroaj.streaminglib.delta;

public interface DeltaStreamWriter {

    /** Starts the streaming write. Returns a handle. Does not block. */
    StreamingQueryHandle start(Dataset<Row> streamingDataset);

    /** Starts the streaming write and blocks until the query terminates
     *  or the timeout elapses. */
    StreamingQueryHandle startAndAwait(Dataset<Row> streamingDataset, Duration timeout);

    void close();
}

public interface StreamingQueryHandle {

    boolean isActive();
    void awaitTermination();
    boolean awaitTermination(Duration timeout);
    void stop();
    StreamingQueryStatus status();
    Optional<Throwable> lastException();
}
```

### Configuration

```java
package com.github.castroaj.streaminglib.delta;

public final class DeltaStreamWriterConfig {
    // Required
    private final String destinationPath;         // e.g. "s3a://warehouse/stream_data"
    private final String checkpointBasePath;      // e.g. "s3a://warehouse/_checkpoints"
    private final String checkpointName;          // appended: basePath/checkpointName

    // Optional (defaults shown)
    private final OutputMode outputMode;          // APPEND
    private final TriggerConfig trigger;          // ProcessingTime(30s)
    private final List<String> clusteringColumns; // empty
    private final long maxRecordsPerFile;         // 5_000_000
    private final Map<String, String> extraOptions; // empty

    public static Builder builder() { ... }

    public static final class Builder {
        public Builder destinationPath(String path) { ... }
        public Builder checkpointBasePath(String path) { ... }
        public Builder checkpointName(String name) { ... }
        public Builder outputMode(OutputMode mode) { ... }
        public Builder trigger(TriggerConfig trigger) { ... }
        public Builder clusteringColumns(List<String> columns) { ... }
        public Builder maxRecordsPerFile(long max) { ... }
        public Builder extraOption(String key, String value) { ... }
        public DeltaStreamWriterConfig build() { ... } // validates via Jakarta Bean Validation; throws ConstraintViolationException
    }
}
```

### TriggerConfig

```java
package com.github.castroaj.streaminglib.delta;

public sealed interface TriggerConfig permits
        TriggerConfig.ProcessingTime,
        TriggerConfig.Once,
        TriggerConfig.AvailableNow,
        TriggerConfig.Continuous {

    record ProcessingTime(Duration interval) implements TriggerConfig {}
    record Once() implements TriggerConfig {}
    record AvailableNow() implements TriggerConfig {}
    record Continuous(Duration checkpointInterval) implements TriggerConfig {}
}
```

### Factory

```java
public final class DeltaStreamWriterFactory {
    public static DeltaStreamWriter create(SparkSession spark,
                                           DeltaStreamWriterConfig config) { ... }
}
```

### Operations

| Method | Description |
|---|---|
| `start(dataset)` | Non-blocking start; returns handle |
| `startAndAwait(dataset, timeout)` | Start with bounded wait |
| `StreamingQueryHandle.stop()` | Graceful query termination |
| `StreamingQueryHandle.awaitTermination()` | Block until completion or error |
| `StreamingQueryHandle.lastException()` | Retrieve terminal error if any |

---

## Exception Hierarchy

All public API exceptions are unchecked (`RuntimeException` subclasses). Connector checked exceptions are always wrapped before propagating to callers.

```
StreamingLibException  (RuntimeException)
├── KafkaSourceException
├── HiveMetastoreException
├── S3StoreException
└── DeltaStreamException
```

Catch `StreamingLibException` for uniform handling or a specific subtype for targeted recovery. The original connector exception is always available via `getCause()`.

**Validation exceptions**: `*Config.build()` and `*Factory.create()` throw `jakarta.validation.ConstraintViolationException` (also unchecked) when required fields are missing or blank. This is thrown before any connector or Spark interaction.

---

## Dependencies (Gradle coordinates)

```groovy
// Spark — provided by cluster at runtime
compileOnly "org.apache.spark:spark-core_2.12:3.5.8"
compileOnly "org.apache.spark:spark-sql_2.12:3.5.8"

// Logging facade — implementation provided by consuming job
compileOnly "org.slf4j:slf4j-api:2.0.13"

// Lombok — annotation processor only; not included in the thin JAR
compileOnly "org.projectlombok:lombok:1.18.36"
annotationProcessor "org.projectlombok:lombok:1.18.36"

// Jakarta Bean Validation — API + Hibernate Validator provider
implementation "jakarta.validation:jakarta.validation-api:3.0.2"
implementation "org.hibernate.validator:hibernate-validator:8.0.1.Final"

// Kafka connector (implemented)
implementation "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"

// Hadoop S3A — planned for S3 module (aligned with Spark 3.5.x bundled Hadoop version)
implementation "org.apache.hadoop:hadoop-aws:3.3.4"
implementation "com.amazonaws:aws-java-sdk-bundle:1.12.262"

// Hive Metastore Thrift client — planned for Hive module (matches docker-compose apache/hive:4.0.0)
implementation "org.apache.hive:hive-metastore:4.0.0"
implementation "org.apache.thrift:libthrift:0.16.0"

// Testing
testImplementation "org.junit.jupiter:junit-jupiter:5.11.0"
testImplementation "org.mockito:mockito-core:5.12.0"
testImplementation platform("org.testcontainers:testcontainers-bom:2.0.5")
testImplementation "org.testcontainers:testcontainers"
testImplementation "org.testcontainers:testcontainers-junit-jupiter"
testImplementation "org.testcontainers:testcontainers-kafka"
testRuntimeOnly "org.slf4j:slf4j-simple:2.0.13"

// Integration testing — real Spark session + Delta
integrationTestImplementation "org.apache.spark:spark-core_2.12:3.5.8"
integrationTestImplementation "org.apache.spark:spark-sql_2.12:3.5.8"
integrationTestImplementation "io.delta:delta-spark_2.12:3.2.1"
```

Version rationale:
- `hadoop-aws:3.3.4` and `aws-java-sdk-bundle:1.12.262` match the versions used in the sibling `ipyworkbook` demo environment.
- `hive-metastore:4.0.0` matches the `apache/hive:4.0.0` image in `docker-compose.yml`.
- `delta-spark_2.12:3.2.1` is compatible with Spark 3.5.x per the [Delta Lake release matrix](https://docs.delta.io/latest/releases.html).
- `hibernate-validator:8.0.1.Final` is the Hibernate Validator release supporting `jakarta.validation` 3.0 (Jakarta EE 10) on Java 17. Configured with `ParameterMessageInterpolator` to avoid requiring a Jakarta EL implementation on the Spark classpath.

---

## Non-Goals

The following are explicitly out of scope for `streaming-lib`:

1. **SparkSession management.** The library never creates or configures a `SparkSession`.
2. **Schema registry integration.** No Avro/Protobuf deserialization via Confluent Schema Registry. The Kafka abstraction returns raw binary `key` and `value` columns.
3. **Delta table maintenance.** `OPTIMIZE`, `VACUUM`, `RESTORE`, and time-travel reads are not exposed.
4. **Kafka producer / write path.** The library only supports consuming from Kafka.
5. **Security and credential rotation.** Credentials are static strings passed at construction time.
6. **Multi-tenant or multi-cluster Spark support.** Each library instance is bound to a single `SparkSession`.
7. **Streaming state management.** Windowing, watermarking, and stateful aggregations are not abstracted.
8. **Job scheduling or orchestration.** No cron, retry framework, or Airflow integration.
