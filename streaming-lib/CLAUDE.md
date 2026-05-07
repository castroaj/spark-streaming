# CLAUDE.md — streaming-lib

This file provides guidance to Claude Code when working in the `streaming-lib/` directory.

## Project Identity

| Property | Value |
|---|---|
| Group | `com.github.castroaj` |
| Artifact | `spark-streaming-lib` |
| Version | `0.1.0-SNAPSHOT` |
| Java | 17 (toolchain) |
| Gradle | 9.5.0 (via wrapper — always use `./gradlew`, never a global `gradle`) |

## What This Is

A Java library providing typed, builder-based abstractions over four connector surfaces for a Spark streaming lakehouse: Kafka, Hive Metastore, S3/MinIO, and Delta Lake. See `SPEC.md` for the full API specification and `STANDARDS.md` for engineering standards.

The library is **not a framework** — it provides building blocks. `SparkSession` is always caller-provided; the library never calls `SparkSession.builder()` internally.

## Build Tasks

```bash
./gradlew build           # compile + check (test + static analysis); produces thin JAR
./gradlew test            # unit tests only (no Spark, no network)
./gradlew integrationTest # integration tests (real Spark session, local mode)
./gradlew check           # test + Checkstyle + SpotBugs; does NOT include integrationTest
./gradlew shadowJar       # fat/uber JAR with classifier 'all' for spark-submit
./gradlew javadoc         # generate HTML Javadoc
```

`integrationTest` is intentionally excluded from `check` and `build` — run it explicitly.

## Source Layout

```
src/main/java/com/github/castroaj/streaminglib/
    kafka/          # KafkaSource, KafkaSourceConfig, KafkaSourceFactory
    hive/           # HiveMetastoreClient, HiveMetastoreConfig, HiveMetastoreClientFactory
    s3/             # S3StoreClient, S3StoreConfig, S3StoreClientFactory
    delta/          # DeltaStreamWriter, DeltaStreamWriterConfig, DeltaStreamWriterFactory
    config/         # Shared config base types
    exception/      # StreamingLibException hierarchy
    util/           # Internal utilities (not public API)

src/test/java/com/github/castroaj/streaminglib/
    # Unit tests — no SparkSession, no network, Testcontainers for any container needs

src/integrationTest/java/com/github/castroaj/streaminglib/
    testing/        # SparkTestSession helper (local[2] Spark with Delta extensions)
    # Integration test classes named *IT, annotated @Tag("integration")
```

## Plugins & Versions

Plugin versions are declared in `pluginManagement` in `settings.gradle`. `build.gradle` references plugins by ID only.

| Plugin | Version | Purpose |
|---|---|---|
| `com.gradleup.shadow` | 8.3.6 | Fat JAR (`shadowJar` task); Gradle 9 compatible fork of johnrengelman/shadow |
| `com.github.spotbugs` | 6.0.18 | Static analysis |
| `checkstyle` | 10.17.0 | Style enforcement (config: `checkstyle/checkstyle.xml`) |

> **Note:** Use `com.gradleup.shadow` — not `com.github.johnrengelman.shadow`. The latter fails on Gradle 9 with a `MissingPropertyException` on `.mode`.

## Key Dependencies

```groovy
// Logging facade only — NO logging implementation in the thin JAR
compileOnly 'org.slf4j:slf4j-api:2.0.13'

// Testing
testImplementation 'org.junit.jupiter:junit-jupiter:5.11.0'
testImplementation platform('org.testcontainers:testcontainers-bom:1.20.4')
testImplementation 'org.testcontainers:testcontainers'
testImplementation 'org.testcontainers:junit-jupiter'
```

Connector dependencies (Spark, Kafka, Delta, Hive, S3A) are added per module as they are implemented. The `integrationTest` configuration extends `testImplementation`.

## Coding Standards

Enforced by Checkstyle + SpotBugs. Key rules from `STANDARDS.md`:

- Public API: one interface + one `*Config` + one `*Factory` per package. Implementation classes are **package-private**.
- All `*Config` objects are immutable (builder pattern, `List.copyOf` / `Map.copyOf`).
- No `null` in or out of the public API — use `Optional<T>` for absent values.
- Unchecked exceptions only; connector checked exceptions always wrapped as `StreamingLibException` subtypes.
- SLF4J parameterized logging only — no string concatenation, no `System.out`.
- Unit tests: no Spark, no I/O, complete in < 5 s. Use Testcontainers for container-backed tests.
- Integration tests: named `*IT`, annotated `@Tag("integration")`, Spark in `local[2]` mode via `SparkTestSession`.
