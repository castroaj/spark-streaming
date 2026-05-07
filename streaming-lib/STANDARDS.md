# streaming-lib Engineering Standards

---

## 1. Java Code Style

### Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Packages | All lowercase, domain-reversed | `com.example.streaminglib.kafka` |
| Classes / Interfaces | UpperCamelCase, noun phrases | `KafkaSourceConfig`, `DeltaStreamWriter` |
| Methods | lowerCamelCase, verb or verb-noun | `readStream()`, `isActive()` |
| Constants | UPPER_SNAKE_CASE | `DEFAULT_POLL_TIMEOUT_SECONDS` |
| Local variables | lowerCamelCase, descriptive | `streamingDataset`, `checkpointPath` |
| Test methods | lowerCamelCase describing scenario | `build_missingBootstrapServers_throwsIllegalArgumentException()` |

Avoid abbreviations unless industry-standard (e.g., `s3`, `uri`, `sql`). Do not use single-letter variable names except inside lambda bodies where context is unambiguous.

### Package Structure

Every public type in a package must have a clear single responsibility. The only public types per package are:

- The primary interface (`KafkaSource`, `HiveMetastoreClient`, etc.)
- The config value object (`KafkaSourceConfig`, etc.)
- The factory class (`KafkaSourceFactory`, etc.)
- Public exception types in `com.example.streaminglib.exception`
- Public enum / sealed type hierarchies (`TriggerConfig`, `OutputMode`, `StartingOffsets`)

Implementation classes (e.g., `DefaultKafkaSource`, `DefaultDeltaStreamWriter`) are **package-private**.

### Immutability

All configuration objects (`*Config`) must be fully immutable after construction:

- All fields `private final`
- No setters
- Constructed exclusively via inner `Builder` classes
- Collections stored as `List.copyOf()` / `Map.copyOf()` inside `Builder.build()`
- `Builder.build()` validates all required fields and throws `IllegalArgumentException` with a descriptive message listing every missing field in a single exception

Non-config classes should be immutable where possible. Mutable state must be confined to implementation internals and protected with appropriate synchronization if shared across threads.

### Null Handling

- Public API methods must never accept `null` as a parameter. Use `Objects.requireNonNull(param, "param must not be null")` at the top of every public method.
- Public API methods must never return `null`. Return `Optional<T>` for values that may be absent (e.g., `StreamingQueryHandle.lastException()`).
- Treat `null` in internal implementation code as a programmer error, not a control-flow signal.

---

## 2. Build Conventions

### Source Sets

| Source Set | Directory | Purpose |
|---|---|---|
| `main` | `src/main/java` | Production library code |
| `test` | `src/test/java` | Unit tests — no Spark session, no network I/O |
| `integrationTest` | `src/integrationTest/java` | Integration tests — real Spark session, may need connectors |

The `integrationTest` source set is defined explicitly in `build.gradle` and wired to a separate Gradle task. It never runs as part of the default `test` task or `check` lifecycle.

### Gradle Tasks

| Task | Purpose |
|---|---|
| `./gradlew test` | Run unit tests only |
| `./gradlew integrationTest` | Run integration tests only |
| `./gradlew check` | Run `test` + static analysis (SpotBugs, Checkstyle); does NOT include `integrationTest` |
| `./gradlew build` | Compile + `check` + produce thin JAR |
| `./gradlew shadowJar` | Produce an uber/fat JAR with all `runtimeClasspath` dependencies |
| `./gradlew javadoc` | Generate HTML Javadoc |

Always use `./gradlew` (wrapper). Never rely on a globally installed Gradle binary.

### JAR Strategy

The default `jar` task produces a **thin JAR** containing only compiled library classes and `META-INF`. This is the artifact intended for `spark-submit --jars`.

The `shadowJar` task (Shadow plugin: `com.github.johnrengelman.shadow`) produces a fat JAR for standalone use. The shadow JAR must:

- Use the classifier `all` to distinguish it from the thin JAR
- Relocate conflicting packages to avoid classpath pollution in the Spark driver:
  - `com.google`
  - `org.apache.commons`
  - `okhttp3`
  - SLF4J implementation classes (if bundled for integration tests)

### Compiler Settings

```groovy
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

compileJava.options.encoding = "UTF-8"
compileTestJava.options.encoding = "UTF-8"
```

---

## 3. Error Handling Philosophy

### Unchecked Exceptions Only

All exceptions thrown by the public API are unchecked (`RuntimeException` subclasses). Connector checked exceptions (e.g., `TException` from Thrift, `KafkaException`) are always caught internally and re-thrown as the appropriate `StreamingLibException` subtype.

Rationale: Spark's own APIs are unchecked. Checked exceptions on Spark job methods provide no practical safety benefit and produce verbose code.

### Wrapping Rules

1. Every catch of a connector-level exception must wrap and re-throw. Silent swallowing is forbidden.
2. The original exception must always be passed as the `cause` to the wrapping exception.
3. The wrapping exception message must include: the operation being attempted, the relevant resource identifier, and the original message. Example:

   ```
   HiveMetastoreException: Failed to register Delta table 'events' in database 'raw'
     at thrift://hive-metastore:9083 — TException: Connect timed out
   ```

4. `IllegalArgumentException` and `IllegalStateException` (for programming errors like null config or calling a method after `close()`) are thrown directly without wrapping as `StreamingLibException`.

### Validation Timing

- Config validation happens in `Builder.build()`, before any connector is touched.
- Precondition checks (e.g., verifying a source hasn't been `close()`d) happen at the top of each method, before any I/O.
- No silent fallback to defaults after construction. Every missing required field causes `build()` to throw `IllegalArgumentException` listing all missing fields in a single message.

---

## 4. Logging Standards

### Framework

SLF4J API (`slf4j-api`) is the only logging dependency in the `implementation` configuration. No logging implementation (Logback, Log4j, `slf4j-simple`) is bundled in the thin JAR. The consuming Spark job provides the binding.

```java
private static final Logger log = LoggerFactory.getLogger(DefaultKafkaSource.class);
```

Use the concrete class literal, never `getClass()`, to allow proper category configuration.

### Log Levels

| Level | When to use |
|---|---|
| `ERROR` | Unrecoverable failure — exception is about to propagate to the caller |
| `WARN` | Recoverable or degraded state — retries, fallbacks, deprecated config keys |
| `INFO` | Lifecycle events: client opened, streaming query started/stopped, table registered |
| `DEBUG` | Per-operation detail: resolved path, offset range, trigger interval used |
| `TRACE` | Raw connector interaction: individual Kafka poll cycles, Thrift call arguments |

Do not log at `INFO` inside loops or per-record/per-batch processing paths. Use `DEBUG` or `TRACE` for high-frequency events.

### Structured Fields

Always use SLF4J parameterized logging. Never use string concatenation in log calls — it evaluates even when the level is disabled.

```java
// Correct
log.info("Registered Delta table '{}' in database '{}' at path '{}'",
         tableName, databaseName, s3aPath);

// Wrong
log.info("Registered Delta table '" + tableName + "'...");
```

Log messages must include the relevant resource identifier (table name, topic, path, URI) to make lines self-contained for grep-based debugging. Never log credentials, access keys, or secret keys at any level.

---

## 5. Configuration Standards

### Design Principles

- **No global state.** There are no static config registries, singleton holders, or thread-local config stores. Every factory call receives an explicit config object.
- **Constructor injection only.** Factory methods accept a `SparkSession` and a `*Config` object. No property file loading occurs inside the library.
- **Builder pattern for all configs.** Every `*Config` is constructed via its inner `Builder`. Direct constructor calls are not part of the public API. Builder access is always `SomeConfig.builder()` (static factory method).
- **Configs are value objects.** Once built, a `*Config` instance is immutable and safe to share across threads.
- **Fail at build time, not at I/O time.** Config validation (required fields present, URIs parseable, numeric ranges valid) happens in `Builder.build()`, not when the first network call is made.

### Extra Options Escape Hatch

Every `*Config` includes a `Map<String, String> extraOptions` or `extraSparkConf` field for raw connector properties. These options are applied last and may override library-set defaults. This must be documented on each config class. The escape hatch exists to avoid version-bumping the library for every new Spark or connector option.

### Framework Agnostic

The library does not integrate with Spring `@ConfigurationProperties`, Micronaut `@ConfigurationInject`, or any other framework's config mechanism. Consuming applications populate builder fields from their own config source (environment variables, YAML, etc.) before calling `build()`.

---

## 6. Testing Standards

### Unit Tests

Unit tests live in `src/test/java` and run via `./gradlew test`.

- No `SparkSession` instantiation.
- No network calls or file system I/O.
- Mock all connector interactions using Mockito.
- Each test must complete in under 5 seconds.
- Use JUnit 5 (`@Test`, `@BeforeEach`, `@ExtendWith`).
- Test class names: `<ClassUnderTest>Test` — e.g., `KafkaSourceConfigTest`
- Test method names: `methodName_condition_expectedBehavior()` — e.g., `build_missingBootstrapServers_throwsIllegalArgumentException()`

Focus unit test coverage on:

- Config builder validation (missing required fields, invalid values, boundary conditions)
- Exception wrapping (verify cause is preserved, message contains resource identifier)
- Path resolution logic in `S3StoreClient`
- `TriggerConfig` sealed type mapping to Spark trigger objects

### Integration Tests

Integration tests live in `src/integrationTest/java` and run via `./gradlew integrationTest`.

- A real `SparkSession` is created once per test class in `@BeforeAll` and closed in `@AfterAll`.
- Must not require a live MinIO, Kafka, or Hive Metastore to run. Use local filesystem paths and in-process alternatives.
- `SparkSession` runs in local mode: `.master("local[2]")`.
- Integration test class names: `<ClassUnderTest>IT` — e.g., `DeltaStreamWriterIT`
- Annotate with `@Tag("integration")` and note expected wall-clock time in a comment.

### SparkSession Test Helper

A shared utility class (not part of the public API) is provided in the `integrationTest` source set:

```java
// src/integrationTest/java/com/example/streaminglib/testing/SparkTestSession.java
public final class SparkTestSession {

    public static SparkSession create(String appName) {
        return SparkSession.builder()
            .master("local[2]")
            .appName(appName)
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    }

    private SparkTestSession() {}
}
```

### Mocking Connector Interfaces

Mock the public interfaces (`KafkaSource`, `HiveMetastoreClient`, etc.), not the implementation classes. This ensures tests depend only on the public contract:

```java
KafkaSource mockSource = Mockito.mock(KafkaSource.class);
when(mockSource.readStream()).thenReturn(fakeDataset);
```

---

## 7. Documentation Standards

### Javadoc Requirements

Every public type and public method must have a Javadoc comment.

| Element | Required Content |
|---|---|
| Interface | One-paragraph description of purpose and lifecycle |
| Interface method | Description, `@param` for each parameter, `@return` for non-void, `@throws` for each permitted exception type |
| `*Config` class | One-line description; note instances are immutable and thread-safe |
| `Builder` method | One line describing the field; `@param`; note if required vs optional |
| `Builder.build()` | List all required fields; document `IllegalArgumentException` thrown if any are missing |
| Factory class/method | What is created; `@param` for `SparkSession` and config; `@throws StreamingLibException` for initialization-time failure |

Package-private implementation classes must have Javadoc on methods that implement a public interface. `{@inheritDoc}` is acceptable where behavior is identical to the interface contract.

### Format Rules

- First sentence of every Javadoc must be a complete, punctuated sentence.
- `@param` and `@return` tags describe meaning, not type.
- Do not use `<p>` tags for single-paragraph Javadocs.
- Code references use `{@code ...}` (not backticks).
- Cross-references use `{@link ...}` for types and methods within the library.
- Config fields that map to an underlying Spark or connector property must note the property key:

  ```java
  /**
   * S3A endpoint URL. Mapped to {@code spark.hadoop.fs.s3a.endpoint}.
   */
  ```

### CHANGELOG

`CHANGELOG.md` at the project root follows [Keep a Changelog](https://keepachangelog.com/) format. Every change that affects the public API surface must have an entry under `### Added`, `### Changed`, or `### Removed`.
