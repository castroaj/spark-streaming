package com.github.castroaj.streaminglib.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.castroaj.streaminglib.testing.SparkTestSession;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for {@link KafkaSource} against a real Kafka broker.
 *
 * <p>Expected wall-clock time: ~60 seconds (Kafka container startup + Spark local mode init).
 */
@Tag("integration")
@Testcontainers
class KafkaSourceIT {

    private static final String TOPIC = "it-test-topic";
    private static final int MESSAGE_COUNT = 10;

    @Container
    private static final ConfluentKafkaContainer KAFKA =
        new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    private static SparkSession spark;

    @BeforeAll
    static void setUpSpark() {
        spark = SparkTestSession.create("KafkaSourceIT");
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.close();
        }
    }

    // -------------------------------------------------------------------------
    // Batch read
    // -------------------------------------------------------------------------

    @Test
    void readBatch_producedMessages_returnsCorrectCount() throws Exception {
        String bootstrapServers = KAFKA.getBootstrapServers();
        createTopic(bootstrapServers, TOPIC + "-batch");
        produceMessages(bootstrapServers, TOPIC + "-batch", MESSAGE_COUNT);

        KafkaSourceConfig config = KafkaSourceConfig.builder()
            .bootstrapServers(bootstrapServers)
            .topic(TOPIC + "-batch")
            .groupId("it-batch-group")
            .startingOffsets(StartingOffsets.EARLIEST)
            .build();

        KafkaSource source = KafkaSourceFactory.create(spark, config);
        try {
            KafkaOffsetRange range = KafkaOffsetRange.of(
                TOPIC + "-batch",
                Map.of(0, 0L),
                Map.of(0, (long) MESSAGE_COUNT));

            Dataset<Row> result = source.readBatch(range);
            long count = result.count();
            assertEquals(MESSAGE_COUNT, count,
                "Batch read should return exactly " + MESSAGE_COUNT + " rows");
        } finally {
            source.close();
        }
    }

    // -------------------------------------------------------------------------
    // Streaming read
    // -------------------------------------------------------------------------

    @Test
    void readStream_producedMessages_canBeQueried() throws Exception {
        String bootstrapServers = KAFKA.getBootstrapServers();
        String streamTopic = TOPIC + "-stream";
        createTopic(bootstrapServers, streamTopic);
        produceMessages(bootstrapServers, streamTopic, MESSAGE_COUNT);

        KafkaSourceConfig config = KafkaSourceConfig.builder()
            .bootstrapServers(bootstrapServers)
            .topic(streamTopic)
            .groupId("it-stream-group")
            .startingOffsets(StartingOffsets.EARLIEST)
            .maxOffsetsPerTrigger(MESSAGE_COUNT * 2)
            .build();

        KafkaSource source = KafkaSourceFactory.create(spark, config);
        StreamingQuery query = null;
        try {
            Dataset<Row> stream = source.readStream();
            assertNotNull(stream, "readStream() must return a non-null dataset");
            assertTrue(stream.isStreaming(), "Dataset returned by readStream() must be streaming");

            query = stream
                .writeStream()
                .format("memory")
                .queryName("it_stream_output")
                .start();

            // Allow one micro-batch to process
            query.processAllAvailable();

            long count = spark.sql("SELECT COUNT(*) FROM it_stream_output")
                .collectAsList()
                .get(0)
                .getLong(0);
            assertEquals(MESSAGE_COUNT, count,
                "Streaming read should have consumed " + MESSAGE_COUNT + " messages");
        } finally {
            if (query != null && query.isActive()) {
                query.stop();
            }
            source.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void createTopic(String bootstrapServers, String topic)
            throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(Collections.singletonList(
                new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private static void produceMessages(String bootstrapServers, String topic, int count)
            throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName());

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(
                    new ProducerRecord<>(topic,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8))
                ).get();
            }
        }
    }
}
