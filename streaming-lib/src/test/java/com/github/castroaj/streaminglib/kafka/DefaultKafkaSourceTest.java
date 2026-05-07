package com.github.castroaj.streaminglib.kafka;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

class DefaultKafkaSourceTest {

    private SparkSession mockSpark;
    private DataStreamReader mockStreamReader;
    private DataFrameReader mockBatchReader;
    private KafkaSourceConfig baseConfig;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockSpark = mock(SparkSession.class);
        mockStreamReader = mock(DataStreamReader.class);
        mockBatchReader = mock(DataFrameReader.class);

        Dataset<Row> mockDataset = mock(Dataset.class);

        when(mockSpark.readStream()).thenReturn(mockStreamReader);
        when(mockStreamReader.format(anyString())).thenReturn(mockStreamReader);
        when(mockStreamReader.option(anyString(), anyString())).thenReturn(mockStreamReader);
        when(mockStreamReader.option(anyString(), eq(100_000L))).thenReturn(mockStreamReader);
        when(mockStreamReader.load()).thenReturn(mockDataset);

        when(mockSpark.read()).thenReturn(mockBatchReader);
        when(mockBatchReader.format(anyString())).thenReturn(mockBatchReader);
        when(mockBatchReader.option(anyString(), anyString())).thenReturn(mockBatchReader);
        when(mockBatchReader.load()).thenReturn(mockDataset);

        baseConfig = KafkaSourceConfig.builder()
            .bootstrapServers("kafka:9092")
            .topic("test-topic")
            .groupId("test-group")
            .build();
    }

    // -------------------------------------------------------------------------
    // Post-close guards
    // -------------------------------------------------------------------------

    @Test
    void readStream_afterClose_throwsIllegalStateException() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        source.close();
        assertThrows(IllegalStateException.class, source::readStream);
    }

    @Test
    void readBatch_afterClose_throwsIllegalStateException() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        source.close();
        KafkaOffsetRange range = KafkaOffsetRange.of(
            "test-topic", Map.of(0, 0L), Map.of(0, 10L));
        assertThrows(IllegalStateException.class, () -> source.readBatch(range));
    }

    // -------------------------------------------------------------------------
    // readStream option mapping
    // -------------------------------------------------------------------------

    @Test
    void readStream_setsBootstrapServersOption() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        source.readStream();
        verify(mockStreamReader).option("kafka.bootstrap.servers", "kafka:9092");
    }

    @Test
    void readStream_setsSubscribeOption() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        source.readStream();
        verify(mockStreamReader).option("subscribe", "test-topic");
    }

    @Test
    void readStream_setsGroupIdPrefixOption() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        source.readStream();
        verify(mockStreamReader).option("groupIdPrefix", "test-group");
    }

    @Test
    void readStream_setsStartingOffsetsOption() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        source.readStream();
        verify(mockStreamReader).option("startingOffsets", "earliest");
    }

    @Test
    void readStream_extraOptionsAppliedLast() {
        KafkaSourceConfig configWithExtra = KafkaSourceConfig.builder()
            .bootstrapServers("kafka:9092")
            .topic("test-topic")
            .groupId("test-group")
            .extraOption("security.protocol", "SASL_PLAINTEXT")
            .build();

        KafkaSource source = KafkaSourceFactory.create(mockSpark, configWithExtra);
        source.readStream();
        verify(mockStreamReader).option("security.protocol", "SASL_PLAINTEXT");
    }

    // -------------------------------------------------------------------------
    // readBatch assign JSON
    // -------------------------------------------------------------------------

    @Test
    void readBatch_setsBootstrapServersOption() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        KafkaOffsetRange range = KafkaOffsetRange.of(
            "test-topic", Map.of(0, 0L), Map.of(0, 10L));
        source.readBatch(range);
        verify(mockBatchReader).option("kafka.bootstrap.servers", "kafka:9092");
    }

    @Test
    void readBatch_setsAssignOptionWithTopicAndPartition() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        KafkaOffsetRange range = KafkaOffsetRange.of(
            "test-topic", Map.of(0, 5L), Map.of(0, 15L));
        source.readBatch(range);
        // assign JSON contains the topic name
        verify(mockBatchReader).option(eq("assign"), org.mockito.ArgumentMatchers.contains("test-topic"));
    }

    @Test
    void readBatch_nullRange_throwsNullPointerException() {
        KafkaSource source = KafkaSourceFactory.create(mockSpark, baseConfig);
        assertThrows(NullPointerException.class, () -> source.readBatch(null));
    }
}
