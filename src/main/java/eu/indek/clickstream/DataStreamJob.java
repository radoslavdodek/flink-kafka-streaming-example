package eu.indek.clickstream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {

    private static final String KAFKA_BROKER = "kafka:9092";

    private static final String KAFKA_INPUT_TOPIC = "input-topic";
    private static final String KAFKA_OUTPUT_TOPIC = "output-topic";


    /**
     * The main entry point for the Flink application.
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the Kafka source (`input-topic`)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKER)
                .setTopics(KAFKA_INPUT_TOPIC)
                .setGroupId("flink-consumer-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Set up the Kafka sink (`output-topic`)
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BROKER)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(KAFKA_OUTPUT_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        // Step 1.
        // Add the Kafka consumer as a source to the Flink job
        DataStreamSource<String> dataStreamSource =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Step 2.
        // Transform the data
        // In this example, we will convert the JSON to a CSV
        SingleOutputStreamOperator<String> transformedStream = dataStreamSource.map(value -> {
            ArticleEvent articleEvent = ArticleEvent.fromJsonString(value);
            if (articleEvent != null) {
                // Convert to CSV format:
                return String.format("%s;%s;%s", articleEvent.articleId, articleEvent.action, articleEvent.eventTime);
            } else {
                return null;
            }
        });

        // Step 3.
        // Sink
        transformedStream.sinkTo(kafkaSink);

        env.execute("Kafka Flink Job");
    }

}
