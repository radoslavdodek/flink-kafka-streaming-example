package eu.indek.clickstream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.functions.FromElementsGeneratorFunction;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class DataStreamJob {

    private static final String KAFKA_BROKER = "kafka:9092";

    private static final String KAFKA_INPUT_TOPIC = "input-topic";
    private static final String KAFKA_OUTPUT_TOPIC = "output-topic";


    /**
     * The main entry point for the Flink application.
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (env instanceof LocalStreamEnvironment) {
            // Set the parallelism to 1 for local development
            env.setParallelism(1);
        }

        // Set up the source (`input-topic`)
        Source<String, ?, ?> source = createSource(env);

        // Set up the sink (`output-topic`)
        Sink<String> sink = createSink(env);

        // Step 1.
        // Get the DataStreamSource from your source
        DataStreamSource<String> dataStreamSource =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source");

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
        transformedStream.sinkTo(sink);

        env.execute("DataStreamJob");
    }

    private static Source<String, ?, ?> createSource(StreamExecutionEnvironment env) {
        if (env instanceof LocalStreamEnvironment) {
            // Local environment
            final ArticleEventGenerator eventGenerator = new ArticleEventGenerator();
            String[] elements = eventGenerator.getNextEventsAsJsonStrings(55);
            FromElementsGeneratorFunction<String> stringFromElementsGeneratorFunction =
                    new FromElementsGeneratorFunction<>(TypeInformation.of(String.class), elements);

            return
                    new DataGeneratorSource<>(
                            stringFromElementsGeneratorFunction,
                            elements.length,
                            RateLimiterStrategy.perSecond(1), // Generate 1 event per second
                            Types.STRING
                    );
        } else {
            // Non local environment
            return
                    KafkaSource.<String>builder()
                            .setBootstrapServers(KAFKA_BROKER)
                            .setTopics(KAFKA_INPUT_TOPIC)
                            .setGroupId("article-click-stream-flink-job")
                            .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build();
        }
    }

    private static Sink<String> createSink(StreamExecutionEnvironment env) {
        if (env instanceof LocalStreamEnvironment) {
            // Local environment
            return new PrintSink<>("LocalSink");
        } else {
            // Non local environment
            return KafkaSink.<String>builder()
                    .setBootstrapServers(KAFKA_BROKER)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                    .setTopic(KAFKA_OUTPUT_TOPIC)
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                    )
                    .build();
        }
    }

}
