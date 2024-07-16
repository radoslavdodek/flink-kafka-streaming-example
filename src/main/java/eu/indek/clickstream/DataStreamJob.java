package eu.indek.clickstream;

import eu.indek.clickstream.model.ArticleClickCount;
import eu.indek.clickstream.model.ArticleClickCountResult;
import eu.indek.clickstream.serialization.ArticleEventDeserializationSchema;
import eu.indek.clickstream.sink.LogSink;
import eu.indek.clickstream.transformation.CountAggregator;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

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

        // Set up the source (print to console or read from Kafka topic `input-topic`)
        Source<ArticleEvent, ?, ?> source = createSource(env);

        // Step 1.
        // Get the DataStreamSource from your source
        WatermarkStrategy<ArticleEvent> watermarkStrategy = WatermarkStrategy
                .<ArticleEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<ArticleEvent>) (event, timestamp) -> event.getEventTime());

        DataStream<ArticleEvent> sourceStream = env
                .fromSource(source, watermarkStrategy, "sourceStream")
                .uid("sourceStream");

        // Print the source stream for debugging purposes
        sourceStream.sinkTo(new LogSink<>("Received event: {}"));

        // Step 2.
        // Aggregate the data by articleId each 5 seconds (tumbling window)
        SingleOutputStreamOperator<String> aggregatedStream = sourceStream
                .keyBy(a -> a.articleId)
                .window(TumblingEventTimeWindows.of(Duration.of(5, ChronoUnit.SECONDS)))
                .aggregate(new CountAggregator<>(), new ArticleClickCountResult())
                .map(ArticleClickCount::toJsonString)
                .uid("aggregatedStream");

        // Step 3.
        // Sink (print to console or write to Kafka topic `output-topic`)
        aggregatedStream.sinkTo(createSink(env));

        env.execute("DataStreamJob");
    }

    private static Source<ArticleEvent, ?, ?> createSource(StreamExecutionEnvironment env) {
        if (env instanceof LocalStreamEnvironment) {
            // Local environment
            final int recordsPerSecond = 1; // Generate 1 event per second
            final ArticleEventGenerator eventGenerator = new ArticleEventGenerator();
            ArticleEvent[] elements = eventGenerator.getNextEvents(55, recordsPerSecond);
            FromElementsGeneratorFunction<ArticleEvent> stringFromElementsGeneratorFunction =
                    new FromElementsGeneratorFunction<>(TypeInformation.of(ArticleEvent.class), elements);

            return
                    new DataGeneratorSource<>(
                            stringFromElementsGeneratorFunction,
                            elements.length,
                            RateLimiterStrategy.perSecond(recordsPerSecond),
                            Types.POJO(ArticleEvent.class)
                    );
        } else {
            // Non local environment
            return KafkaSource.<ArticleEvent>builder()
                    .setBootstrapServers(KAFKA_BROKER)
                    .setTopics(KAFKA_INPUT_TOPIC)
                    .setGroupId("article-click-stream-flink-job")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new ArticleEventDeserializationSchema())
                    .build();
        }
    }

    private static Sink<String> createSink(StreamExecutionEnvironment env) {
        if (env instanceof LocalStreamEnvironment) {
            // Local environment
            return new LogSink<>("Aggregation: {}");
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
