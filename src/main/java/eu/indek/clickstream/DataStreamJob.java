package eu.indek.clickstream;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableIterator;
import org.apache.flink.util.IterableUtils;

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

        // Set up the source (`input-topic`)
        Source<ArticleEvent, ?, ?> source = createSource(env);

        // Set up the sink (`output-topic`)
        Sink<ArticleEventCount> sink = createSink(env);

        // Step 1.
        // Get the DataStreamSource from your source
        DataStreamSource<ArticleEvent> dataStreamSource =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source");

        // Print the events to the console
        PrintSink<ArticleEvent> localSink = new PrintSink<>(">>> ");
        dataStreamSource.sinkTo(localSink);

        // Step 2.
        // Aggregate the ArticleEvents by articleId and count them in a sliding window of 100 seconds with a 10-second slide
        SingleOutputStreamOperator<ArticleEventCount> aggregatedStream = dataStreamSource
                .keyBy(a -> a.articleId)
                .window(SlidingProcessingTimeWindows.of(
                        Duration.of(100, ChronoUnit.SECONDS),
                        Duration.of(10, ChronoUnit.SECONDS))
                )
                .process(new ArticleEventCountWindowFunction());

        // Step 3.
        // Sink
        aggregatedStream.sinkTo(sink);

        env.execute("DataStreamJob");
    }

    private static Source<ArticleEvent, ?, ?> createSource(StreamExecutionEnvironment env) {
        if (env instanceof LocalStreamEnvironment) {
            // Local environment
            final ArticleEventGenerator eventGenerator = new ArticleEventGenerator();
            ArticleEvent[] elements = eventGenerator.getNextEvents(55);
            FromElementsGeneratorFunction<ArticleEvent> dataGeneratorFunction =
                    new FromElementsGeneratorFunction<>(TypeInformation.of(ArticleEvent.class), elements);

            return
                    new DataGeneratorSource<>(
                            dataGeneratorFunction,
                            elements.length,
                            RateLimiterStrategy.perSecond(1), // Generate 1 event per second
                            Types.POJO(ArticleEvent.class)
                    );
        } else {
            // Non local environment
            return
                    KafkaSource.<ArticleEvent>builder()
                            .setBootstrapServers(KAFKA_BROKER)
                            .setTopics(KAFKA_INPUT_TOPIC)
                            .setGroupId("article-click-stream-flink-job")
                            .setValueOnlyDeserializer(new TypeInformationSerializationSchema<>(TypeInformation.of(ArticleEvent.class), env.getConfig()))
                            .build();
        }
    }

    private static Sink<ArticleEventCount> createSink(StreamExecutionEnvironment env) {
        if (env instanceof LocalStreamEnvironment) {
            // Local environment
            return new PrintSink<>("LocalSink");
        } else {
            // Non local environment
            return KafkaSink.<ArticleEventCount>builder()
                    .setBootstrapServers(KAFKA_BROKER)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                    .setTopic(KAFKA_OUTPUT_TOPIC)
                                    .setValueSerializationSchema(new TypeInformationSerializationSchema<>(TypeInformation.of(ArticleEventCount.class), env.getConfig()))
                                    .build()
                    )
                    .build();
        }
    }

    public static class ArticleEventCount {
        private String articleId;
        private long count;

        public ArticleEventCount() {
        }

        public ArticleEventCount(String articleId, long count) {
            this.articleId = articleId;
            this.count = count;
        }

        public String getArticleId() {
            return articleId;
        }

        public void setArticleId(String articleId) {
            this.articleId = articleId;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "ArticleEventCount{" +
                    "articleId='" + articleId + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    public static class ArticleEventCountWindowFunction extends ProcessWindowFunction<ArticleEvent, ArticleEventCount, String, TimeWindow> {
        @Override
        public void process(
                String key,
                ProcessWindowFunction<ArticleEvent, ArticleEventCount, String, TimeWindow>.Context context,
                Iterable<ArticleEvent> elements, Collector<ArticleEventCount> out) throws Exception
        {
            long count = Iterables.size(elements);
            out.collect(new ArticleEventCount(key, count));
        }
    }

}
