package eu.indek.clickstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/**
 * Continuously sends simulated article events to Kafka
 */
public class KafkaDummyProducer {

    private static final Logger LOG = LogManager.getLogger(KafkaDummyProducer.class);

    private static void checkUsage(String[] args) {
        if (args.length != 1) {
            System.out.println();
            System.out.println("Usage: KafkaDummyProducer <topic>");
            System.out.println();
            System.exit(0);
        }
    }

    private static void sendMessage(
            ArticleEvent event, Producer<String, String> producer, String topic)
    {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.articleId, event.toJsonString());

        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.warn("Error sending event to Kafka", exception);
                } else {
                    System.out.printf("Sent message to topic %s partition %d with offset %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            LOG.warn("Error sending event to Kafka", e);
        }
    }

    public static void main(String[] args) throws Exception {
        checkUsage(args);

        String topic = args[0];

        Properties producerProperties = KafkaProducerConfig.getProducerProperties();

        try (Producer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties)) {
            ArticleEventGenerator eventGenerator = new ArticleEventGenerator();

            //noinspection InfiniteLoopStatement
            while (true) {
                ArticleEvent event = eventGenerator.getNextEvent();
                sendMessage(event, kafkaProducer, topic);

                //noinspection BusyWait
                Thread.sleep(1000);
            }
        }
    }

}
