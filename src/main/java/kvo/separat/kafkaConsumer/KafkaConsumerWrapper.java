package kvo.separat.kafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerWrapper {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerWrapper.class);
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public KafkaConsumerWrapper(ConfigLoader configLoader) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configLoader.getProperty("BROKER"));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, configLoader.getProperty("GROUP_ID"));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(consumerProps);
        this.topic = configLoader.getProperty("TOPIC");

        consumer.subscribe(Collections.singletonList(topic));
        logger.info("Start Kafka source ...");
    }

    public Iterable<ConsumerRecord<String, String>> pollRecords() {
        return consumer.poll(Duration.ofMillis(100));
    }
}
