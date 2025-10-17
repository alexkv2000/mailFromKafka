package kvo.separat.kafkaSender.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;

//Реализация интерфейса Producer для Kafka
public class KafkaProducerImpl implements Producer{
    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaProducerImpl(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void send(String topic, String message, Callback callback) {
        ProducerRecord<String, String> recordTopicMessage = new ProducerRecord<>(topic, message); //TODO по необходимости добавить new ProducerRecord<>(topicName, 1, null, message); -  Отправка в partition 1
        kafkaProducer.send(recordTopicMessage,callback);
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}
