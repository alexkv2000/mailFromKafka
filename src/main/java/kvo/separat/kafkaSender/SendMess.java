package kvo.separat.kafkaSender;

import kvo.separat.kafkaSender.message.Message;
import kvo.separat.kafkaSender.message.MessageBuilder;
import kvo.separat.kafkaSender.producer.KafkaProducerFactory;
import kvo.separat.kafkaSender.producer.MessageSender;
import kvo.separat.kafkaSender.producer.Producer;

import java.time.format.DateTimeFormatter;
import java.time.LocalTime;
import java.util.List;

public class SendMess {
    static final int count = 200;

    public static void main(String[] args) {
        String startDate = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

        // Конфигурация Kafka
        KafkaProducerFactory producerFactory = new KafkaProducerFactory("doc-test:9092");
        Producer producer = producerFactory.createProducer();

        // Создание отправителя сообщений
        MessageSender messageSender = new MessageSender(producer);

        // Параметры сообщений
        String topic = "test-topic";
        String recipient = "KvochkinAY@itsnn.ru";
        String recipientCC = null;
        //recipientCC = "AlexKv2000@mail.ru";

        List<String> urls = List.of(
                "http://zagorie.ru/upload/iblock/4ea/4eae10bf98dde4f7356ebef161d365d5.pdf",
                "http://tvojkomp.ru/wp-content/uploads/2016/08/pdf-forma-dlya-zapolneniya.pdf",
                "http://usefulscript.ru/download/pdf_doc_site.doc",
                "http://crewmarket.net/wp-content/uploads/application_form.xls"
        );

        // Отправка сообщений
        for (int i = 0; i < count; i++) {
            MessageBuilder messageBuilder = MessageBuilder.builder()
                    .to(recipient)
                    // .toCC(recipientCC)
                    .caption(String.format("%s %d: %s ", "!Потоков 20: Тема сообщения ", i, LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))))
                    .body(String.format("%s : %d %s", "Тело сообщения", i, "сообщение"))
                    .urls(urls)
                    .uuid();

            Message message = messageBuilder.build();

            messageSender.sendMessage(topic, message);
        }

        // Закрытие producer
        producer.close();

        System.out.println("Дата старта    : " + startDate + "\nДата окончания : " + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    }
}