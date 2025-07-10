package kvo.separat.kafkaSender;

import kvo.separat.kafkaSender.message.Message;
import kvo.separat.kafkaSender.message.MessageBuilder;
import kvo.separat.kafkaSender.producer.KafkaProducerFactory;
import kvo.separat.kafkaSender.producer.MessageSender;
import kvo.separat.kafkaSender.producer.Producer;
import org.json.JSONObject;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class SendMessBin {
    static final int count = 2;

    public static void main(String[] args) {
        String startDate = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

        // Конфигурация Kafka
        KafkaProducerFactory producerFactory = new KafkaProducerFactory("172.18.8.60:9092,172.18.2.198:9092");
        Producer producer = producerFactory.createProducer();

        // Создание отправителя сообщений
        MessageSender messageSender = new MessageSender(producer);

        // Параметры сообщений
        String topic = "topicDVMessage";
        String message = "{\n" +
                "    \"uuid\": \"4c9f9132-3c22-419e-94a1-d4c9d59882e3\",\n" +
                "    \"typeMes\": \"1912\",\n" +
                "    \"To\": \"KvochkinAY@itsnn.ru\",\n" +
                "    \"ToCC\": \"KvochkinAY@itsnn.ru\",\n" +
                "    \"Caption\": \"Тестовое сообщение \",\n" +
                "    \"Body\": \"Тело сообщения : 0 сообщение\",\n" +
                "    \"Url\": {\n" +
                "    \"Печатная форма тестовая для БД 3.pdf\": \"JVBERi0xLjQKMSAwIG9iago8PAovQ3JlYXRvciAoT3JhY2xlMTJjIEFTIFJlcG9ydHMgU2VydmljZXMpCi9DcmVhdGlvbkRhdGUgKEQ6MjAyNDEwMzExNjM3MDkpCi9Nb2REYXRlIChEOjIwMjQxMDMxMTYzNzA5KQovUHJvZHVjZXIgKE9yYWNsZSBQREYgZHJpdmVyKQovVGl0bGUgKGZuX2J1ZGdldF82MzkzMy5wZGYpCi9BdXRob3IgKE9yYWNsZSBSZXBvcnRzKQo+PgplbmRvYmoKNSAwIG9iago8PC9MZW5ndGggNiAwIFIKL0ZpbHRlciBbL0FTQ0lJODVEZWNvZGUgL0ZsYXRlRGVjb2RlXQo+PgpzdHJlYW0KR2IiLyk5OTdqZCUpMSZrbkA/P0VISjErYydVWFknNV05KC1rYjc0QkQxRVQ1MW1lND84ZWBeT25vTTdtRDNDSEJmPFtPU3AkcScwCixlXXBwXWJRTkM3LWo8OXE9TlZnPig/UzgrSVhETnAkcVA7RGlKRlo1OkcmY2VhdWFvRGxuU0c8JC1SPHFQLScjKEtUZC9CXG8nZAouT0RyTWwrcnUma0dRNVU+NCVxWTZSdTEuRlMzN24zLi01SStALC8+VDguVlcjakYjKDBDOkZRMCtVZyJycCdJRG5ASzFGJilSZ1wKMWYjMig2XWM2cUheYy08I1dncT0xRzY4c3JKQ3RRSikrTiZwaUozQkAtRHI7QkYzViFzL1BOKXFLRU4lYVJmOiJNPktfLFUzLjFuCmtyNHMpPE9gYFtZKUoqQGo1dW8oRFFbZy1wOFpqJWVGTlArUjFVRDFSNS1PMCQ9dV4tWy0wQ0pCSz1pVWk3UEhfPCFeZGonalxZOwpLPjJjTmImQlFrSFlDQTA9WjpJTWolMiZwOi5pKWw1YChZQWZWNjxgXS4mU1M4ZGc3PjQ2SEJRVD42PUhjYS1WK0ZdOl05YXUqNS0KYGZOYGpaOm9PUDlUTG9FLF5KYTRtLC5AS0lMa2lJWTNoaV0xLkRYc0dFPFItOGsiTD5OJSNIPGwoMT00R1cqVlQ3LXJmZDg3UVJTClliMT4rcG5eRG9JcWVsaGpSdDJpais7SE1RY1hQJl5RNVo0I0BXOUtMXE5mISdiI1JhKSlxNnRyUz9JUypGXj11PDJkITNxSGQlbQpKODguP1kxTiVnVC5yUypsVFAjdW1OSkRYbE9eUkdkNUtzVDRxSUVvXD1YPyRHb0ZHSm5WZ1JOSURta00hKFEzSFgqJktqKlE1aEUKIzhNOE1RUHEoWXBgKSFFK3N0KUNqR3FVdU5PLGEuNS5LO2lcTWQ3TEpVNmZLaj1wKSlqZ0hjKVI/WXQiUGdzMkxWK2cya19mc3A3CiJwcmgvO11SYk1XKGN1aCNHRj9ZOUBIO3UwaFZxPyotTl0tSnJDZ05AQi5dVShSK0FfQGYpa2RcaHUjP1NIW1U7OFZhJnFpK15hXQpfNzJMYE9MSidrIS88KE9dWVo0bk9tUzBTSFE+QzI7NE9WUypTPW4+RD1MOi5maF4/ZFM7ZyhfOm5AYG0sT08hUyJGZyJDZkEydWQKNTJKX1RWOUsvckZAWks7XTcoPkFiVEh1dGotLSxLRihbZFVAUms8NVxlIjw5WHFzZzkraTZtPENaZmNkblBeUTwwUnRGMUwvJ1MhCkNhK18/QDg3NS8hJTMhNClrTCdcazZUYW9LM1BtX2NwQm1KUzBZOk5rWSk+QzAzLT4pTDYsXW1FXVxpajZiSEI6QkMmRDdJSFNJTApMQDJAbj02dF0rVj5CaHVXb1lfOFlBMDVxZD8pLycwOkVtQ0VCMm8xRClHKDFNTT1hXkpyL2Q6Y3JuN10pJW5oX1Rzc2dbOl5UT3QKbHQ6dWJUYyNsTkI3Xm1MYF8sSS5jc2RaTkhhLFlkQkpJSGhuLzl1QithRCNYZGdLPVtEIzojZyM1KlE3ZG1McnBrU2Y+VCxKLVY4CitLLDhcLjZSZV8tL0pHMGJZV1czNjtIUmRLaTZsPFZhTExhUzM+K1FfMSFGJihlOGpxU2ZYPGc3M0A7VztvSnNjanJXNSVLLUpeSAoxYz5iJzJAVHQlJ0leU0FJJj9tY1FzX3RkRC40ZitLQSQ\"" +
                "}" +
                "}";

        // Отправка сообщений

        Message msg = new MessageImpl(message);
        messageSender.sendMessage(topic, msg);


        // Закрытие producer
        producer.close();

        System.out.println("Дата старта    : " + startDate + "\nДата окончания : " + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    }
}