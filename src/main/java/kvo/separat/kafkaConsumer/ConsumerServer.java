package kvo.separat.kafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConsumerServer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);
    private final KafkaConsumerWrapper kafkaConsumer;
    private final DatabaseService databaseService;
    private final EmailService emailService;
    private final FileService fileService;
    private final String topic;
    private final String server;
    private final int limitSelect;
    private final ExecutorService executor;

    public ConsumerServer(KafkaConsumerWrapper kafkaConsumer, DatabaseService databaseService, EmailService emailService, FileService fileService, ConfigLoader configLoader) {
        this.kafkaConsumer = kafkaConsumer;
        this.databaseService = databaseService;
        this.emailService = emailService;
        this.fileService = fileService;
        this.topic = configLoader.getProperty("TOPIC");
        this.server = configLoader.getProperty("SERVER");
        this.limitSelect = Integer.parseInt(configLoader.getProperty("LIMIT_SELECT"));
        this.executor = Executors.newFixedThreadPool(Integer.parseInt(configLoader.getProperty("NUM_THREADS")));
    }

    public void start() throws SQLException {
        databaseService.createTableIfNotExist();
        List<ConsumerRecord<String, String>> recordList;
        List<MessageData> resultSet;

        while (true) {
            recordList = getConsumerRecords();
            databaseService.insertMessages(recordList, topic, server);

            databaseService.updateMessagesStatus(topic, server, "select", limitSelect);
            resultSet = databaseService.selectMessages(topic, server, limitSelect);
            List<Future<?>> futures = new ArrayList<>();
            for (MessageData result : resultSet) {
                Future<?> future = executor.submit(() -> {
                    try {
                        result.setCaption(result.getId() + " " + result.getCaption());
                        emailService.sendMessage(result, fileService); // --> Отправить сообщение
                        databaseService.updateMessageStatusDate(topic, server, result.getId(), "send", new Timestamp(System.currentTimeMillis())); // --> Обновление статуса и времени отправки
                    } catch (IOException | SQLException e) {
                        try {
                            databaseService.updateMessageStatusDate(topic, server, result.getId(), "error", new Timestamp(System.currentTimeMillis())); // --> добавил подсчет попыток NUM_ATTEMPT
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }
            for (Future<?> future : futures) {
                try {
                    future.get(); // Блокируем до завершения задачи
//TODO Удалить записи успешно отправленные и попытки которые превысили NUM_ATTEMPT
                } catch (Exception e) {
                    logger.info("Ошибка закачки данных по url");
                    e.printStackTrace();
                }
            }
//                CompletableFuture.supplyAsync(() -> {
//                            logger.info("Thread: " + messageId + " поток : " + Thread.currentThread().getName());
//                            try {
//                                logger.info("Message sent for ID: " + messageId + " поток : " + Thread.currentThread().getName());
//                                emailService.sendMessage(finalResultSet, fileService); // --> Отправить сообщение
//                                return messageId; // Возвращаем messageId для следующего этапа
//                            } catch (IOException e) {
//                                logger.error("Error sending message ID: " + messageId, e);
//                                throw new RuntimeException(e);
//                            }
//                        }, executor)
//                        .thenAccept(id -> {
//                            try {
//                                // Обновляем статус сообщения после успешной отправки
//                                databaseService.updateMessageStatusDate(id, "send", new Timestamp(System.currentTimeMillis()));
//                                logger.info("Updated message status to 'send' for ID: " + id);
//                            } catch (SQLException e) {
//                                logger.error("Error updating message status to 'send' for ID: " + id, e);
//                                throw new RuntimeException(e);
//                            }
//                        })
//                        .exceptionally(e -> {
//                            // Обработаем исключение и обновим статус на 'error'
//                            try {
//                                databaseService.updateMessageStatusDate(messageId, "error", new Timestamp(System.currentTimeMillis()));
//                                logger.error("Updated message status to 'error' for ID: " + messageId + " due to error: ", e);
//                            } catch (SQLException ex) {
//                                logger.error("Error updating message status to 'error' for ID: " + messageId, ex);
//                            }
//                            return null; // Возвращаем null для завершения exceptionally
//                        });
        }
    }

    private List<ConsumerRecord<String, String>> getConsumerRecords() {
        List<ConsumerRecord<String, String>> recordList;
        Iterable<ConsumerRecord<String, String>> records;
        records = kafkaConsumer.pollRecords();
        recordList = StreamSupport.stream(records.spliterator(), false)
                .collect(Collectors.toList());
        return recordList;
    }

    public static void main(String[] args) throws SQLException, IOException {
        ConfigLoader configLoader = new ConfigLoader("src/main/setting.txt");

        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapper(configLoader);
        DatabaseService databaseService = new DatabaseService(configLoader);
        EmailService emailService = new EmailService(configLoader);
        FileService fileService = new FileService(configLoader);

        ConsumerServer consumerServer = new ConsumerServer(kafkaConsumer, databaseService, emailService, fileService, configLoader);
        consumerServer.start();
    }
}