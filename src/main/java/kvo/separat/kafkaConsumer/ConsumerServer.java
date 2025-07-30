package kvo.separat.kafkaConsumer;

import kvo.separat.mssql.MSSQLConnectionExample;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class ConsumerServer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);
    private final KafkaConsumerWrapper kafkaConsumer;
    private final DatabaseService databaseService;
    private final EmailService emailService;
    //    private  final SoapDownloadBinaryDV downloadFilesFromJSON;
    private final String topic;
    private final String server;
    private final int limitSelect;
    private final ExecutorService executor;
    private final String typeMes;
    private final MSSQLConnectionExample mssqlConnectionExample;
    private final String file_Path;

    public ConsumerServer(KafkaConsumerWrapper kafkaConsumer, DatabaseService databaseService, EmailService emailService, MSSQLConnectionExample mssqlConnectionExample, ConfigLoader configLoader) {
        this.kafkaConsumer = kafkaConsumer;
        this.databaseService = databaseService;
        this.emailService = emailService;
//        this.downloadFilesFromJSON = downloadFilesFromJSON;
        this.topic = configLoader.getProperty("TOPIC");
        this.server = configLoader.getProperty("SERVER");
        this.limitSelect = Integer.parseInt(configLoader.getProperty("LIMIT_SELECT"));
        this.typeMes = configLoader.getProperty("TYPE_MES");
        this.file_Path = configLoader.getProperty("FILE_PATH");
        this.executor = Executors.newFixedThreadPool(Integer.parseInt(configLoader.getProperty("NUM_THREADS")));
        this.mssqlConnectionExample = mssqlConnectionExample;
    }

    public void start() {
        try {
            databaseService.createTableIfNotExist();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        List<ConsumerRecord<String, String>> recordList;
        List<MessageData> resultSet;

        while (true) {
            try {
                recordList = getConsumerRecords();

                AddCorrectDataJSONFromBrokerToDBSQL(recordList);

                updateStatusDBSQL("select");

                resultSet = databaseService.selectMessages(topic, server, typeMes, limitSelect);

                if (resultSet == null || resultSet.isEmpty()) {
                    logger.debug("Нет сообщений для обработки");
                    continue;
                }

                List<Future<?>> futures = new ArrayList<>();

                for (MessageData result : resultSet) {
                    try {
                        // Проверка данных сообщения перед обработкой
                        if (result == null || result.getUuid() == null || (result.getTo() == null && result.getToCC() == null)) {
                        // if (result == null || result.getUuid() == null || result.getTo() == null) {
                            logger.error("Некорректные данные сообщения, пропускаем ID: " +
                                    (result != null ? result.getId() : "null"));
                            continue;
                        }

                        Future<?> future = executor.submit(() -> {
                            try {
                                result.setCaption(result.getId() + " " + result.getCaption());
                                StringBuilder sPath = MSSQLConnectionExample.DownloadBinaryDV(result.getUuid());

                                // Дополнительная проверка перед отправкой
//                                 if (sPath == null || sPath.length() == 0) {
//                                    throw new IOException("Не удалось получить файл по UUID: " + result.getUuid());
//                                }

                                emailService.sendMail(result.getTo(), result.getToCC(), result.getCaption(),
                                        result.getBody(), String.valueOf(sPath));
                                if (Files.exists(Path.of(file_Path + result.getUuid()))) {
                                    MSSQLConnectionExample.deleteDirectory(result.getUuid());
                                }
                                databaseService.updateMessageStatusDate(topic, server, result.getId(),
                                        "send", new Timestamp(System.currentTimeMillis()));
                            } catch (SQLException e) {
                                try {
                                    databaseService.updateMessageStatusDate(topic, server, result.getId(),
                                            "error", new Timestamp(System.currentTimeMillis()));
                                } catch (SQLException ex) {
                                    logger.error("Ошибка при обновлении статуса сообщения ID: " + result.getId(), ex);
                                }
                                logger.error("Ошибка при обработке сообщения ID: " + result.getId(), e);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
//                            catch (IOException e) {
//                                logger.error("IO ошибка при обработке сообщения ID: " + result.getId(), e);
//                            }
                        });
                        futures.add(future);
                    } catch (Exception e) {
                        logger.error("Ошибка при создании задачи для сообщения, пропускаем запись", e);
                    }
                }

                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        logger.error("Ошибка при выполнении задачи", e);
                    }
                }

            } catch (Exception e) {
                logger.error("Ошибка в основном цикле обработки", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Поток был прерван", ie);
                    break;
                }
            }
        }
    }

    private void updateStatusDBSQL(String status) {
        databaseService.updateMessagesForProcessing(topic, server, status, typeMes);
    }

    private void AddCorrectDataJSONFromBrokerToDBSQL(List<ConsumerRecord<String, String>> recordList) {
        // Проверка корректности JSON перед вставкой в БД
        for (ConsumerRecord<String, String> record : recordList) {
            try {
                // Проверяем валидность JSON
                new JSONObject(record.value());
            } catch (JSONException e) {
                logger.error("Некорректный JSON в сообщении, пропускаем: " + record.value());
                continue; // Пропускаем некорректные сообщения
            }
            databaseService.insertMessages(Collections.singletonList(record), server, typeMes);
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
        String currentDir = System.getProperty("user.dir");
        String configPath = currentDir + "\\config\\setting.txt";
        ConfigLoader configLoader = new ConfigLoader(configPath);

        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapper(configLoader);
        DatabaseService databaseService = new DatabaseService(configLoader);
        EmailService emailService = new EmailService(configLoader);
//        new SoapDownloadBinaryDV(configLoader);

        MSSQLConnectionExample mssqlConnectionExample = new MSSQLConnectionExample(configLoader);
        ConsumerServer consumerServer = new ConsumerServer(kafkaConsumer, databaseService, emailService, mssqlConnectionExample, configLoader);
        consumerServer.start();
    }
}