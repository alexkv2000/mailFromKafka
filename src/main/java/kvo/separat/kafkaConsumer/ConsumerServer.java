package kvo.separat.kafkaConsumer;

import kvo.separat.mssql.MSSQLConnection;
import kvo.separat.kafkaConsumer.ConfigLoader;
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
    private static ExecutorService executor;
    private final String typeMes;
    private final String file_Path;
    private static String htread_sleep;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final String URL_MSSQL;
    private static String USER_MSSQL;
    private static String PASSWORD_MSSQL;

    public ConsumerServer(KafkaConsumerWrapper kafkaConsumer, DatabaseService databaseService, EmailService emailService, ConfigLoader configLoader) {
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
        htread_sleep = configLoader.getProperty("THREAD_SLEEP");
        this.URL_MSSQL = configLoader.getProperty("URL_MSSQL");
        this.USER_MSSQL = configLoader.getProperty("USER_MSSQL");
        this.PASSWORD_MSSQL = configLoader.getProperty("PASSWORD_MSSQL");
    }

    public void startProcessing() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); //запускаем 2 потока

        scheduler.scheduleWithFixedDelay(() -> {
            try {
                this.processMessages();
            } catch (Exception e) {
                logger.error("Error in processMessages", e);
            }
        }, 0, 5, TimeUnit.SECONDS);

        scheduler.scheduleWithFixedDelay(() -> {
            try {
                this.setKafkaConsumer();
            } catch (Exception e) {
                logger.error("Error in setKafkaConsumer", e);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }
    private void setKafkaConsumer(){
        logger.info("Start setKafkaConsumer : getConsumerRecords() =>");
        List<ConsumerRecord<String, String>> recordList = getConsumerRecords();
        logger.info("Start setKafkaConsumer (проверка структуры JSON) : AddCorrectDataJSONFromBrokerToDBSQL() => ");
        AddCorrectDataJSONFromBrokerToDBSQL(recordList); //проверка структуры JSON
    }
    private void processMessages() {
        try {
//            logger.info("Start processMessages : getConsumerRecords() =>");
//            List<ConsumerRecord<String, String>> recordList = getConsumerRecords();
//            logger.info("Start processMessages (проверка структуры JSON) : AddCorrectDataJSONFromBrokerToDBSQL() => ");
//            AddCorrectDataJSONFromBrokerToDBSQL(recordList); //проверка структуры JSON
            logger.info("Start processMessages (забронировали данные для отбработки) : updateMessagesForProcessing() => ");
            databaseService.updateMessagesForProcessing(topic, server,"select", typeMes, limitSelect); //забронировали данные для отбработки
            logger.info("Start processMessages (берем забронированные данные в работу и данные с ошибкой кол-во попыток < NUM_ATTEMPT) : selectMessages() => ");
            List<MessageData> resultSet = databaseService.selectMessages(topic, server, typeMes, limitSelect); //берем забронированные данные в работу и данные с ошибкой кол-во попыток < NUM_ATTEMPT

            if (resultSet == null || resultSet.isEmpty()) {
                logger.debug("Нет сообщений для обработки");
                return;
            }

            List<Future<?>> futures = new ArrayList<>();
            logger.info("Start processMessages (проверка записей resultSet) : for (MessageData result : resultSet) => ");
            for (MessageData result : resultSet) {
                logger.info("Start processMessages (проверка записей resultSet) : isValidMessage() => ");
                if (!isValidMessage(result)) {
                    logger.error("Некорректные данные сообщения, пропускаем ID: {}",
                            result != null ? result.getId() : "null");
                    logger.info("Вызов processMessages.updateMessageStatusDate => ");
                    databaseService.updateMessageStatusDate(topic, server, result.getId(), "error DATA JSON", new Timestamp(System.currentTimeMillis()));
                    continue;
                }
                logger.info("Start processMessages (подготовка / отправка сообщения) : processSingleMessageAsync() => ");
                futures.add(processSingleMessageAsync(result));
            }
            logger.info("Start processMessages (ожидение отправки всех сообщений) : waitForFuturesCompletion() => ");
            waitForFuturesCompletion(futures);
        } catch (Exception e) {
            logger.error("Критическая ошибка в процессе обработки", e); //не нужен sleep, scheduleWithFixedDelay сам управляет интервалами
        }
    }

    private boolean isValidMessage(MessageData message) {
        return message != null && message.getUuid() != null &&
                (message.getTo() != null || message.getToCC() != null);
    }

    private Future<?> processSingleMessageAsync(MessageData result) {
        return executor.submit(() -> {
            try {
                // TODO Временная модификация (удалить в продакшене)
//                result.setCaption(result.getId() + " " + result.getCaption());

                // Основная логика обработки

                StringBuilder sPath = MSSQLConnection.DownloadBinaryDV(result.getUuid(), URL_MSSQL, USER_MSSQL , PASSWORD_MSSQL, file_Path);
                emailService.sendMail(result.getTo(), result.getToCC(), result.getBCC(), result.getCaption(), result.getBody(), String.valueOf(sPath));

                // Очистка временных файлов
                if (Files.exists(Path.of(file_Path + result.getUuid()))) {
                    MSSQLConnection.deleteDirectory(result.getUuid(), file_Path);
                }

                // Обновление статуса
                logger.info("Вызов processSingleMessageAsync.updateMessageStatusDate => ");
                databaseService.updateMessageStatusDate(topic, server, result.getId(),
                        "send", new Timestamp(System.currentTimeMillis()));
            } catch (SQLException e) {
                handleMessageProcessingError(result, e);
            } catch (IOException e) {
                throw new RuntimeException("IO ошибка при обработке сообщения", e);
            }
        });
    }

    private void handleMessageProcessingError(MessageData result, SQLException e) {
        try {
            logger.info("Вызов handleMessageProcessingError.updateMessageStatusDate");
            databaseService.updateMessageStatusDate(topic, server, result.getId(),
                    "error", new Timestamp(System.currentTimeMillis()));
        } catch (SQLException ex) {
            logger.error("Ошибка при обновлении статуса 'ERROR' сообщения ID: {}", result.getId(), ex);
        }
        logger.error("Ошибка при обработке сообщения ID: {}", result.getId(), e);
    }

    private void waitForFuturesCompletion(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Обработка прервана во время ожидания задач");
                break;
            } catch (ExecutionException e) {
                logger.error("Ошибка выполнения задачи", e.getCause());
            }
        }
    }

    public void stopProcessing() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }


    public void start() {

//        try {
//            databaseService.createTableIfNotExist();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
        List<ConsumerRecord<String, String>> recordList;
        List<MessageData> resultSet;

        while (true) {
            try {
                recordList = getConsumerRecords();

                AddCorrectDataJSONFromBrokerToDBSQL(recordList);

                databaseService.updateMessagesForProcessing(topic, server, "select", typeMes);

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
//                                result.setCaption(result.getId() + " " + result.getCaption()); //TODO удалить строку перед внедрением на ПРОД
                                StringBuilder sPath = MSSQLConnection.DownloadBinaryDV(result.getUuid(), URL_MSSQL, USER_MSSQL, PASSWORD_MSSQL, file_Path);

                                emailService.sendMail(result.getTo(), result.getToCC(), result.getBCC(), result.getCaption(),
                                        result.getBody(), String.valueOf(sPath));
                                if (Files.exists(Path.of(file_Path + result.getUuid()))) {
                                    MSSQLConnection.deleteDirectory(result.getUuid(), file_Path);
                                }
                                logger.info("Вызов start.executor.updateMessageStatusDate");
                                databaseService.updateMessageStatusDate(topic, server, result.getId(),
                                        "send", new Timestamp(System.currentTimeMillis()));
                            } catch (SQLException e) {
                                try {
                                    logger.info("Вызов start.executor.catch.updateMessageStatusDate");
                                    databaseService.updateMessageStatusDate(topic, server, result.getId(),
                                            "error", new Timestamp(System.currentTimeMillis()));
                                } catch (SQLException ex) {
                                    logger.error("Ошибка при обновлении статуса 'ERROR' сообщения ID: " + result.getId(), ex);
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
                    Thread.sleep(Long.parseLong(htread_sleep));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Поток был прерван", ie);
                    break;
                }
            }
        }
    }

//    private void updateStatusDBSQL(String status) {
//        databaseService.updateMessagesForProcessing(topic, server, status, typeMes);
//    }
    // Проверка корректности JSON перед вставкой в БД
    private void AddCorrectDataJSONFromBrokerToDBSQL(List<ConsumerRecord<String, String>> recordList) {

        for (ConsumerRecord<String, String> record : recordList) {
            try {
                // Проверяем валидность JSON
                new JSONObject(record.value());
                databaseService.insertMessages(Collections.singletonList(record), typeMes); //параметр 'server'="" (не передается)
            } catch (JSONException e) {
                logger.error("Некорректный JSON в сообщении, пропускаем: " + record.value());
            }
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

    public static void main(String[] args) throws IOException {
        String currentDir = System.getProperty("user.dir");
        String configPath = currentDir + "\\config\\setting.txt";

        //Проверка параметров запуска
        boolean useWhileLoop = false;
        for (String arg : args) {
            if (arg.startsWith("config.path=")) {
                configPath = arg.substring("config.path=".length());
                continue;
            }
            if (arg.startsWith("while=")) {
                 useWhileLoop = arg.substring("while=".length()).equals("true");
            }
        }
        logger.info("Запуск в режиме: {}", useWhileLoop ? "While-Loop" : "Scheduled");
        //

        ConfigLoader configLoader = new ConfigLoader(configPath);
        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapper(configLoader);
        DatabaseService databaseService = new DatabaseService(configLoader);
        EmailService emailService = new EmailService(configLoader);
//        MSSQLConnection mssqlConnection = new MSSQLConnection(configLoader);
        ConsumerServer consumerServer = new ConsumerServer(kafkaConsumer, databaseService, emailService, configLoader);

        //Создать таблицу если не существует
//        try {
//            databaseService.createTableIfNotExist();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
        //
        // Регистрация обработчика Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Получен сигнал завершения (Ctrl+C)");
            consumerServer.stopProcessing();
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("Приложение корректно завершено");
        }));
        //
        // Запуск соответствующего режима
        if (useWhileLoop) {
            consumerServer.start(); // Бесконечный цикл while
        } else {
            consumerServer.startProcessing(); // Стандартный режим
        }
        //
        // Ожидание завершения (для while-loop режима)
        if (useWhileLoop) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(Long.parseLong(htread_sleep));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("Поток прерван, завершение работы");
                }
            }
        }
        //
    }
}