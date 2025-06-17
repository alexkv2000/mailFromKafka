package kvo.separat;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;

public class ConsumerServerDV {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerServerDV.class);
    static ConfigLoader configLoader;

    static {
        try {
            configLoader = new ConfigLoader("src/main/setting.txt");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String TOPIC = configLoader.getProperty("TOPIC");
    private static final String BROKER = configLoader.getProperty("BROKER");
    private static final String GROUP_ID = configLoader.getProperty("GROUP_ID");
    private static final String EMAIL = configLoader.getProperty("EMAIL");
    private static final String PASSWORD = configLoader.getProperty("PASSWORD");
    private static final String SMTP_SERVER = configLoader.getProperty("SMTP_SERVER");
    private static final String FILE_PATH = configLoader.getProperty("FILE_PATH");
    private static final int NUM_THREADS = Integer.parseInt(configLoader.getProperty("NUM_THREADS"));
    private static final int NUM_ATTEMPT = Integer.parseInt(configLoader.getProperty("NUM_ATTEMPT"));
    private static final String LIMIT_SELECT = configLoader.getProperty("LIMIT_SELECT");
    private static final String SERVER = configLoader.getProperty("SERVER");

    public static void main(String[] args) throws SQLException {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        logger.info("Start Kafka source ...");

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:D:/DataBase/sql_kafka.s3db")) {
            createTableIsNotExist_Massages(connection);

            String insertSQL = "INSERT INTO messages (kafka_topic, message, date_create, server) VALUES (?, ?, ?, ?)";
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(TOPIC));
                ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
                while (true) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    // --> Добавление в БД новые сообщения
                    for (ConsumerRecord<String, String> record : records) {
                        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
                            preparedStatement.setString(1, record.topic());
                            preparedStatement.setString(2, record.value());
                            preparedStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                            preparedStatement.setString(4, SERVER);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            logger.error("Error processing Insert DB", e);
                        }

                    }
                    String updateSQL = "UPDATE messages SET status = 'select', server = ? WHERE id in (SELECT id FROM messages WHERE status IS NULL AND kafka_topic = ? AND server = ? LIMIT ?)";
                    try (PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
                        updateStatement.setString(1, SERVER);
                        updateStatement.setString(2, TOPIC);
                        updateStatement.setString(3, SERVER);
                        updateStatement.setString(4, LIMIT_SELECT);
                        updateStatement.executeUpdate();
                    }
                    // --> Отправка сообщений (по 200 штук)
                    String selectSQL = "SELECT * FROM messages WHERE status = 'select' AND kafka_topic = ? AND server = ? LIMIT ?";
                    try (PreparedStatement preparedStatement = connection.prepareStatement(selectSQL)) {
                        preparedStatement.setString(1, TOPIC);
                        preparedStatement.setString(2, SERVER);
                        preparedStatement.setString(3, LIMIT_SELECT);
                        ResultSet resultSet = preparedStatement.executeQuery();
                        while (resultSet.next()) {
                            String msg = resultSet.getString("message");
                            executor.submit(() -> {
                                try {
                                    sendMessage(msg); // --> Отправить сообщение
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                            ChangeStatusMess(connection, resultSet); // --> Обновление статуса и времени отправки
                            try {
                                sleep(1000);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void createTableIsNotExist_Massages(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS messages (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "kafka_topic TEXT NOT NULL," +
                "message TEXT NOT NULL," +
                "date_create TIMESTAMP NOT NULL," +
                "status TEXT DEFAULT NULL," +
                "date_end TIMESTAMP DEFAULT NULL," +
                "server TEXT DEFAULT NULL" +
                ");";
        try (PreparedStatement preparedStatement = connection.prepareStatement(createTableSQL)) {
            preparedStatement.executeUpdate();
        }
    }

    private static void ChangeStatusMess(Connection connection, ResultSet resultSet) throws SQLException {
        String updateSQL = "UPDATE messages SET status = ?, date_end = ? WHERE id = ?";
        try (PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
            updateStatement.setString(1, "send");
            updateStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            updateStatement.setInt(3, resultSet.getInt("id"));
            updateStatement.executeUpdate();
        }
    }

    private static void sendMessage(String message) throws IOException {
        JSONObject jsonMessage = new JSONObject(message);
        String to = jsonMessage.optString("To", "");
        String toCC = jsonMessage.optString("ToСС", "");
        String caption = jsonMessage.optString("Caption", "Информация от сужбы DocsVision");
        String body = jsonMessage.optString("Body", "");
        // Получение массива URLS
        JSONArray urls = new JSONArray();
        if (jsonMessage.has("Url") && !jsonMessage.isNull("Url")) {
            JSONArray urls_ = jsonMessage.getJSONArray("Url");
            if (urls_.length() > 0) {
                // Массив не пустой, можно работать с ним
                urls = urls_;
            }
        }

        // Создать директорию (из IDDoc)
        UUID uuid = UUID.randomUUID();
        Files.createDirectories(Path.of((FILE_PATH + uuid)));

        // Проход по массиву URLS и загрузка файлов
        StringBuilder filePaths = new StringBuilder();
        logger.info("Start download Files ...");
        for (int i = 0; i < urls.length(); i++) {
            String url = urls.getString(i);
            String filePath = downloadFile(url, uuid);

            if (!filePath.isEmpty() && !filePath.isBlank()) {
                if (filePaths.length() > 0) {
                    filePaths.append(", "); // Добавление запятой для разделения файлов
                }
                filePaths.append(filePath);
            }
        }
        logger.info("Stop download Files ...");

        logger.info("Start send Email ...");
        // Отправка сообщения по электронной почте
        sendEmail(to, toCC, caption, body, filePaths.toString());
        logger.info("Stop send Email ...");

        logger.info("Start delete Directory Temp...");
        // Удаление файлов из tmp
        if (filePaths.length() > 0) {
            deleteDirectory(Path.of(FILE_PATH + uuid));
            logger.info("Directory deleted access ..." + FILE_PATH + uuid);
            //System.out.println("Каталог успешно удалён: " + FILE_PATH + uuid);
        }
        logger.info("Stop delete Directory Temp ...");
    }

    static String downloadFile(String fileUrl, UUID uuid) {
        String fileName = ""; // Имя файла
        String fullPath = "";
        int attempts = NUM_ATTEMPT;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                // Создание URL
                URL url = new URL(fileUrl);
                HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
                httpConn.setRequestMethod("GET");
                httpConn.setRequestProperty("User-Agent", "Mozilla/5.0");
                httpConn.setRequestProperty("Accept", "application/pdf, application/msword, application/vnd.ms-excel");
                // Проверка кода ответа
                if (httpConn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    String path = url.getFile();
                    // Извлечение имени файла
                    fileName = path.substring(path.lastIndexOf('/') + 1);
                    fullPath = FILE_PATH + uuid.toString() + "\\" + fileName;
                    // Получение InputStream из URL
                    try (InputStream in = httpConn.getInputStream()) {
                        Files.copy(in, Paths.get(fullPath), StandardCopyOption.REPLACE_EXISTING);
                        logger.info("File downloaded access" + fullPath);
                        return fullPath;
                    }
                } else {
                    logger.error("An error 'downloadFile' " + httpConn.getResponseCode());
                }
                // Закрытие соединения
                httpConn.disconnect();
            } catch (IOException e) {
                logger.error("An error 'downloadFile' ", e);
                e.printStackTrace();
            }

            if (attempt < attempts) {
                try {
                    sleep(1000); // Задержка на 1 секунду
                } catch (InterruptedException e) {
                    logger.error("An error 'downloadFile' stopping wait", e);
                    System.err.println("Ошибка при задержке: " + e.getMessage());
                }
            }
        }
        return fullPath;
    }

    static void sendEmail(String to, String toCC, String caption, String body, String filePaths) {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", SMTP_SERVER);
        props.put("mail.smtp.port", "587");
        props.put("mail.smtp.ssl.socketFactory", SSLSocketFactory.getDefault());

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        String formattedDate = dateFormat.format(new Date());

        // Создание SSLContext и SSLSocketFactory
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, null, new java.security.SecureRandom());
            SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            // Установка SSLSocketFactory в свойства
            props.put("mail.smtp.ssl.socketFactory " + formattedDate, sslSocketFactory);

        } catch (Exception e) {
            logger.error("An error 'sendEmail' create SSLContext " + formattedDate, e);
            e.printStackTrace();
        }
        // Получение сеанса
        Session session = setSession(Session.getInstance(props));

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(EMAIL));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(toCC));
            message.setSubject(caption);
            // Создание текстовой части сообщения
            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setText(body);
            // Объединение частей в одно сообщение
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(textPart);

            // Обработка вложений
            if (!filePaths.isEmpty()) {
                String[] paths = filePaths.split(", ");
                for (String path : paths) {
                    MimeBodyPart attachmentPart = new MimeBodyPart();
                    attachmentPart.attachFile(path.trim());
                    multipart.addBodyPart(attachmentPart);
                }
            }
            message.setContent(multipart);
            //Отправка сообщения
            int num_attempts = NUM_ATTEMPT;
            while (num_attempts != 0) {
                try {
                    Transport.send(message);
                    logger.info("Email sent successfully " + formattedDate);
                    //System.out.printf("Email sent successfully to %s, copy %s ", to, toCC);
                    break;
                } catch (Exception ee) {
                    ee.printStackTrace();

                    logger.error("An error 'sendEmail' To or ToCC " + formattedDate, ee);
                    //System.out.printf("Email sent error to %s, copy %s ", to, toCC);
                }
                // Задержка на 1 секунду (1000 миллисекунд)
//                try {
//                    sleep(1000);
//                } catch (InterruptedException e) {
//                    logger.error("An error 'downloadFile' stopping wait", e);
//                    e.printStackTrace();
//                }
                num_attempts--;
            }
        } catch (Exception e) {
            logger.error("An error 'sendEmail' Transport.send(message) ", e);
            e.printStackTrace();
        }
    }

    static Session setSession(Session props) {
        Session session = Session.getInstance(props.getProperties(), new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(EMAIL, PASSWORD);
            }
        });
        return session;
    }

    static void deleteDirectory(Path path) throws IOException {
        // Files.walkFileTree для рекурсивного удаления
        Files.walk(path).sorted((a, b) -> b.compareTo(a)) // Сортируем в обратном порядке перед каталогами
                .forEach(p -> {
                    try {
                        Files.delete(p);
                        logger.info("File delete access" + p);
                    } catch (IOException e) {
                        logger.error("An error 'deleteDirectory' ", e);
                    }
                });
    }
}