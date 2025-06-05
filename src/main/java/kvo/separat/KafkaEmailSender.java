package kvo.separat;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.mail.*;
import javax.mail.internet.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Properties;
import javax.net.ssl.*;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.utils.Utils.sleep;

public class KafkaEmailSender {
    private static final Logger logger = LoggerFactory.getLogger(KafkaEmailSender.class);
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

    public static void main(String[] args) {

        // Настройки для подключения к Kafka Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        logger.info("Starting Kafka source ...");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
            while (true) {
//                Date now = new Date();
//                SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
//                String formattedDate = sdf.format(now);
//                logger.info(formattedDate);

                var records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info(records.toString());
                    //System.out.println(records.toString());
                    executor.submit(() -> {
                        try {
                            processMessage(record.value());
                        } catch (IOException e) {
                            logger.error("An error 'processMessage' ", e);
                            throw new RuntimeException(e);
                        }
                    });
                }
                try {
                    sleep(1000); // Задержка на 1 секунду
                } catch (Exception e) {
                    logger.error("An error 'main' stopping wait", e);
                    System.err.println("Ошибка при задержке: " + e.getMessage());
                }
            }
        }
    }

    static void processMessage(String message) throws IOException {
        JSONObject jsonMessage = new JSONObject(message);
        String to = jsonMessage.optString("To", "");
        String toCC = jsonMessage.optString("ToСС", "");
        String caption = jsonMessage.optString("Caption", "Информация от сужбы DocsVision");
        String body = jsonMessage.optString("Body", "");
        // Получение массива URLS
        JSONArray urls = jsonMessage.getJSONArray("Url");

        // Создать директорию (из IDDoc)
        UUID uuid = UUID.randomUUID();
        Files.createDirectories(Path.of((FILE_PATH + uuid)));

        // Проход по массиву URLS и загрузка файлов
        StringBuilder filePaths = new StringBuilder();
        logger.info("Starting download Files ...");
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

        logger.info("Starting send Email ...");
        // Отправка сообщения по электронной почте
        sendEmail(to, toCC, caption, body, filePaths.toString());
        logger.info("Stop send Email ...");

        logger.info("Starting delete Directory Temp...");
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
                        // Копирование файла
                        Files.copy(in, Paths.get(fullPath), StandardCopyOption.REPLACE_EXISTING);
                        logger.info("File downloaded access"+ fullPath);
                        //System.out.println("Файл успешно скачан: " + fullPath);
                        return fullPath;
                    }
                } else {
                    logger.error("An error 'downloadFile' " + httpConn.getResponseCode());
                    //System.out.println("Ошибка ответа от сервера закачки файлов: " + httpConn.getResponseCode());
                }
                // Закрытие соединения
                httpConn.disconnect();
            } catch (IOException e) {
                logger.error("An error 'downloadFile' ", e);
                e.printStackTrace();
            }

            if (attempt < attempts) {
                try {
                    Thread.sleep(1000); // Задержка на 1 секунду
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

        // Создание SSLContext и SSLSocketFactory
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, null, new java.security.SecureRandom());
            SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            // Установка SSLSocketFactory в свойства
            props.put("mail.smtp.ssl.socketFactory", sslSocketFactory);

        } catch (Exception e) {
            logger.error("An error 'sendEmail' create SSLContext ", e);
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
            Integer num_attempts = NUM_ATTEMPT;
            while (num_attempts != 0) {
                try {
                    Transport.send(message);
                    logger.info("Email sent successfully");
                    //System.out.printf("Email sent successfully to %s, copy %s ", to, toCC);
                    break;
                } catch (Exception ee) {
                    ee.printStackTrace();
                    logger.error("An error 'sendEmail' To or ToCC", ee);
                    //System.out.printf("Email sent error to %s, copy %s ", to, toCC);
                }
                // Задержка на 1 секунду (1000 миллисекунд)
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("An error 'downloadFile' stopping wait", e);
                    e.printStackTrace();
                }
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
        // Используем Files.walkFileTree для рекурсивного удаления
        Files.walk(path).sorted((a, b) -> b.compareTo(a)) // Сортируем в обратном порядке для удаления файлов перед каталогами
                .forEach(p -> {
                    try {
                        Files.delete(p);
                        logger.info("File delete access"+ p);
                    } catch (IOException e) {
                        logger.error("An error 'deleteDirectory' ", e);
                        //System.err.println("Ошибка при удалении: " + p + " - " + e.getMessage());
                    }
                });
    }

}
