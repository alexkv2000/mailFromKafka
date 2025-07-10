package kvo.separat.kafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static kvo.separat.ConsumerServerDV.configLoader;

public class DatabaseService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private final String dbUrl;
    private static final int NUM_ATTEMPT = Integer.parseInt(configLoader.getProperty("NUM_ATTEMPT"));

    public DatabaseService(ConfigLoader configLoader) {
        this.dbUrl = "jdbc:sqlite:" + configLoader.getProperty("DB_PATH");
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl);
    }

    public void createTableIfNotExist() throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS messages (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "kafka_topic TEXT NOT NULL," +
                "message TEXT NOT NULL," +
                "date_create TIMESTAMP NOT NULL," +
                "status TEXT DEFAULT NULL," +
                "date_end TIMESTAMP DEFAULT NULL," +
                "server TEXT DEFAULT NULL," +
                "NUM_ATTEMPT INTEGER DEFAULT 0" +
                ");";

        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(createTableSQL)) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error creating table", e);
            throw e;
        }
    }

    public void insertMessages(List<ConsumerRecord<String, String>> records, String topic, String server) {
        String insertSQL = "INSERT INTO messages (kafka_topic, message, date_create, server) VALUES (?, ?, ?, ?)";

        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

            for (ConsumerRecord<String, String> record : records) {
                preparedStatement.setString(1, record.topic());
                preparedStatement.setString(2, record.value());
                preparedStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                preparedStatement.setString(4, server);
                preparedStatement.executeUpdate();
            }

        } catch (SQLException e) {
            logger.error("Error inserting messages into database", e);
        }
    }

    public void updateMessagesStatus(String topic, String server, String status, int limitSelect) {
        String updateSQL = "UPDATE messages SET status = ?, server = ? WHERE id in (SELECT id FROM messages WHERE status IS NULL AND kafka_topic = ? AND server = ? LIMIT ?)";

        try (Connection connection = getConnection();
             PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {

            updateStatement.setString(1, status);
            updateStatement.setString(2, server);
            updateStatement.setString(3, topic);
            updateStatement.setString(4, server);
            updateStatement.setInt(5, limitSelect);
            updateStatement.executeUpdate();

        } catch (SQLException e) {
            logger.error("Error updating messages status in database", e);
        }
    }

    public void updateMessageStatusDate(String topic, String server, int messageId, String status, Timestamp timestamp) throws SQLException {
        int numAttempt = getIncNumAttempt(messageId);

        String updateSQL = "UPDATE messages SET status = ?, date_end = ?, NUM_ATTEMPT = ? WHERE id = ? AND kafka_topic = ? AND server = ?";
        try (Connection connection = getConnection();
             PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
            updateStatement.setString(1, status);
            updateStatement.setTimestamp(2, timestamp);
            updateStatement.setInt(3, ++numAttempt);
            updateStatement.setInt(4, messageId);
            updateStatement.setString(5, topic);
            updateStatement.setString(6, server);
            updateStatement.executeUpdate();
            logger.info("Database UPDATE Statue and Date_END");
        } catch (SQLException e) {
            logger.error("Error updating message status in database", e);
            //          throw e;
        }
    }

    private int getIncNumAttempt(int messageId) throws SQLException {
        String selSQL = "SELECT DISTINCT NUM_ATTEMPT FROM messages WHERE id = ?";
        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(selSQL)) {
            preparedStatement.setInt(1, messageId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) { // Проверка наличия результата
                    return resultSet.getInt(1);
                } else {
                    logger.warn("No records found for message ID: " + messageId);
                }
            } catch (SQLException e) {
                logger.error("Error select NUM_ATTEMPT on ID message ", e);
                //          throw e;
            }
        }
        return 0;
    }

    MessageData convertResultSetToMessageData(ResultSet resultSet) throws SQLException {
        String fileName;
        String message = resultSet.getString("message");
        JSONObject jsonMessage = new JSONObject(message);

        String to = jsonMessage.optString("To", "");
        String toCC = jsonMessage.optString("ToСС", "");
        String caption = jsonMessage.optString("Caption", "Информация от сужбы DocsVision");
        String body = jsonMessage.optString("Body", "");
        UUID uuid = UUID.fromString(jsonMessage.optString("uuid", ""));

        JSONObject urlFiles = new JSONObject(); // Создаем объект для хранения файлов

        if (jsonMessage.has("Url") && !jsonMessage.isNull("Url")) {
            JSONObject urlsObj = jsonMessage.getJSONObject("Url");

            // Получаем все ключи (имена файлов) из объекта Url
            Iterator<String> keys = urlsObj.keys();

            while (keys.hasNext()) {
                fileName = keys.next();
                String fileContent = urlsObj.getString(fileName);
                urlFiles.put(fileName, fileContent);
            }
        }

        return new MessageData(resultSet.getInt("id"), to, toCC, caption, body, urlFiles, uuid);
    }

    public List<MessageData> selectMessages(String topic, String server, int limitSelect) {
        String selectSQL = "SELECT * FROM messages WHERE (status = 'select' AND date_end is null AND kafka_topic = ? AND server = ?) " +
                "OR (status = 'error' AND NUM_ATTEMPT < ? AND kafka_topic = ? AND server = ?) LIMIT ?";
        // Добавены сообщения с Error кол-во цикла не превышает NUM_ATTEMPT
        List<MessageData> aListMessage = new ArrayList<>();

        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(selectSQL)) {

            preparedStatement.setString(1, topic);
            preparedStatement.setString(2, server);
            preparedStatement.setInt(3, NUM_ATTEMPT);
            preparedStatement.setString(4, topic);
            preparedStatement.setString(5, server);
            preparedStatement.setInt(6, limitSelect);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    aListMessage.add(convertResultSetToMessageData(resultSet)); //-> если кто-то добавит текст (НЕ JSON) просто закоментировать эту строку, потом вернуть.
                }
                return aListMessage;
            }
        } catch (SQLException e) {
            logger.error("Error processing messages from database", e);
        }
        return null;
    }
}
