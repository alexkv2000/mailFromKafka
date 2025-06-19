package kvo.separat.kafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

public class DatabaseService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private final String dbUrl;

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
                "server TEXT DEFAULT NULL" +
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

    public void updateMessagesStatus(String topic, String server, int limitSelect) {
        String updateSQL = "UPDATE messages SET status = 'select', server = ? WHERE id in (SELECT id FROM messages WHERE status IS NULL AND kafka_topic = ? AND server = ? LIMIT ?)";

        try (Connection connection = getConnection();
             PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {

            updateStatement.setString(1, server);
            updateStatement.setString(2, topic);
            updateStatement.setString(3, server);
            updateStatement.setInt(4, limitSelect);
            updateStatement.executeUpdate();

        } catch (SQLException e) {
            logger.error("Error updating messages status in database", e);
        }
    }

    public void updateMessageStatusDate(int messageId, String status, Timestamp timestamp) throws SQLException {
        String updateSQL = "UPDATE messages SET status = ?, date_end = ? WHERE id = ?";

        try (Connection connection = getConnection();
             PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {

            updateStatement.setString(1, status);
            updateStatement.setTimestamp(2, timestamp);
            updateStatement.setInt(3, messageId);
            updateStatement.executeUpdate();
            logger.info("Database UPDATE Statue and Date_END");
        } catch (SQLException e) {
            logger.error("Error updating message status in database", e);
            //          throw e;
        }
    }
    MessageData convertResultSetToMessageData(ResultSet resultSet) throws SQLException {
        String message = resultSet.getString("message");
        JSONObject jsonMessage = new JSONObject(message);

        String to = jsonMessage.optString("To", "");
        String toCC = jsonMessage.optString("ToСС", "");
        String caption = jsonMessage.optString("Caption", "Информация от сужбы DocsVision");
        String body = jsonMessage.optString("Body", "");
        UUID uuid = UUID.fromString(jsonMessage.optString("uuid", ""));

        JSONArray urls = new JSONArray();
        if (jsonMessage.has("Url") && !jsonMessage.isNull("Url")) {
            JSONArray urls_ = jsonMessage.getJSONArray("Url");
            if (urls_.length() > 0) {
                urls = urls_;
            }
        }
        return new MessageData(resultSet.getInt("id"), to, toCC, caption, body, urls, uuid);
    }
    public List<MessageData> selectMessages(String topic, String server, int limitSelect) {
        String selectSQL = "SELECT * FROM messages WHERE status = 'select' AND date_end is null AND kafka_topic = ? AND server = ? LIMIT ?";

        List<MessageData> aListMessage = new ArrayList<>();

        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(selectSQL)) {

            preparedStatement.setString(1, topic);
            preparedStatement.setString(2, server);
            preparedStatement.setInt(3, limitSelect);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    aListMessage.add(convertResultSetToMessageData(resultSet));
                }
                return aListMessage;
            }
        } catch (SQLException e) {
            logger.error("Error processing messages from database", e);
        }
        return null;
    }
}
