package kvo.separat.kafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static kvo.separat.ConsumerServerDV.configLoader;

public class DatabaseService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private static final int NUM_ATTEMPT = Integer.parseInt(configLoader.getProperty("NUM_ATTEMPT"));
    private final String dbUrl;
    private final String user;
    private final String password;

    public DatabaseService(ConfigLoader configLoader) {
        this.dbUrl = configLoader.getProperty("DB_PATH");
        this.user = configLoader.getProperty("USER_SQL");
        this.password = configLoader.getProperty("PASSWORD_SQL");
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl, user, password);
    }

    public void createTableIfNotExist() throws SQLException {
        String createTableSQL = "CREATE TABLE `messages` (" +
                "`id` int NOT NULL AUTO_INCREMENT, " +
                "`kafka_topic` varchar(255) NOT NULL, " +
                "`message` text NOT NULL, " +
                "`date_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                "`status` varchar(100) DEFAULT NULL, " +
                "`date_end` datetime DEFAULT NULL, " +
                "`server` varchar(100) DEFAULT NULL, " +
                "`NUM_ATTEMPT` int DEFAULT '0', " +
                "`typeMes` varchar(50) DEFAULT NULL, " +
                "PRIMARY KEY (`id`), " +
                "UNIQUE KEY `Id_UNIQUE` (`id`)) " +
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";

        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(createTableSQL)) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error creating table", e);
            throw e;
        }
    }

    public void insertMessages(List<ConsumerRecord<String, String>> records, String typeMessage) {
        insertMessages(records, "", typeMessage);
    }

    public void insertMessages(List<ConsumerRecord<String, String>> records, String server, String typeMessage) {
        String insertSQL = "INSERT INTO messages (kafka_topic, message, date_create, server, typeMes) VALUES (?, ?, ?, ?, ?)";
        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

            for (ConsumerRecord<String, String> record : records) {
                preparedStatement.setString(1, record.topic());
                preparedStatement.setString(2, record.value());
                preparedStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                preparedStatement.setString(4, server);
                preparedStatement.setString(5, typeMessage);
                preparedStatement.executeUpdate();
            }

        } catch (SQLException e) {
            logger.error("Error inserting messages into database", e);
        }
    }



    public void repeatSend(Integer messageId) {
        updateParametersMessage(messageId, 0);
    }

    public void updateParametersMessage(Integer messageId, Integer NUM_ATTEMPT){
        String updateSQL = "UPDATE messages m SET m.status = NULL, m.date_end = NULL, m.server = '', m.NUM_ATTEMPT = " + NUM_ATTEMPT.toString() + " WHERE m.id = " + messageId.toString() + ";";
        try (Connection connection = getConnection();
             PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
            updateStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error updating messages status in database", e);
        }
    }


    public void updateMessagesForProcessing(String topic, String server, String status, String typeMessage) {
        updateMessagesForProcessing(topic, server, status, typeMessage, 100);
    }

    public void updateMessagesForProcessing(String topic, String server, String status, String typeMessage, int limitSelect) {
        String[] types = typeMessage.split(",");
//        String placeholders = String.join(",", Collections.nCopies(types.length, "?"));
//        String updateSQL = "UPDATE messages SET status = ?, server = ? WHERE id in (SELECT id FROM messages WHERE status IS NULL AND kafka_topic = ? AND server = ? AND typeMes IN(" + placeholders + ") LIMIT ?)";
        String updateSQL = "UPDATE messages m1 JOIN (SELECT id FROM messages WHERE status IS NULL AND server = ? AND kafka_topic = ? AND typeMes IN (?) LIMIT ?) m2 ON m1.id = m2.id SET m1.status = ?, m1.server = ?;";
        try (Connection connection = getConnection();
             PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
            updateStatement.setString(1, "");
            updateStatement.setString(2, topic);

            for (int i = 0; i < types.length; i++) {
                updateStatement.setString(3 + i, types[i].trim());
            }
            updateStatement.setInt(3 + types.length, limitSelect);
            updateStatement.setString(4 + types.length, status);
            updateStatement.setString(5 + types.length, server);
            updateStatement.executeUpdate();

        } catch (SQLException e) {
            logger.error("Error updating messages status in database", e);
        }
    }

    public void updateMessageStatusDate(String topic, String server, Integer messageId, String status, Timestamp timestamp) throws SQLException {
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
        try {
//            String fileName;
            String message = resultSet.getString("message");
            JSONObject jsonMessage = new JSONObject(message);

            String to = jsonMessage.optString("To", "");
            String toCC = jsonMessage.optString("ToCC", "");
            String BCC = jsonMessage.optString("BCC", "");
            String caption = jsonMessage.optString("Caption", "Информация от службы DocsVision");
            String body = jsonMessage.optString("Body", "");
            UUID uuid = UUID.fromString(jsonMessage.optString("uuid", ""));

            return new MessageData(resultSet.getInt("id"), to, toCC, BCC, caption, body, uuid);
        } catch (Exception e) {
            System.err.println("Ошибка при конвертации (JSON) ResultSet в MessageData: " + e.getMessage());
            //updateMessageStatusDate(topic, server, resultSet.getInt("id"), "error", new Timestamp(System.currentTimeMillis()));//TODO обновлять ошибку в БД - проверить!!!
            throw new SQLException("Ошибка при конвертации (JSON) ResultSet в MessageData ", e);
        }
    }

    int getErrorMessage(ResultSet resultSet) throws SQLException {
        int id_ = 0;
        try {
//            String fileName;
            String message = resultSet.getString("message");
            JSONObject jsonMessage = new JSONObject(message);
            id_ = resultSet.getInt("id");
            String to = jsonMessage.optString("To", "");
            String toCC = jsonMessage.optString("ToCC", "");
//            String caption = jsonMessage.optString("Caption", "Информация от службы DocsVision");
            String body = jsonMessage.optString("Body", "");
            if (to.isEmpty() && toCC.isEmpty() || body.isEmpty()) {
                return id_;
            }
            UUID uuid = UUID.fromString(jsonMessage.optString("uuid", ""));

        } catch (Exception e) {
            System.err.println("Ошибка при конвертации (JSON) ResultSet в MessageData: " + e.getMessage());
            return id_;
            //throw new SQLException("Ошибка при конвертации (JSON) ResultSet в MessageData ", e);
        }
        return 0;
    }

    public List<MessageData> selectMessages(String topic, String server, String typeMessage, int limitSelect) {
        String[] types = typeMessage.split(",");
        String placeholders = String.join(",", Collections.nCopies(types.length, "?"));
        String selectSQL = "SELECT * FROM messages WHERE (status = 'select' AND date_end is null AND kafka_topic = ? AND server = ? AND typeMes IN(" + placeholders + ")) " +
                "OR (status = 'error' AND NUM_ATTEMPT < ? AND kafka_topic = ? AND server = ? AND typeMes IN(" + placeholders + ")) LIMIT ?";
        // Добавены сообщения с Error кол-во цикла не превышает NUM_ATTEMPT
        List<MessageData> aListMessage = new ArrayList<>();
        ArrayList<Integer> id_error = new ArrayList<>();
        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(selectSQL)) {

            preparedStatement.setString(1, topic);
            preparedStatement.setString(2, server);
            for (int i = 0; i < types.length; i++) {
                preparedStatement.setString(3 + i, types[i].trim());
            }
            preparedStatement.setInt(3 + types.length, NUM_ATTEMPT);
            preparedStatement.setString(4 + types.length, topic);
            preparedStatement.setString(5 + types.length, server);
            for (int i = 0; i < types.length; i++) {
                preparedStatement.setString(6 + i + types.length, types[i].trim());
            }
            preparedStatement.setInt(6 + types.length * 2, limitSelect);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {

                    if (getErrorMessage(resultSet) == 0) {
                        aListMessage.add(convertResultSetToMessageData(resultSet)); //-> если кто-то добавит текст (НЕ JSON) просто закоментировать эту строку, потом вернуть.
                    } else {
                        id_error.add(getErrorMessage(resultSet));
                    }
                }
                //return aListMessage;
            }
        } catch (SQLException e) {
            logger.error("Error processing messages from database", e);
        }
        //Обновим статус записи с ошибкой внутри JSON
        for (Integer id : id_error) {
            try {
                updateMessageStatusDate(topic, server, id, "error", new Timestamp(System.currentTimeMillis()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return aListMessage;
    }
}
