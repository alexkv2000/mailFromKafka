package kvo.separat.kafkaConsumer;

import kvo.separat.mssql.MSSQLConnection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DatabaseService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private final int NUM_ATTEMPT;
    private static String dbUrl = null;
    private static String user = null;
    private static String password = null;
    private static String mssqlUrl = null;
    private static String mssqlUser = null;
    private static String mssqlPassword = null;

    public DatabaseService(ConfigLoader configLoader) {
        dbUrl = configLoader.getProperty("DB_PATH");
        user = configLoader.getProperty("USER_SQL");
        password = configLoader.getProperty("PASSWORD_SQL");
        NUM_ATTEMPT = Integer.parseInt(configLoader.getProperty("NUM_ATTEMPT"));

        mssqlUrl = configLoader.getProperty("URL_MSSQL");
        mssqlUser = configLoader.getProperty("USER_MSSQL");
        mssqlPassword = configLoader.getProperty("PASSWORD_MSSQL");
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl, user, password);
    }

    public static void setStatisticSizeTable(String fileGroupName) throws ClassNotFoundException {
        try {
            ResultSet resultSet;
            resultSet = MSSQLConnection.getStatisticSizeTable(fileGroupName, mssqlUrl, mssqlUser, mssqlPassword);
            Connection connection = DriverManager.getConnection(dbUrl, user, password);
            deleteStatisticPeriodDay(connection,"7");

            String setStatisticSizeTableSQL = "INSERT INTO table_sizes (TableName, FileGroupName, TotalSizeMB, UsedSizeMB, FreeSizeMB, DataSize) SELECT TABLE_NAME AS TableName, 'PRIMARY' AS FileGroupName, ROUND((data_length + index_length) / 1024 / 1024, 2) AS TotalSizeMB, ROUND(data_length / 1024 / 1024, 2) AS UsedSizeMB, ROUND(index_length / 1024 / 1024, 2) AS FreeSizeMB, DATE_FORMAT(NOW(), '%d-%m-%Y') AS DataSize FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY UsedSizeMB DESC;";

            PreparedStatement insertStmt = connection.prepareStatement(setStatisticSizeTableSQL);
            LocalDate currentDate = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
            //Поиск даты обновления
            String sql = "select max(DataSize) AS dateMax from table_sizes";
            String dMax = "";
            try (PreparedStatement pstmt = connection.prepareStatement(sql);
                 ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    dMax = rs.getString("dateMax");  // Возвращает строку в формате dd-MM-yyyy или null, если нет данных
                }
            } catch (Exception e) {
                connection.close();
                e.printStackTrace();
            }
            if (Objects.equals(dMax, null)) {
                dMax = "01-01-1900";
            }
            //Обновить новыми данными
            while (resultSet != null && resultSet.next()) {
                String tableName = resultSet.getString("TableName");
                String fileGroupNameValue = resultSet.getString("FileGroupName");
                double totalSizeMB = resultSet.getDouble("TotalSizeMB");
                double usedSizeMB = resultSet.getDouble("UsedSizeMB");
                double freeSizeMB = resultSet.getDouble("FreeSizeMB");
                String dataSize = resultSet.getString("DataSize");
                LocalDate parsedDate = LocalDate.parse(dMax, formatter);
                if (currentDate.isEqual(parsedDate)) {
                    resultSet.close();
                    break;
                }
                String insertSql = "INSERT INTO table_sizes (TableName, FileGroupName, TotalSizeMB, UsedSizeMB, FreeSizeMB, DataSize) VALUES ('" + tableName + "', '" + fileGroupNameValue + "', " + totalSizeMB + ", " + usedSizeMB + ", " + freeSizeMB + ", '" + dataSize + "');";
                insertStmt.executeUpdate(insertSql);
            }
            assert resultSet != null;
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void deleteStatisticPeriodDay(Connection connection, String days) throws SQLException {
        String delStatisticSizeTableSQL = "delete FROM table_sizes WHERE STR_TO_DATE(DataSize, '%d-%m-%Y') IS NOT NULL AND STR_TO_DATE(DataSize, '%d-%m-%Y') + INTERVAL " + days + " DAY <= CURDATE() LIMIT 5000;";
        PreparedStatement insertStmtDel = connection.prepareStatement(delStatisticSizeTableSQL);
        insertStmtDel.executeUpdate();
    }

    private static void printResultSet(ResultSet resultSet, boolean columnName) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        if (columnName) { // Вывод заголовков столбцов
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i) + "\t");
            }
            System.out.println();
        }
        // Вывод данных строк
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.println();
        }
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

    public void insertMessages(List<ConsumerRecord<String, String>> records, String typeMessage, String extractedUUID) {
        insertMessages(records, "", typeMessage, extractedUUID);
    }

    public void insertMessages(List<ConsumerRecord<String, String>> records, String server, String typeMessage, String uuid) {
        String insertSQL = "INSERT INTO messages (kafka_topic, message, date_create, server, typeMes, uuid) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            for (ConsumerRecord<String, String> recordMessage : records) {
                int paramIndex = 1;
                preparedStatement.setString(paramIndex++, recordMessage.topic());
                preparedStatement.setString(paramIndex++, recordMessage.value());
                preparedStatement.setTimestamp(paramIndex++, new Timestamp(System.currentTimeMillis()));
                preparedStatement.setString(paramIndex++, server);
                preparedStatement.setString(paramIndex++, typeMessage);
                preparedStatement.setString(paramIndex++, uuid);
                //preparedStatement.addBatch(); //пошли дубли рассылки сообщений
                preparedStatement.executeUpdate();
            }
//            preparedStatement.executeBatch();  //пошли дубли рассылки сообщений
        } catch (SQLException e) {
            logger.error("Error inserting messages into database", e);
        }
    }

    static void deleteOldMessages(Integer days) {
        String deleteSQL = "DELETE FROM messages WHERE date_create < DATE_SUB(NOW(), INTERVAL " + days.toString() + " DAY)";

        try (Connection connection = getConnection();
             PreparedStatement deleteStatement = connection.prepareStatement(deleteSQL)) {

            int deletedCount = deleteStatement.executeUpdate();
            logger.info("Удалено {} записей из MySQL (старше 7 дней)", deletedCount);

        } catch (SQLException e) {
            logger.error("Ошибка удаление старых сообщений из MySQL базы : ", e);
        }
    }

    public void repeatSend(Integer messageId) {
        updateParametersMessage(messageId, 0);
    }

    public void updateParametersMessage(Integer messageId, Integer NUM_ATTEMPT) {
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
        String placeholders = String.join(",", Collections.nCopies(types.length, "?"));

        String updateSQL = "UPDATE messages m1 JOIN (SELECT id FROM messages WHERE status IS NULL AND server = ? AND kafka_topic = ? AND typeMes IN (" + placeholders + ") LIMIT ?) m2 ON m1.id = m2.id SET m1.status = ?, m1.server = ?;";
        try (Connection connection = getConnection();
             PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
            int paramIndex = 1;
            // server = ?
            updateStatement.setString(paramIndex++, "");
            // kafka_topic = ?
            updateStatement.setString(paramIndex++, topic);
            // typeMes IN (?, ?, ...)
            for (String type : types) {
                updateStatement.setString(paramIndex++, type.trim());
            }
            // LIMIT ?
            updateStatement.setInt(paramIndex++, limitSelect);
            // SET m1.status = ?
            updateStatement.setString(paramIndex++, status);
            // SET m1.server = ?
            updateStatement.setString(paramIndex++, server);
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
            int paramIndex = 1;
            updateStatement.setString(paramIndex++, status);
            updateStatement.setTimestamp(paramIndex++, timestamp);
            updateStatement.setInt(paramIndex++, ++numAttempt);
            updateStatement.setInt(paramIndex++, messageId);
            updateStatement.setString(paramIndex++, topic);
            updateStatement.setString(paramIndex++, server);
            updateStatement.executeUpdate();
            logger.info("Database UPDATE Statue and Date_END");
        } catch (SQLException e) {
            logger.error("Error updating message status in database", e);
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
            int paramIndex = 1;
            preparedStatement.setString(paramIndex++, topic);
            preparedStatement.setString(paramIndex++, server);
            for (int i = 0; i < types.length; i++) {
                preparedStatement.setString(paramIndex++, types[i].trim());
            }
            preparedStatement.setInt(paramIndex++, NUM_ATTEMPT);
            preparedStatement.setString(paramIndex++, topic);
            preparedStatement.setString(paramIndex++, server);
            for (int i = 0; i < types.length; i++) {
                preparedStatement.setString(paramIndex++, types[i].trim());
            }
            preparedStatement.setInt(paramIndex++, limitSelect);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {

                    if (getErrorMessage(resultSet) == 0) {
                        aListMessage.add(convertResultSetToMessageData(resultSet)); //-> если кто-то добавит текст (НЕ JSON) просто закоментировать эту строку, потом вернуть.
                    } else {
                        id_error.add(getErrorMessage(resultSet));
                    }
                }
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
