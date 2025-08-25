package kvo.separat.mssql;

import kvo.separat.kafkaConsumer.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

public class MSSQLConnection {
    private static String URL;
    private static String USER;
    private static String PASSWORD;
    private static String file_Path;
    private static final Logger logger = LoggerFactory.getLogger(MSSQLConnection.class);

    private MSSQLConnection(ConfigLoader configLoader) {
        URL = configLoader.getProperty("URL_MSSQL");
        USER = configLoader.getProperty("USER_MSSQL");
        PASSWORD = configLoader.getProperty("PASSWORD_MSSQL");
        file_Path = configLoader.getProperty("FILE_PATH");
    }

    public static String getURL() {
        return URL;
    }

    public static void setURL(String URL) {
        MSSQLConnection.URL = URL;
    }

    public static void deleteSQL(Connection connection, String uuid) {
        try {
//            String deleteSQL = "DELETE FROM [dbo].[temp_message] WHERE [uuid] like ('%?%')"; //TODO *****
            String deleteSQL = "DELETE FROM [dbo].[temp_message] WHERE [uuid] = '?'";
            PreparedStatement preparedStatement = connection.prepareStatement(deleteSQL);

            int totalRowsAffected = 0;
//            for (Integer id : idToDelete) {
                preparedStatement.setString(1, uuid);
//                preparedStatement.addBatch();  // Добавляем в пакет
//            }

            int[] batchResults = preparedStatement.executeBatch();  // Выполняем пакет
            totalRowsAffected = Arrays.stream(batchResults).sum();

            System.out.println("Удалено строк: " + totalRowsAffected);
//
//            preparedStatement.setInt(1, idToDelete);
//
//            int rowsAffected = preparedStatement.executeUpdate();
//            System.out.println("Удалено строк: " + rowsAffected);
        } catch (SQLException e) {
            System.err.println("Ошибка SQL: " + e.getMessage());
        }
    }

    public static void updateStatusSQL(Connection connection, String uuid, String new_status) {
        try {
            // 4. Пример обновления поля status по ID

//            String updateSQL = "UPDATE [dbo].[temp_message] SET [status] = ? WHERE [uuid] like ?"; //TODO *****
            String updateSQL = "UPDATE [dbo].[temp_message] SET [status] = ? WHERE [uuid] = `?`";
            PreparedStatement preparedStatement = connection.prepareStatement(updateSQL);
            preparedStatement.setString(1, new_status);
            preparedStatement.setString(2, uuid);

            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("Обновлено строк: " + rowsAffected);

        } catch (SQLException e) {
            System.err.println("Ошибка SQL: " + e.getMessage());
        }
    }

    public static ResultSet selectSQL(Statement statement, String select) {
        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery(select);
            return resultSet;
//// Вывод результатов выборки
//            ResultSetMetaData metaData = resultSet.getMetaData();
//            int columnCount = metaData.getColumnCount();
//            while (resultSet.next()) {
//                for (int i = 1; i <= columnCount; i++) {
////                System.out.print(metaData.getColumnName(i).trim() + ": " + resultSet.getString(i).trim() + " | ");
//                    System.out.print(resultSet.getString(i).trim() + " | ");
//                }
//                System.out.println();
//            }

        } catch (SQLException e) {
            System.err.println("Ошибка SQL: " + e.getMessage());
        }
        return resultSet;
    }

    public static StringBuilder DownloadBinaryDV(UUID uuid, String URL, String  USER, String  PASSWORD, String file_Path) throws IOException {
        Connection connection = null;
        Statement statement = null;
        StringBuilder pathFiles = new StringBuilder();

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            connection.setAutoCommit(false);

            //Выборка всех данных
            System.out.println("Данные из таблицы MSSQL temp_message (закачка Binary):");
            statement = connection.createStatement();


//            String selSQL = "SELECT ID, uuid, namefiles, status, type, bin FROM [dbo].[temp_message] WHERE status='new' AND uuid like ('%" + uuid + "%')"; //TODO *****
            String selSQL = "SELECT ID, uuid, namefiles, status, type, bin FROM [dbo].[temp_message] WHERE status='new' AND uuid ='" + uuid + "';";
            ResultSet resultSet = selectSQL(statement, selSQL);
            if (resultSet == null) {
                System.err.println("MessageData или Urls отсутствуют");
                return pathFiles;
            }
            Path filePathFull = null;
            while (resultSet.next()) {
                String name_file = resultSet.getString("namefiles").trim();
                byte[] bin = resultSet.getBytes("bin");
                byte[] fileBytes = Base64.getDecoder().decode(bin);
//                System.out.println(Arrays.toString(fileBytes));
                logger.info("Данные из таблицы MSSQL temp_message (uuid) : " + uuid);
                String uuid_ = resultSet.getString("uuid");
                Path targetDir = Path.of(file_Path, uuid_);
                Files.createDirectories(targetDir);
                filePathFull = targetDir.resolve(name_file);
                Files.write(filePathFull, fileBytes);
                pathFiles.append(filePathFull.toString()).append(", ");
            }
//***********************************************************************************
//      *Обновление статуса
            updateStatusSQL(connection, String.valueOf(uuid), "update"); //TODO удалено из-за оправки пустых сообщений при сбоях
//      *Удаление записи по ID
//            Integer[] idToDelete = {7, 10};
//            deleteSQL(connection, String.valueOf(uuid));
//***********************************************************************************
            connection.commit();
            System.out.println("Транзакция с UUID :" + uuid + " - успешно завершена");
            // Добавляем путь к результату


        } catch (SQLException e) {
            System.err.println("Ошибка SQL / закрытии ресурсов: " + e.getMessage());
            logger.error("Ошибка SQL / закрытии ресурсов: ", e);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                System.err.println("Ошибка при закрытии ресурсов: " + e.getMessage());
                logger.error("Ошибка при закрытии ресурсов: ", e);
            }
        }
        return pathFiles;
    }
    public static void deleteDirectory(UUID uuid, String file_Path) {
        deleteDirectoryRecurs(Path.of(file_Path + uuid));
        logger.info("Deleted directory success: " + file_Path + uuid);
    }

    private static void deleteDirectoryRecurs(Path path) {
        try {
            Files.walk(path)
                    .sorted((p1, p2) -> -p1.compareTo(p2))
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            logger.error("Error deleting file/directory: " + p, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Error walking directory: " + path + " ", e);
//            throw new RuntimeException(e); //не нужно ожидание, просто изнорируем
        }
    }
}