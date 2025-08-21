package kvo.separat.mssql;

import kvo.separat.kafkaConsumer.ConfigLoader;
import kvo.separat.kafkaConsumer.DatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import static kvo.separat.ConsumerServerDV.configLoader;

public class MSSQLConnectionExample {
    private String URL;
    private String USER;
    private String PASSWORD;
    private static String file_Path;
    private static final Logger logger = LoggerFactory.getLogger(MSSQLConnectionExample.class);
    public MSSQLConnectionExample(ConfigLoader configLoader) {
        this.URL = "jdbc:sqlserver://docprod\\sqlprod;databaseName=GAZ;encrypt=false;trustServerCertificate=true;";
        this.USER = "DVSQL";
        this.PASSWORD = "DV_Cthdbc14@";
        file_Path = configLoader.getProperty("FILE_PATH");
    }

    //TODO должен возвращать StringBuilder полный_путь_к_ФАЙЛУ через ','
//    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
//        Connection connection = null;
//        Statement statement = null;
//
//        String currentDir = System.getProperty("user.dir");
//        String configPath = currentDir + "\\config\\setting.txt";
//        ConfigLoader configLoader = new ConfigLoader(configPath);
//        MSSQLConnectionExample mssqlConnectionExample = new MSSQLConnectionExample(configLoader);
//        try {
////            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
//            connection = DriverManager.getConnection(mssqlConnectionExample.URL, mssqlConnectionExample.USER, mssqlConnectionExample.PASSWORD);
//            connection.setAutoCommit(false);
//
//            //Выборка всех данных
//            System.out.println("Данные из таблицы temp_message:");
//            statement = connection.createStatement();
//            ArrayList<String> arrayList = new ArrayList<>(Arrays.asList("4c9f9132-3c22-419e-94a1-d4c9d59882e3",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e4",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e3",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e5",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e3",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e3",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e6",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e7",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e8",
//                    "4c9f9132-3c22-419e-94a1-d4c9d59882e9",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988213",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988223",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988233",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988243",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988253",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988263",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988273",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988283",
//                    "4c9f9132-3c22-419e-94a1-d4c9d5988293"));
//            String selSQL = "";
//            for (String uuid : arrayList) {
//
//
//                selSQL = "SELECT ID, uuid, namefiles, status, type, bin FROM [dbo].[temp_message] WHERE status='new' AND uuid like ('%" + uuid + "%')";
//                ResultSet resultSet = selectSQL(statement, selSQL);
////***********************************************************************************
////      *Обновление статуса
////            String selSQL = "SELECT namefiles, bin FROM [dbo].[temp_message]";
////            Integer idToUpdate = 7;
////            updateStatusSQL(connection, idToUpdate, "update");
////      *Удаление записи по ID
////            Integer[] idToDelete = {7, 10};
////            deleteSQL(connection, idToDelete);
////***********************************************************************************
//                while (resultSet.next()) {
//                    String name_file = resultSet.getString("namefiles").trim();
//                    byte[] bin = resultSet.getBytes("bin");
//                    byte[] fileBytes = Base64.getDecoder().decode(bin);
////                System.out.println(Arrays.toString(fileBytes));
//                    String uuid_ = resultSet.getString("uuid");
//                    Path targetDir = Path.of(file_Path, uuid_);
//                    Files.createDirectories(targetDir);
//                    Path filePathFull = targetDir.resolve(name_file);
//                    Files.write(filePathFull, fileBytes);
//                }
//                connection.commit();
//                System.out.println("Транзакция с UUID :" + uuid + " - успешно завершена");
//            }
//        } catch (SQLException e) {
//            System.err.println("Ошибка SQL / закрытии ресурсов: " + e.getMessage());
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        } finally {
//            try {
//                if (statement != null) statement.close();
//                if (connection != null) connection.close();
//            } catch (SQLException e) {
//                System.err.println("Ошибка при закрытии ресурсов: " + e.getMessage());
//            }
//        }
//    }

    public static void deleteSQL(Connection connection, String uuid) {
        try {
            String deleteSQL = "DELETE FROM [dbo].[temp_message] WHERE [uuid] like ('%?%')";
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

            String updateSQL = "UPDATE [dbo].[temp_message] SET [status] = ? WHERE [uuid] like ?";
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
            // Вывод результатов выборки
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

    public static StringBuilder DownloadBinaryDV(UUID uuid) throws IOException {
        Connection connection = null;
        Statement statement = null;
        StringBuilder pathFiles = new StringBuilder();

        String currentDir = System.getProperty("user.dir");
        String configPath = currentDir + "\\config\\setting.txt";
        ConfigLoader configLoader = new ConfigLoader(configPath);
        MSSQLConnectionExample mssqlConnectionExample = new MSSQLConnectionExample(configLoader);
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(mssqlConnectionExample.URL, mssqlConnectionExample.USER, mssqlConnectionExample.PASSWORD);
            connection.setAutoCommit(false);

            //Выборка всех данных
            System.out.println("Данные из таблицы MSSQL temp_message:");
            statement = connection.createStatement();
            String selSQL = "";


            selSQL = "SELECT ID, uuid, namefiles, status, type, bin FROM [dbo].[temp_message] WHERE status='new' AND uuid like ('%" + uuid + "%')";
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
                String uuid_ = resultSet.getString("uuid");
                Path targetDir = Path.of(file_Path, uuid_);
                Files.createDirectories(targetDir);
                filePathFull = targetDir.resolve(name_file);
                Files.write(filePathFull, fileBytes);
                pathFiles.append(filePathFull.toString()).append(", ");
            }
//***********************************************************************************
//      *Обновление статуса
            updateStatusSQL(connection, String.valueOf(uuid), "update");
//      *Удаление записи по ID
//            Integer[] idToDelete = {7, 10};
//            deleteSQL(connection, String.valueOf(uuid));
//***********************************************************************************
            connection.commit();
            System.out.println("Транзакция с UUID :" + uuid + " - успешно завершена");
            // Добавляем путь к результату


        } catch (SQLException e) {
            System.err.println("Ошибка SQL / закрытии ресурсов: " + e.getMessage());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                System.err.println("Ошибка при закрытии ресурсов: " + e.getMessage());
            }
        }
        return pathFiles;
    }
    public static void deleteDirectory(UUID uuid) {
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
            logger.error("Error walking directory: " + path, e);
        }
    }
}