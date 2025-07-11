package kvo.separat.mssql;

import kvo.separat.kafkaConsumer.ConfigLoader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Arrays;
import java.util.Base64;

import static kvo.separat.ConsumerServerDV.configLoader;

public class MSSQLConnectionExample {
    private static String URL;
    private static String USER;
    private static String PASSWORD;
    private static String file_Path;

    public MSSQLConnectionExample(ConfigLoader configLoader) {
        URL = "jdbc:sqlserver://docprod\\sqlprod;databaseName=GAZ;encrypt=false;trustServerCertificate=true;";
        USER = "DVSQL";
        PASSWORD = "DV_Cthdbc14@";
        file_Path = configLoader.getProperty("FILE_PATH");
    }
//TODO должен возвращать StringBuilder полный_путь_к_ФАЙЛУ через ','
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        Connection connection = null;
        Statement statement = null;

        String currentDir = System.getProperty("user.dir");
        String configPath = currentDir + "\\config\\setting.txt";
        ConfigLoader configLoader = new ConfigLoader(configPath);
        MSSQLConnectionExample mssqlConnectionExample = new MSSQLConnectionExample(configLoader);
        try {
//            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            connection.setAutoCommit(false);

            //Выборка всех данных
            System.out.println("Данные из таблицы temp_message:");
            statement = connection.createStatement();
            String selSQL = "SELECT ID, uuid, namefiles, status, type, bin FROM [dbo].[temp_message] WHERE status='new' AND uuid like ('%4c9f9132-3c22-419e-94a1-d4c9d59882e3%')";
            ResultSet resultSet = selectSQL(statement, selSQL);
//***********************************************************************************
//      *Обновление статуса
//            String selSQL = "SELECT namefiles, bin FROM [dbo].[temp_message]";
//            Integer idToUpdate = 7;
//            updateStatusSQL(connection, idToUpdate, "update");
//      *Удаление записи по ID
//            Integer[] idToDelete = {7, 10};
//            deleteSQL(connection, idToDelete);
//***********************************************************************************
            while (resultSet.next()) {
                String name_file = resultSet.getString("namefiles").trim();
                byte[] bin = resultSet.getBytes("bin");
                byte[] fileBytes = Base64.getDecoder().decode(bin);
//                System.out.println(Arrays.toString(fileBytes));
                String uuid = resultSet.getString("uuid");
                Path targetDir = Path.of(file_Path, uuid);
                Files.createDirectories(targetDir);
                Path filePathFull = targetDir.resolve(name_file);
                Files.write(filePathFull, fileBytes);
            }
            connection.commit();
            System.out.println("Транзакция успешно завершена");
        } catch (SQLException e) {
            System.err.println("Ошибка SQL / закрытии ресурсов: " + e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                System.err.println("Ошибка при закрытии ресурсов: " + e.getMessage());
            }
        }
    }

    public static void deleteSQL(Connection connection, Integer[] idToDelete) {
        try {
            String deleteSQL = "DELETE FROM [dbo].[temp_message] WHERE ID = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(deleteSQL);

            int totalRowsAffected = 0;
            for (Integer id : idToDelete) {
                preparedStatement.setInt(1, id);
                preparedStatement.addBatch();  // Добавляем в пакет
            }

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

    public static void updateStatusSQL(Connection connection, Integer idToUpdate, String new_status) {
        try {
            // 4. Пример обновления поля status по ID

            String updateSQL = "UPDATE [dbo].[temp_message] SET status = ? WHERE ID = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(updateSQL);
            preparedStatement.setString(1, new_status);
            preparedStatement.setInt(2, idToUpdate);

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
}