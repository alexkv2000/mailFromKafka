package kvo.separat.mssql;

import java.sql.*;

public class MSSQLConnectionExample {
    private static final String URL = "jdbc:sqlserver://docprod\\sqlprod;databaseName=GAZ;encrypt=false;trustServerCertificate=true;";
    private static final String USER = "DVSQL";
    private static final String PASSWORD = "DV_Cthdbc14@";

    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            // 1. Загрузка драйвера JDBC
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            // 2. Установка соединения с базой данных
            connection = DriverManager.getConnection(URL, USER, PASSWORD);

            // 3. Выборка всех данных из таблицы dvsys_binaries
            System.out.println("Данные из таблицы dvsys_binaries:");
            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT top (100) ID, FullTextTimeStamp, Type, StreamData, PartNum, StorageID, Path, Size FROM [dbo].[dvsys_binaries]");

            // Вывод результатов выборки
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i) + ": " + resultSet.getString(i) + " | ");
                }
                System.out.println();
            }

            // 4. Пример обновления поля status по ID
//            int idToUpdate = 1; // Укажите нужный ID
//            String newStatus = "updated"; // Новое значение для status
//
//            String updateSQL = "UPDATE [dbo].[dvsys_binaries] SET status = ? WHERE ID = ?";
//            PreparedStatement preparedStatement = connection.prepareStatement(updateSQL);
//            preparedStatement.setString(1, newStatus);
//            preparedStatement.setInt(2, idToUpdate);
//
//            int rowsAffected = preparedStatement.executeUpdate();
//            System.out.println("Обновлено строк: " + rowsAffected);

        } catch (SQLException e) {
            System.err.println("Ошибка SQL: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            // 5. Закрытие ресурсов
            try {
                if (resultSet != null) resultSet.close();
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                System.err.println("Ошибка при закрытии ресурсов: " + e.getMessage());
            }
        }
    }
}