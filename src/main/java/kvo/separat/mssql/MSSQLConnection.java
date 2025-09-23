package kvo.separat.mssql;

import kvo.separat.kafkaConsumer.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

public class MSSQLConnection {
    private static final Logger logger = LoggerFactory.getLogger(MSSQLConnection.class);

    private MSSQLConnection() {
    }

    public static void deleteBinUUID(Connection connection, String uuid) {
        try {
            String deleteSQL = "DELETE FROM [dbo].[temp_message] WHERE [uuid] = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(deleteSQL);

            int totalRowsAffected = 0;
//            for (Integer id : idToDelete) {
                preparedStatement.setString(1, uuid);
//                preparedStatement.addBatch();  // Добавляем в пакет
//            }

            int[] batchResults = preparedStatement.executeBatch();  // Выполняем пакет
            totalRowsAffected = Arrays.stream(batchResults).sum();

            System.out.println("Удалено строк: " + totalRowsAffected);
        } catch (SQLException e) {
            System.err.println("Ошибка SQL: " + e.getMessage());
        }
    }

    public static void deleteBinMoreSevenDays(Connection connection, LocalDate data) {
        try {
            String deleteSQL = "DELETE FROM [dbo].[temp_message] WHERE [data_create]+7 <= ?;";
            PreparedStatement preparedStatement = connection.prepareStatement(deleteSQL);
            int totalRowsAffected = 0;
            preparedStatement.setString(1, data.toString());
            int[] batchResults = preparedStatement.executeBatch();  // Выполняем пакет
            totalRowsAffected = Arrays.stream(batchResults).sum();
            logger.info("Удалено из истории {} Binary файлов (MSSQL база)", totalRowsAffected);
        } catch (SQLException e) {
            logger.error("Ошибка удаления из истории Binary файлов (MSSQL база): " + e.getMessage());
        }
    }

    public static void updateStatusSQL(Connection connection, String uuid, String new_status) {
        try {
            // обновления поля status по ID
            String updateSQL = "UPDATE [dbo].[temp_message] SET [status] = ? WHERE [uuid] = ?";
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

            String selSQL = "SELECT ID, uuid, namefiles, status, type, bin FROM [dbo].[temp_message] WHERE status='new' AND uuid ='" + uuid + "';";
            ResultSet resultSet = selectSQL(statement, selSQL);
            if (resultSet == null) {
                System.err.println("MessageData или Urls отсутствуют");
                return pathFiles;
            }

            Path filePathFull = null;
            while (resultSet.next()) {
                String name_file = null;
                byte[] bin = null;
                String uuid_ = null;

                try {
                    name_file = resultSet.getString("namefiles").trim();
                    // Проверка: если имя файла не указано (null, пустое или только пробелы), пропускаем закачку
                    if (name_file == null || name_file.trim().isEmpty()) {
                        logger.warn("Имя файла отсутствует для UUID: " + uuid + ". Закачка пропущена.");
                        continue;
                    }

                    // Проверка валидности имени файла
                    if (!isValidFileName(name_file)) {
                        logger.warn("Некорректное имя файла: '" + name_file + "' для UUID: " + uuid + ". Закачка пропущена.");
                        continue;
                    }

                    bin = resultSet.getBytes("bin");
                    // Проверка наличия бинарных данных
                    if (bin == null) {
                        logger.warn("Бинарные данные отсутствуют (null) для файла: " + name_file + ", UUID: " + uuid + ". Закачка пропущена.");
                        continue;
                    }

                    byte[] fileBytes = Base64.getDecoder().decode(bin);
                    // Дополнительная проверка: если бинарные данные пустые, пропускаем
                    if (fileBytes == null || fileBytes.length == 0) {
                        logger.warn("Бинарные данные отсутствуют или пустые для файла: " + name_file + ", UUID: " + uuid + ". Закачка пропущена.");
                        continue;
                    }

                    logger.info("Данные из таблицы MSSQL temp_message (uuid) : " + uuid);
                    uuid_ = resultSet.getString("uuid");
                    Path targetDir = Path.of(file_Path, uuid_);
                    Files.createDirectories(targetDir);

                    // Очистка имени файла от недопустимых символов и создание безопасного имени
                    String safeFileName = cleanFileName(name_file);
                    filePathFull = targetDir.resolve(safeFileName);

                    // Проверка, не существует ли уже файл с таким именем
                    if (Files.exists(filePathFull)) {
                        logger.warn("Файл уже существует: " + filePathFull + ". Закачка пропущена.");
                        continue;
                    }

                    Files.write(filePathFull, fileBytes);
                    pathFiles.append(filePathFull.toString()).append(", ");
                    logger.info("Файл успешно загружен: " + filePathFull);

                } catch (InvalidPathException e) {
                    logger.warn("Некорректный путь для файла: '" + name_file + "' для UUID: " + uuid + ". Закачка пропущена. Ошибка: " + e.getMessage());
                    continue;
                } catch (IllegalArgumentException e) {
                    logger.warn("Ошибка декодирования Base64 для файла: " + name_file + ", UUID: " + uuid + ". Закачка пропущена. Ошибка: " + e.getMessage());
                    continue;
                } catch (IOException e) {
                    logger.warn("Ошибка записи файла: '" + name_file + "' для UUID: " + uuid + ". Закачка пропущена. Ошибка: " + e.getMessage());
                    continue;
                } catch (Exception e) {
                    logger.warn("Неожиданная ошибка при обработке файла: '" + name_file + "' для UUID: " + uuid + ". Закачка пропущена. Ошибка: " + e.getMessage());
                    continue;
                }
            }

            // Обновление статуса
            updateStatusSQL(connection, String.valueOf(uuid), "update");

            connection.commit();
            System.out.println("Транзакция с UUID :" + uuid + " - успешно завершена");

        } catch (SQLException e) {
            System.err.println("Ошибка SQL / закрытии ресурсов: " + e.getMessage());
            logger.error("Ошибка SQL / закрытии ресурсов: ", e);
            try {
                if (connection != null) connection.rollback();
            } catch (SQLException ex) {
                logger.error("Ошибка при откате транзакции: ", ex);
            }
        } catch (ClassNotFoundException e) {
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
//            throw new RuntimeException(e); //не нужно проброса, просто изнорируем
        }
    }
    private static boolean isValidFileName(String fileName) {
        if (fileName == null || fileName.trim().isEmpty()) {
            return false;
        }

        // Проверка на запрещенные символы в именах файлов Windows/Unix
        String invalidChars = "/\\:*?\"<>|";
        for (char c : invalidChars.toCharArray()) {
            if (fileName.indexOf(c) != -1) {
                return false;
            }
        }

        // Проверка на зарезервированные имена Windows
        String[] reservedNames = {"CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4",
                "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2",
                "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"};
        String fileNameUpper = fileName.toUpperCase().split("\\.")[0];
        for (String reserved : reservedNames) {
            if (fileNameUpper.equals(reserved)) {
                return false;
            }
        }

        // Проверка длины имени файла
        if (fileName.length() > 255) {
            return false;
        }

        return true;
    }

    private static String cleanFileName(String fileName) {
        if (fileName == null) return "unknown_file";

        // Замена запрещенных символов на подчеркивания
        String cleaned = fileName.replaceAll("[\\\\/:*?\"<>|]", "_");

        // Удаление ведущих и завершающих точек и пробелов
        cleaned = cleaned.replaceAll("^\\.+|\\.+$", "").trim();

        // Если после очистки имя пустое, генерируем случайное имя
        if (cleaned.isEmpty()) {
            cleaned = "file_" + System.currentTimeMillis();
        }

        return cleaned;
    }
}