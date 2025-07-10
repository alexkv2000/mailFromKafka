package kvo.separat.kafkaConsumer;

//import org.json.JSONArray;
//import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.io.IOException;
//import java.net.URL;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.StandardCopyOption;
//import java.util.UUID;

public class FileService {
    private static final Logger logger = LoggerFactory.getLogger(FileService.class);
//    private final String filePath;

//    public FileService(ConfigLoader configLoader) {
//        this.filePath = configLoader.getProperty("FILE_PATH");
//    }

//    public String getFilePath(UUID uuid) {
//        return filePath + uuid;
//    }
//
//    public String downloadFiles(JSONObject urls, UUID uuid) throws IOException {
//        Files.createDirectories(Path.of(filePath + uuid));
//
//        StringBuilder filePaths = new StringBuilder();
//        logger.info("Start download Files ...");
//        for (int i = 0; i < urls.length(); i++) {
//            String url = urls.getString(String.valueOf(i));
//            String file = downloadFile(url, uuid);
//
//            if (file != null && !file.isEmpty()) {
//                if (filePaths.length() > 0) {
//                    filePaths.append(", ");
//                }
//                filePaths.append(file);
//            }
//        }
//        logger.info("Stop download Files ...");
//        return filePaths.toString();
//    }
//    private String downloadFile(String urlString, UUID uuid) {
//        try {
//            URL url = new URL(urlString);
//            String fileName = urlString.substring(urlString.lastIndexOf('/') + 1);
//            Path targetPath = Path.of(filePath + uuid + "/" + fileName);
//            Files.copy(url.openStream(), targetPath, StandardCopyOption.REPLACE_EXISTING);
//            return targetPath.toString();
//        } catch (IOException e) {
//            logger.error("Error downloading file from URL: " + urlString, e);
//            return null;
//        }
//    }
//    public void deleteDirectory(UUID uuid) {
//        deleteDirectoryRecurs(Path.of(filePath + uuid));
//    }
//
//    private void deleteDirectoryRecurs(Path path) {
//        try {
//            Files.walk(path)
//                    .sorted((p1, p2) -> -p1.compareTo(p2))
//                    .forEach(p -> {
//                        try {
//                            Files.delete(p);
//                        } catch (IOException e) {
//                            logger.error("Error deleting file/directory: " + p, e);
//                        }
//                    });
//        } catch (IOException e) {
//            logger.error("Error walking directory: " + path, e);
//        }
//    }
}
