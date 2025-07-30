package kvo.separat;


import java.io.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import kvo.separat.kafkaConsumer.MessageData;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import kvo.separat.kafkaConsumer.ConfigLoader;

import static com.google.gson.JsonParser.parseString;
import static kvo.separat.ConsumerServerDV.configLoader;

public class SoapDownloadBinaryDV {
    private static final Logger logger = LoggerFactory.getLogger(SoapDownloadBinaryDV.class);
    private static String filePath;

    public SoapDownloadBinaryDV(ConfigLoader configLoader) {
        filePath = configLoader.getProperty("FILE_PATH");
    }
    public static void deleteDirectory(UUID uuid) {
        deleteDirectoryRecurs(Path.of(filePath + uuid));
        logger.info("Deleted directory success: " + filePath + uuid);
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