package com.DaDaoYunJiSuan.SparkStreaming.utils;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by Cristph on 2017/10/17.
 */
public class MongoDataMonitor {

    private final static String logPath="/tmp/userPattern.log";

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<Document> getMongodbCollection(String dbName, String collectionName) {
        if (mongoClient == null) {
            mongoClient = new MongoClient("localhost", 27017);
        }
        database = mongoClient.getDatabase(dbName);
        return database.getCollection(collectionName);
    }

    public void monitor() {
        MongoCollection<Document> collection = getMongodbCollection("test", "UserPattern");
        Path path= Paths.get(logPath);
        BufferedWriter bufferedWriter= null;
        try {
            bufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter finalBufferedWriter = bufferedWriter;
        Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                try {
                    finalBufferedWriter.write(new Date(System.currentTimeMillis()) + "\n========================");
                    finalBufferedWriter.write(document.toJson() + "\n========================");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(
                () -> {
                    collection.find(eq("name", "user15")).forEach(printBlock);
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    public static void main(String[] args) {
        MongoDataMonitor mongoDataMonitor=new MongoDataMonitor();
        mongoDataMonitor.monitor();
    }
}
