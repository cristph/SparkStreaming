package com.DaDaoYunJiSuan.SparkStreaming.utils;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * Created by Cristph on 2017/10/18.
 */
public class MongoDataSocket {

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private final static int BUFFER_SIZE=100;

    private MongoCollection<Document> getMongodbCollection(String dbName, String collectionName) {
        if (mongoClient == null) {
            mongoClient = new MongoClient("localhost", 27017);
        }
        database = mongoClient.getDatabase(dbName);
        return database.getCollection(collectionName);
    }

    private void sendData() throws IOException {

        MongoCollection<Document> collection = getMongodbCollection("test", "rel");
        MongoCursor<Document> cursor=collection.find().sort(Sorts.ascending("time")).iterator();
        StringBuilder stringBuilder=new StringBuilder();
        int count=0;

        Socket socket = new Socket("localhost", 1234);
        OutputStream ops = socket.getOutputStream();
        OutputStreamWriter opsw = new OutputStreamWriter(ops);
        BufferedWriter bw = new BufferedWriter(opsw);

        StringBuilder tmpBuilder=new StringBuilder();
        while(cursor.hasNext()){
            stringBuilder.append(getText(tmpBuilder, cursor.next()));
            count++;
            if(count%BUFFER_SIZE==0){
                bw.write(stringBuilder.toString());
                System.out.println(stringBuilder.toString());
                bw.flush();
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                stringBuilder.setLength(0);
            }
        }

        String left=stringBuilder.toString();
        if(left.length()>0){
            bw.write(left);
            bw.flush();
        }

        bw.close();
    }

    private String getText(StringBuilder stringBuilder, Document document){
        stringBuilder.setLength(0);
        document.values().forEach(s->{
            stringBuilder.append(s);
            stringBuilder.append(" ");
        });
        stringBuilder.append("\n");
        return stringBuilder.toString();
    }

    public static void main(String [] args){
        MongoDataSocket mongoDataSocket=new MongoDataSocket();
        try {
            mongoDataSocket.sendData();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
