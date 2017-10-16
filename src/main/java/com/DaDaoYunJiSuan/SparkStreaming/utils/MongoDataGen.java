package com.DaDaoYunJiSuan.SparkStreaming.utils;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by Cristph on 2017/10/16.
 *
 * generate test data and insert into mongodb
 * =================================================
 * people:
 *
 * {
 *  "_id" :{ "$oid" : "59e47187fd6cea0b9cb4e715" },
 *  "url" : "/group/0",
 *  "name" : "group0",
 *  "tags" : ["tag6"]
 * }
 *
 * {
 *  "_id" : { "$oid" : "59e47187fd6cea0b9cb4e716" },
 *  " url" : "/group/1",
 *  "name" : "group1",
 *  "tags" : ["tag2", "tag3", "tag4", "tag7"]
 * }
 *
 * ================================================
 * group:
 *
 * {
 *  "_id" : { "$oid" : "59e47187fd6cea0b9cb4e724" },
 *  "url" : "/people/0",
 *  "name" : "user0",
 *  "join_groups" : [
 *                  { "group_url" : "/group/3", "time" : { "$date" : 1508142666764 } },
 *                  { "group_url" : "/group/13", "time" : { "$date" : 1508142824565 } },
 *                  { "group_url" : "/group/0", "time" : { "$date" : 1508143309339 } }
 *                  ]
 * }
 *
 * {
 *  "_id" : { "$oid" : "59e47187fd6cea0b9cb4e725" },
 *  "url" : "/people/1",
 *  "name" : "user1",
 *  "join_groups" : [
 *                   { "group_url" : "/group/2", "time" : { "$date" : 1508142568434 } },
 *                   { "group_url" : "/group/10", "time" : { "$date" : 1508142636361 } },
 *                   { "group_url" : "/group/9", "time" : { "$date" : 1508143183172 } },
 *                   { "group_url" : "/group/5", "time" : { "$date" : 1508143325771 } }
 *                   ]
 *}
 */
public class MongoDataGen {

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private final int tagArrayLen = 15;
    private final int unameArrayLen = 1000;
    private final int gnameArrayLen = 15;

    private String[] tagArray;
    private String[] unameArray;
    private String[] gnameArray;

    public void ini() {

        if (mongoClient == null) {
            mongoClient = new MongoClient("localhost", 27017);
            database = mongoClient.getDatabase("test");
        }

        tagArray = new String[tagArrayLen];
        for (int i = 0; i < tagArrayLen; i++) {
            tagArray[i] = "tag" + i;
        }

        unameArray = new String[unameArrayLen];
        for (int j = 0; j < unameArrayLen; j++) {
            unameArray[j] = "user" + j;
        }

        gnameArray = new String[gnameArrayLen];
        for (int k = 0; k < gnameArrayLen; k++) {
            gnameArray[k] = "group" + k;
        }
    }

    public void genData() {
        MongoCollection<Document> collection = database.getCollection("testCollection");
        Random rd = new Random();
        List<Document> gdocuments = getGDocs(rd);
        collection.insertMany(gdocuments);

        List<Document> udocuments = getUDocs(rd);
        collection.insertMany(udocuments);
    }

    public void printColectionNames() {
        System.out.print("Collections:[ ");
        for (String name : database.listCollectionNames()) {
            System.out.print(name + " ");
        }
        System.out.println("]");
    }


    private List<String> getTags(Random rd) {
        List<String> list = new ArrayList<>();
        int tagsNum = rd.nextInt(6) + 1;
        for (int i = 0; i < tagsNum; i++) {
            String tmp = tagArray[rd.nextInt(tagArray.length)];
            if (!list.contains(tmp)) {
                list.add(tmp);
            }
        }
        return list;
    }

    private List<Document> getGDocs(Random rd) {
        List<Document> gdocuments = new ArrayList<>();
        for (int i = 0; i < gnameArrayLen; i++) {
            Document gdoc = new Document("url", "/group/" + i)
                    .append("name", gnameArray[i])
                    .append("tags", getTags(rd));
            gdocuments.add(gdoc);
        }
        return gdocuments;
    }

    private List<Document> getUDocs(Random rd) {
        List<Document> udocuments = new ArrayList<>();
        int groupNum = rd.nextInt(gnameArrayLen - 1) + 1;
        for (int i = 0; i < unameArrayLen; i++) {
            Document udoc = new Document("url", "/people/" + i)
                    .append("name", unameArray[i]);

            List<Document> joinGroups = new ArrayList<>();
            for (int j = 0; j < groupNum; j++) {
                String tmp = "/group/" + rd.nextInt(gnameArrayLen);
                if (!checkIsExist(joinGroups, tmp)) {
                    joinGroups.add(new Document("group_url", tmp)
                            .append("time", new Date(System.currentTimeMillis() - ((long) rd.nextInt(1000000)))));
                }
            }

            udoc.append("join_groups", joinGroups);
            udocuments.add(udoc);
        }
        return udocuments;
    }

    private boolean checkIsExist(List<Document> list, String gurl) {
        return list.stream()
                .anyMatch(document -> document.get("group_url").equals(gurl));
    }

    public void delAll() {
        MongoCollection<Document> collection = database.getCollection("testCollection");
        collection.drop();
        System.out.println(database.getCollection("testCollection") == null ? "finish del collection" : "del collection fail");
    }


    public void search() {
        MongoCollection<Document> collection = database.getCollection("testCollection");
        Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson() + "\n");
            }
        };
        collection.find().forEach(printBlock);
    }

    public static void main(String[] args) {
        MongoDataGen mongoDataGen = new MongoDataGen();
        mongoDataGen.ini();
        mongoDataGen.printColectionNames();
        mongoDataGen.delAll();
        mongoDataGen.printColectionNames();
        mongoDataGen.genData();
        mongoDataGen.search();
    }

}
