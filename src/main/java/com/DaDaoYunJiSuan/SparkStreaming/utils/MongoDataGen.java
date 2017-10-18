package com.DaDaoYunJiSuan.SparkStreaming.utils;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.print.Doc;
import java.util.*;

/**
 * Created by Cristph on 2017/10/16.
 * <p>
 * generate test data and insert into mongodb
 * =================================================
 * group:
 * <p>
 * {
 * "_id" :{ "$oid" : "59e47187fd6cea0b9cb4e715" },
 * "url" : "/group/0",
 * "name" : "group0",
 * "tags" : ["tag6"]
 * }
 * <p>
 * {
 * "_id" : { "$oid" : "59e47187fd6cea0b9cb4e716" },
 * " url" : "/group/1",
 * "name" : "group1",
 * "tags" : ["tag2", "tag3", "tag4", "tag7"]
 * }
 * <p>
 * {
 * "_id" : { "$oid" : "59e47187fd6cea0b9cb4e716" },
 * " url" : "/group/1",
 * "name" : "group1",
 * "tags" : ["tag2", "tag3", "tag4", "tag7"]，
 * "members":[
 * { "name" : "user0", "url":"people/1", "time" : { "$date" : 1508143309339 } },
 * { "name" : "user0", "url":"people/1", "time" : { "$date" : 1508143309339 } }.
 * { "name" : "user0", "url":"people/1", "time" : { "$date" : 1508143309339 } }
 * ]
 * }
 * <p>
 * ================================================
 * user:
 * <p>
 * {
 * "_id" : { "$oid" : "59e47187fd6cea0b9cb4e724" },
 * "url" : "/people/0",
 * "name" : "user0",
 * "groups" : [
 * { "url" : "/group/3", "time" : { "$date" : 1508142666764 } },
 * { "url" : "/group/13", "time" : { "$date" : 1508142824565 } },
 * { "url" : "/group/0", "time" : { "$date" : 1508143309339 } }
 * ]
 * }
 * <p>
 * {
 * "_id" : { "$oid" : "59e47187fd6cea0b9cb4e725" },
 * "url" : "/people/1",
 * "name" : "user1",
 * "join_groups" : [
 * { "group_url" : "/group/2", "time" : { "$date" : 1508142568434 } },
 * { "group_url" : "/group/10", "time" : { "$date" : 1508142636361 } },
 * { "group_url" : "/group/9", "time" : { "$date" : 1508143183172 } },
 * { "group_url" : "/group/5", "time" : { "$date" : 1508143325771 } }
 * ]
 * }
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

    public void genUserData() {
        MongoCollection<Document> collection = database.getCollection("People");
        Random rd = new Random();
        List<Document> udocuments = getUDocs(rd);
        collection.insertMany(udocuments);
    }

    public void genGroupData() {
        MongoCollection<Document> collection = database.getCollection("Group");
        Random rd = new Random();
        List<Document> gdocuments = getGDocs(rd);
        collection.insertMany(gdocuments);
    }

    public void genRelData() {
        MongoCollection<Document> collection = database.getCollection("rel");
        Random random = new Random();
        List<Document> documents = getRelDocs(random);
        collection.insertMany(documents);
    }

    private HashMap<Integer, List<String>> getTags() {
        HashMap<Integer, List<String>> hashMap = new HashMap<>();
        Random rd = new Random();
        for (int i = 0; i < gnameArrayLen; i++) {
            hashMap.put(i, getTags(rd));
        }

        hashMap.forEach((k,v)->System.out.println(
                k+"|"+String.join("-",v)
        ));

        return hashMap;
    }

    private List<Document> getRelDocs(Random rd) {
        List<Document> documents = new ArrayList<>();
        HashMap<Integer, List<String>> hashMap = getTags();
        for (int i = 0; i < unameArrayLen; i++) {
            for (int j = 0; j < gnameArrayLen; j++) {
                Document doc = new Document("url", "/people/" + i)
                        .append("name", unameArray[i])
                        .append("gname", gnameArray[j])
                        .append("gurl", "/group/" + j)
                        .append("time", new Date(System.currentTimeMillis() - rd.nextInt(1000000)))
                        .append("tags", hashMap.get(j));
                documents.add(doc);
            }
        }
        return documents;
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
        for (int i = 0; i < unameArrayLen; i++) {
            Document udoc = new Document("url", "/people/" + i)
                    .append("name", unameArray[i]);

            int groupNum = rd.nextInt(gnameArrayLen - 1) + 1;
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

    public void delAll(String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.drop();
        System.out.println(database.getCollection(collectionName) == null ? "finish del collection " + collectionName : "del collection " + collectionName + " fail");
    }


    public void search(String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
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

        //ini connection
        mongoDataGen.ini();
        mongoDataGen.printColectionNames();

        //del collection
//        mongoDataGen.delAll("People");
//        mongoDataGen.delAll("Group");
//        mongoDataGen.delAll("UserPattern");
        mongoDataGen.delAll("rel");

        //list exist collection names
        mongoDataGen.printColectionNames();

        //generate data
//        mongoDataGen.genUserData();
//        mongoDataGen.genGroupData();
        mongoDataGen.genRelData();

        //search
//        mongoDataGen.search("People");
//        mongoDataGen.search("Group");
        mongoDataGen.search("rel");
    }

}
