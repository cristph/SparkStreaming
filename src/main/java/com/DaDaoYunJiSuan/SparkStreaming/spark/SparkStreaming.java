package com.DaDaoYunJiSuan.SparkStreaming.spark;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;


/**
 * Created by Cristph on 2017/10/16.
 */
public class SparkStreaming {

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private JavaSparkContext getJsc(String collectionName) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/test." + collectionName)
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/test." + collectionName)
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        return jsc;
    }

    private MongoCollection<Document> getMongodbCollection(String dbName, String collectionName) {
        if (mongoClient == null) {
            mongoClient = new MongoClient("localhost", 27017);
        }
        database = mongoClient.getDatabase(dbName);
        return database.getCollection(collectionName);
    }

    public void generateUserPattern() {

        MongoCollection<Document> collection = getMongodbCollection("test", "UserPattern");
        JavaSparkContext peopleJsc = getJsc("People");
        JavaMongoRDD<Document> peopleRdd = MongoSpark.load(peopleJsc);

        //构造mongodb数据流
        Document unwind = Document.parse("{$unwind:\"$join_groups\"}");
        Document sort = Document.parse("{$sort:{\"join_groups.time\":1}}");
        List<Document> operations = Arrays.asList(new Document[]{unwind, sort});
        JavaMongoRDD<Document> aggregatedRdd = peopleRdd.withPipeline(operations);

        //流计算得到实时更新的用户画像
        aggregatedRdd
                .map(document -> {
                    String uname = (String) document.get("name");
                    Document doc = collection.find(new Document("name", uname)).first();

                    String gname = (String) document.get("ganme");
                    List<String> newTags = (List<String>) collection.find(eq("gname", gname)).first().get("tags");
                    if (doc != null) {
                        List<String> tags = (List<String>) doc.get("tags");
                        mergeTags(tags, newTags);
                        return new Document("exist", "-1")
                                .append("tags", tags);
                    } else {
                        return new Document("exist", "1")
                                .append("doc", document.put("tags", newTags));
                    }
                })
                .foreach(document -> {
                    if ((int) document.get("exist") == 1) {
                        collection.insertOne((Document) document.get("doc"));
                    } else {
                        collection.updateOne(eq("name", document.get("name")), set("tags", document.get("tags")));
                    }
                });

        //检验数据流输出

        peopleJsc.close();
    }

    private List<String> mergeTags(List<String> source, List<String> items) {
        items.forEach(tag -> {
            if (!source.contains(tag)) {
                source.add(tag);
            }
        });
        return source;
    }

    public static void main(String[] args) throws InterruptedException {
        SparkStreaming sparkStreaming = new SparkStreaming();
        sparkStreaming.generateUserPattern();
    }
}
