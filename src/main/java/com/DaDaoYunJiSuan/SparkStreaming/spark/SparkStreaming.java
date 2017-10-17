package com.DaDaoYunJiSuan.SparkStreaming.spark;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.lang3.tuple.Pair;
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

    private void iniMongodbConnection() {
        if (mongoClient == null) {
            mongoClient = new MongoClient("localhost", 27017);
            database = mongoClient.getDatabase("test");
        }
    }

    public void generateUserPattern() {
//        JavaSparkContext groupJsc = getJsc("Group");
//        JavaMongoRDD<Document> groupRdd = MongoSpark.load(groupJsc);

        iniMongodbConnection();
        MongoCollection<Document> collection = database.getCollection("UserPattern");

        JavaSparkContext peopleJsc = getJsc("People");
        JavaMongoRDD<Document> peopleRdd = MongoSpark.load(peopleJsc);

        //构造mongodb数据流
        Document unwind = Document.parse("{$unwind:\"$join_groups\"}");
        Document sort = Document.parse("{$sort:{\"join_groups.time\":1}}");
        List<Document> operations = Arrays.asList(new Document[]{unwind, sort});
        JavaMongoRDD<Document> aggregatedRdd = peopleRdd.withPipeline(operations);

        //临时内存存储，后期存数据库
        Set<UserPattern> userPatternSet = new ConcurrentSet<>();

        //流计算得到实时更新的用户画像
        aggregatedRdd
                .map(document -> {
                    String uname = (String) document.get("name");
                    Document doc = collection.find(new Document("name", uname)).first();
                    if (doc != null) {
                        List<String> tags=(List<String>) doc.get("tags");
                        return new Document("exist", "-1")
                                .append("doc", document.put("tags",tags));
                    } else {
                        return new Document("exist", "1")
                                .append("doc", document);
                    }
                })
                .foreach(document -> collection.updateOne(eq("", ""), set("", "")));


        //检验数据流输出
        System.out.println("================================================================");
        userPatternSet.stream().forEach(userPattern -> System.out.println(userPattern.toString() + "\ns"));
        peopleJsc.close();
//        groupJsc.close();
    }

    private Document parseDocument(Document document){
        document.remove()
    }

    private void saveUserPattern(UserPattern userPattern) {

    }

    public static void main(String[] args) throws InterruptedException {
        SparkStreaming sparkStreaming = new SparkStreaming();
        sparkStreaming.generateUserPattern();
    }
}
