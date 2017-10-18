package com.DaDaoYunJiSuan.SparkStreaming.spark;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


/**
 * Created by Cristph on 2017/10/16.
 */
public class SparkRDD implements Serializable {

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
        System.out.println("===================in method=======================");
        JavaSparkContext peopleJsc = getJsc("rel");
        JavaMongoRDD<Document> peopleRdd = MongoSpark.load(peopleJsc);
        System.out.println("==============" + peopleRdd == null ? "rdd null" : "rdd not null" + "==================");
        System.out.println("================" + peopleRdd.first() == null ? "null" : peopleRdd.first().toJson() + "==================");

        //构造mongodb数据流
//        Document sort = Document.parse("{$sort:{\"time\":1}}");
//        List<Document> operations = Arrays.asList(new Document[]{matchAll, sort});
        JavaMongoRDD<Document> aggregatedRdd = peopleRdd.withPipeline(
                Collections.singletonList(Document.parse("{$sort:{\"time\":1}}"))
        );


        System.out.println("================" + aggregatedRdd.first() == null ? "null" : aggregatedRdd.first().toJson() + "==================");

        //流计算得到实时更新的用户画像
        JavaPairRDD<String, Document> pairRDD = aggregatedRdd.mapToPair(doc ->
                new Tuple2(doc.get("name"), doc));
        JavaPairRDD<String, Document> result = pairRDD.reduceByKey((a, b) -> {
                    List<String> source = (List<String>) a.get("tags");
                    List<String> tmp = (List<String>) b.get("tags");
                    tmp.forEach(s -> {
                        if (!tmp.contains(s)) {
                            source.add(s);
                        }
                    });
                    a.replace("tas", source);
                    return a;
                }
        );
        result.filter(doc ->
                ((String) doc._2().get("name")).endsWith("5"))
                .foreach(doc -> System.out.println(doc._2().toJson()));

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
        SparkRDD sparkStreaming = new SparkRDD();
        sparkStreaming.generateUserPattern();
    }
}
