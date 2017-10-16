package com.DaDaoYunJiSuan.SparkStreaming.spark;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * Created by Cristph on 2017/10/16.
 */
public class GettingStarted {

    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/test.testCollection")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/test.testCollection")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        List<UserPattern> userPatterns=new ArrayList<>();

        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(Document.parse("{$unwind:\"$join_groups\"},{$sort:{\"join_groups.time\":-1}}"))
        );
        aggregatedRdd.map(document -> (String)document.get("join_groups.time")).collect();
        jsc.close();
    }
}
