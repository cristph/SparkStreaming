package com.DaDaoYunJiSuan.SparkStreaming.spark;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import javax.swing.text.Document;
import java.io.Serializable;

/**
 * Created by Cristph on 2017/10/18.
 */
public class SparkStreaming implements Serializable {

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

    public void streaming() {
        JavaSparkContext javaSparkContext = getJsc("user");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(1));

        JavaDStream<String> dStream = javaStreamingContext.receiverStream(new JavaCustomReceiver("localhost", 1234));
    }

}
