package com.DaDaoYunJiSuan.SparkStreaming.spark;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import io.netty.util.internal.ConcurrentSet;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.*;


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

        Document match=Document.parse("");

        //构造mongodb数据流
        Document unwind=Document.parse("{$unwind:\"$join_groups\"}");
        Document sort=Document.parse("{$sort:{\"join_groups.time\":1}}");
        List<Document> operations=Arrays.asList(new Document[]{unwind,sort});
        JavaMongoRDD<Document> aggregatedRdd=rdd.withPipeline(operations);

        //临时内存存储，后期存数据库
        Set<UserPattern> userPatternSet=new ConcurrentSet<>();

        //流计算得到实时更新的用户画像
        aggregatedRdd.foreach(document -> {
            UserPattern userPattern=new UserPattern(document);
            System.out.println(userPattern.toString());
            userPatternSet.add(userPattern);
        });

        //检验数据流输出
        System.out.println("================================================================");
        userPatternSet.stream().forEach(userPattern -> System.out.println(userPattern.toString()+"\ns"));
        jsc.close();
    }
}
