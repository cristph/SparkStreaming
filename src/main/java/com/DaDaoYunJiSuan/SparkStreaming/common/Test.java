package com.DaDaoYunJiSuan.SparkStreaming.common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Created by Cristph on 2017/10/15.
 */
public class Test {

    /**
     * Set a name for your application.
     */
    private final static String appName = "test";

    /**
     * The master URL to connect to,
     * such as "local" to run locally with one thread,
     * "local[4]" to run locally with 4 cores,
     * or "spark://master:7077" to run on a Spark standalone cluster,
     * or "yarn"
     * <p>
     * more see here: http://spark.apache.org/docs/2.2.0/submitting-applications.html#master-urls
     */
    private final static String master = "yarn";

    /**
     * save logs
     */
    private final static String logPath = "/tmp/catlog.log";

    private void ini() {

        //ini data
        List<Integer> data = new Random()
                .ints(100)
                .boxed()
                .collect(Collectors.toList());

        //write file
        Path path = Paths.get(logPath);
        long a=0;
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            a = System.currentTimeMillis();
            bufferedWriter.write("dataSize:"+data.size());
            bufferedWriter.write("start at:"+a + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

        //ini spark conf and context
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //calculate
        LongAccumulator longAccumulator = jsc.sc().longAccumulator();
        jsc.parallelize(data).foreach(x -> longAccumulator.add(x));

        //write result
        long b;
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            b=System.currentTimeMillis();
            bufferedWriter.write("end at:"+b + "\n");
            bufferedWriter.write("use time:"+(b-a)/1000+"s\n");
            bufferedWriter.write("result"+String.valueOf(longAccumulator.value()) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Test test = new Test();
        test.ini();
    }

}
