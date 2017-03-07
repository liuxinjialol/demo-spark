package com.demo;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountByJava8 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setAppName("WordCounter").setMaster("local");

        String fileName = "e:/word.txt";

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(fileName, 1);

        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((e, acc) -> e + acc, 1)
                .map(e -> new Tuple2<>(e._1, e._2))
                .sortBy(e -> e._2, false, 1)
                .foreach(e -> {
                    System.out.println("【" + e._1 + "】出现了" + e._2 + "次");
                });
        sc.close();

    }
}