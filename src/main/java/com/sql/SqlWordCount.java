package com.sql;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class SqlWordCount {
	
	private static String FILEPATH="e:/word.txt";
	

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		conf.setAppName("Java Spark SQL basic example").setMaster("local");

		SparkSession spark = SparkSession.builder()	.config(conf).getOrCreate();

		Dataset<Row> df = spark.read().text(FILEPATH);

//		df.show();
		
		df.createOrReplaceTempView("word");
		Dataset<Row> sqlDF = spark.sql("SELECT value,count(*) as wordSum FROM word group by value order by wordSum desc");
		sqlDF.show();

		
		spark.stop();

	}
}