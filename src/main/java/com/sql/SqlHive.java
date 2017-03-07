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

public class SqlHive {

	public static class Record implements Serializable {
		private int key;
		private String value;

		public int getKey() {
			return key;
		}

		public void setKey(int key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	private static String FILEPATH = "e:/word.txt";

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		conf.setAppName("Java Spark SQL basic example").setMaster("local");

		String warehouseLocation = "spark-warehouse";

		SparkSession spark = SparkSession.builder()
				.appName("Java Spark Hive Example")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.enableHiveSupport().getOrCreate();

		spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
		spark.sql("LOAD DATA LOCAL INPATH 'e:/kv1.txt' INTO TABLE src");

		spark.sql("SELECT * FROM src").show();

		spark.stop();

	}
}