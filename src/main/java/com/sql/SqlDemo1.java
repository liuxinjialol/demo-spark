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

public class SqlDemo1 {
	
	
	public static class Person implements Serializable {
	    private String name;
	    private int age;
	    private String sex;

	    public String getName() {
	      return name;
	    }

	    public void setName(String name) {
	      this.name = name;
	    }

	    public int getAge() {
	      return age;
	    }

	    public void setAge(int age) {
	      this.age = age;
	    }
	    
	    public String getSex() {
		      return sex;
		}

		public void setSex(String sex) {
		      this.sex = sex;
		}
	    
	  }
	
	
	private static String FILEPATH="e:/people.json";
	

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		conf.setAppName("Java Spark SQL basic example").setMaster("local");

		SparkSession spark = SparkSession.builder()	.config(conf).getOrCreate();

		Dataset<Row> df = spark.read().json(FILEPATH);

//		df.show();
		
//		df.printSchema();
		
//		df.select("name").show();
		
//		df.select(col("name"), col("age").plus(1)).show();

//		df.filter(col("age").gt(21)).show();

//		df.groupBy("age").count().show();
		
//		df.groupBy("sex").count().show();

//		Dataset<Row> result=df.groupBy("sex").count();
		
//		df.createOrReplaceTempView("people");
//		Dataset<Row> sqlDF = spark.sql("SELECT sex,count(*) as sexSum FROM people group by sex");
//		sqlDF.show();
		
//		try {
//			df.createGlobalTempView("people");
//			
//			spark.sql("SELECT * FROM global_temp.people").show();
//			// Global temporary view is cross-session
//			spark.newSession().sql("SELECT * FROM global_temp.people").show();
//		} catch (AnalysisException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
//		Encoder<Person> personEncoder = Encoders.bean(Person.class);
//		Dataset<Person> peopleDS = spark.read().json(FILEPATH).as(personEncoder);
//		peopleDS.show();
//		peopleDS.groupBy("sex").count().show();
		

//		Dataset<Row> peopleDF = spark.createDataFrame(peopleDS, Person.class);
		
		df.createOrReplaceTempView("people");
		Dataset<Row> sqlDF = spark.sql("SELECT sex,count(*) as sexSum FROM people group by sex");
		sqlDF.write().save("e:/result.parquet");
		
		spark.stop();

	}
}