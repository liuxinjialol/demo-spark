package com.test;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkChp1 {
	
	class GetLength implements Function<String, Integer> {
		  public Integer call(String s) { return s.length(); }
	}
	
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("sparkChp1").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("e:/data.txt");
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		System.out.println(totalLength);
		
//		List<Integer> list=Arrays.asList(1,2,3,4,5);
//		Long total=sc.parallelize(list).count();
//		System.out.println("totol="+total);
//		Long s=sc.textFile("e:/word.txt").count();
//		System.out.println("s="+s);
		

		
		sc.close();
		
	}

}
