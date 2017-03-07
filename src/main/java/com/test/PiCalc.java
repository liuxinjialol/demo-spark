package com.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class PiCalc {
	
	private static int NUM_SAMPLES=10;

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("sparkChp1").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
		for (int i = 0; i < NUM_SAMPLES; i++) {
		  l.add(i);
		}

		@SuppressWarnings("serial")
		long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
		  public Boolean call(Integer i) {
		    double x = Math.random();
		    double y = Math.random();
		    System.out.println(x*x+y*y);
		    return x*x + y*y < 1;
		  }
		}).count();
		
		sc.close();
		
		System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
		
		
	}

}
