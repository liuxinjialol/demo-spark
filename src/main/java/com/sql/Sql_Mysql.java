package com.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Sql_Mysql {

	private static Logger logger = Logger.getLogger(Sql_Mysql.class);

	public static void main(String[] args) {
		
		String table = "users";

		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Java Spark SQL basic example").setMaster("local");
		
		SparkSession sparkSession=null;
		
		try {

			// 一个条件表示一个分区
			String[] predicates = new String[] {  
					"1=1 order by id limit 1,20000",  
					"1=1 order by id limit 20000,40000",  
					"1=1 order by id limit 40000,60000",  
					"1=1 order by id limit 60000,80000",  
					"1=1 order by id limit 80000,100000",  
					"1=1 order by id limit 100000,120000",  
					"1=1 order by id limit 120000,999999" };  
			

			String url = "jdbc:mysql://10.216.38.52:3306/ngdb";
			
			Properties connectionProperties = new Properties();
			connectionProperties.setProperty("dbtable", table);// 设置表
			connectionProperties.setProperty("user", "root");// 设置用户名
			connectionProperties.setProperty("password", "root");// 设置密码
			
			sparkSession = SparkSession.builder()	.config(sparkConf).getOrCreate();
			
			//partitions=1
//			Dataset<Row> readDF=sparkSession.read().jdbc(url, table, connectionProperties);

			// 带条件的partitions>1
			Dataset<Row> readDF = sparkSession.read().jdbc(url, table,	predicates, connectionProperties);
			
			readDF.printSchema();
			readDF.show();
			
			
			//下面是写入的操作
			List<Users> userList=new ArrayList<Users>();
			for(int i=0;i<100000;i++){
				Users u=new Users();
				u.setAccount(UUID.randomUUID().toString());
				u.setPassword(System.currentTimeMillis()+"");
				u.setName(UUID.randomUUID().toString().substring(0, 4));
				userList.add(u);
			}
			Encoder<Users> userEncoder = Encoders.bean(Users.class);
			Dataset<Users> writeDF = sparkSession.createDataset(userList, userEncoder);
			
			//也可以从json加载
//			Dataset<Row> writeDF=  sparkSession.read().json("e:/users.json");
			
			// 写入数据
			// SaveMode.Append表示添加的模式
			// SaveMode.Append:在数据源后添加；
			// SaveMode.Overwrite:如果如果数据源已经存在记录，则覆盖；
			// SaveMode.ErrorIfExists:如果如果数据源已经存在记录，则包异常；
			// SaveMode.Ignore:如果如果数据源已经存在记录，则忽略；
//			writeDF.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
			
			System.out.println("total num is "+readDF.count());
			
			readDF.groupBy("name").count().orderBy(col("count").desc()).show(50);
			
			System.out.println(readDF.rdd().partitions().length);
			 
		} catch (Exception e) {
			logger.error("|main|exception error", e);
		} finally {
			if (sparkSession != null) {
				sparkSession.stop();
			}

		}

	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static class Users implements Serializable {
		
	    private String name;
	    private String  account;
	    private String password;

	    public String getName() {
	      return name;
	    }

	    public void setName(String name) {
	      this.name = name;
	    }

		public String getAccount() {
			return account;
		}

		public void setAccount(String account) {
			this.account = account;
		}

		public String getPassword() {
			return password;
		}

		public void setPassword(String password) {
			this.password = password;
		}
	    
	  }
	
	
	
	
	
	
}