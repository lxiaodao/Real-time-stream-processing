/**
 * 
 */
package com.rooibook.sparkdemo;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author yang
 *
 */
public class RemotInvokeMain {
	
	//.master("spark://192.168.10.12:7077")
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] arr) {
		
		
		String hadoophome="C:/spark-2.4.4-bin-hadoop2.7";
		String jarpath="c:/workspaces/sparkdemo/target/sparkdemo-0.0.1-SNAPSHOT.jar";
		String filepath="C:/workspaces/sparkdemo/target/test.txt";
        if(arr!=null&&arr.length>=3) {
			
        	hadoophome=arr[0];
        	jarpath=arr[1];
        	filepath=arr[2];
		}
		
		//E:\hadoop\winutil
		//System.setProperty("hadoop.home.dir", hadoophome);
		
		SparkConf conf = new SparkConf().setAppName("MemberAccount").setMaster("spark://10.3.0.83:7077")
				.setJars(new String[]{jarpath});
		JavaSparkContext sparkContext=new JavaSparkContext(conf);
				
		//sparkContext.addFile("E:/stsworkspace/sparkdemo/target/test.txt");
		
		JavaRDD<String> lines = sparkContext.textFile(filepath);

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
	   
		
		System.out.println("------This is end------");
	}

}
