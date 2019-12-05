/**
 * 
 */
package com.rooibook.sparkdemo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * @author yangliu
 *
 */
public class KafkaReader {
	
	public static void main(String[] arr) throws InterruptedException {
		
		//System.setProperty("hadoop.home.dir", "C:/spark-2.4.4-bin-hadoop2.7");
		  //SparkConf conf = new SparkConf().setAppName("KafkaReader");
		  SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReader"); 
	        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
	        

	        Map<String, Object> kafkaParams = new HashMap<>();
	        // - 10.3.0.83:9092
	       // - 10.3.0.82:9092
	       // - 10.3.0.84:9092
	        kafkaParams.put("bootstrap.servers", "10.3.0.83:9092,10.3.0.82:9092,10.3.0.84:9092");
	        kafkaParams.put("key.deserializer", StringDeserializer.class);
	        kafkaParams.put("value.deserializer", StringDeserializer.class);
	        kafkaParams.put("group.id", "fooGroup");
	        kafkaParams.put("auto.offset.reset", "earliest");//earliest latest
	        kafkaParams.put("enable.auto.commit", false);


	        Collection<String> topics =Arrays.asList("devlog");
	        JavaInputDStream<ConsumerRecord<String, String>> stream =
	                KafkaUtils.createDirectStream(
	                        jssc,
	                        LocationStrategies.PreferConsistent(),
	                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
	                );
		  // JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream();
	        JavaPairDStream<String, String> jPairDStream =  stream.mapToPair(
	                new PairFunction<ConsumerRecord<String, String>, String, String>() {
	                    @Override
	                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
	                        return new Tuple2<>(record.key(), record.value());
	                    }
	                });
	        System.out.println("=============READ FROM KAFKA===================================");
	        jPairDStream.foreachRDD(jPairRDD -> {
	               jPairRDD.foreach(rdd -> {
	            	   
	                    System.out.println("key="+rdd._1()+" value="+rdd._2());
	                });
	            }); 
 
		    jssc.start();
		    jssc.awaitTermination();
		
		
	}

}
