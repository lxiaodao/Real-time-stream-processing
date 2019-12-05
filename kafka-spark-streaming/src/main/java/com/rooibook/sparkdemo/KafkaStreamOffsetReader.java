package com.rooibook.sparkdemo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class KafkaStreamOffsetReader {
public static void main(String[] arr) throws InterruptedException {
	
		  SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReader"); 
	        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
	        

	        Map<String, Object> kafkaParams = new HashMap<>();
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
	        
		
		/*
		 * stream.foreachRDD(rdd -> { OffsetRange[] offsetRanges = ((HasOffsetRanges)
		 * rdd.rdd()).offsetRanges(); rdd.foreachPartition(consumerRecords -> {
		 * OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
		 * System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() +
		 * " " + o.untilOffset());
		 * 
		 * });
		 * 
		 * });
		 */
		 
	        stream.foreachRDD(rdd -> {
	        	  OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
	        	  
	        	  rdd.foreach(record -> {
	        		   OffsetRange o = offsetRanges[record.partition()];
	        		   System.out.println("---------------------------");
		        	    System.out.println(
		        	      o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
		              System.out.println("Record Key " + record.key());
		              System.out.println("Record value " + record.value());
		              System.out.println("Record partition " + record.partition());
		              System.out.println("Record offset " + record.offset());
		              
		           });

	        	  // some time later, after outputs have completed
	        	  ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
	        	});
 
		    jssc.start();
		    jssc.awaitTermination();
		
		
	}

}
