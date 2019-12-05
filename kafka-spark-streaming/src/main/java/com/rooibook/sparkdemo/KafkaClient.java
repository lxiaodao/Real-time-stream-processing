package com.rooibook.sparkdemo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaClient {
	
	public static final String TOPIC_DEFAULT="orderRecord";//devlog
	
	public static void main(String[] arr)  {
		
		excuteProduce();
		//excuteConsume();
		
	}
	
	public static void excuteProduce() {
		KafkaProducer producer=KafkaClient.createProducer();
		Random rd=new Random();
		 for(int i=0;i<12;i++) {
			 ProducerRecord record=new ProducerRecord(TOPIC_DEFAULT,"thoooooor some product"+i);
			// producer.send(record);
			 
			 RecordMetadata metadata;
			try {
				metadata = (RecordMetadata) producer.send(record).get();
				  System.out.println("Record sent with key " + i + " to partition " + metadata.partition()
	              + " with offset " + metadata.offset());
			} catch (ExecutionException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		 }
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void excuteConsume() {
		  Consumer<String, String> consumer = KafkaClient.createConsumer();
	        int noMessageFound = 0;
	        while (true) {
	        	
	          ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(5));
	          // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
	          if (consumerRecords.count() == 0) {
	              noMessageFound++;
	              if (noMessageFound > 30)
	                // If no message found count is reached to threshold exit loop.  
	                break;
	              else
	                  continue;
	          }
	          //print each record. 
	          consumerRecords.forEach(record -> {
	              System.out.println("Record Key " + record.key());
	              System.out.println("Record value " + record.value());
	              System.out.println("Record partition " + record.partition());
	              System.out.println("Record offset " + record.offset());
	           });
	          // commits the offset of record to broker. 
	           consumer.commitAsync();
	        }
	    consumer.close();
	}
	private static final Collection<String> topics =Arrays.asList(TOPIC_DEFAULT);
	
	public static KafkaProducer createProducer() {
		   Map<String, Object> kafkaParams = new HashMap<>();
	        // - 10.3.0.83:9092
	       // - 10.3.0.82:9092
	       // - 10.3.0.84:9092
	        kafkaParams.put("bootstrap.servers", "10.3.0.83:9092,10.3.0.82:9092,10.3.0.84:9092");
	        kafkaParams.put("key.serializer", StringSerializer.class);
	        kafkaParams.put("value.serializer", StringSerializer.class);
	        kafkaParams.put("group.id", "fooGroup");
	        kafkaParams.put("auto.offset.reset", "latest");
	        kafkaParams.put("enable.auto.commit", false);
	        KafkaProducer producer=new KafkaProducer<>(kafkaParams);
	    
	       return producer;
		
	}
	
	public static KafkaConsumer createConsumer() {
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
	        KafkaConsumer consumer=new KafkaConsumer(kafkaParams);
	        consumer.subscribe(topics);
	        return consumer;
	}

}
