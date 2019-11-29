/**
 * 
 */
package com.rooibook.sparkdemo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.junit.Assert;

/**
 * @author yangliu
 *
 */
public class KafkaStreamOffsetTest implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5038450627124775507L;

	private final Logger logger = LoggerFactory.getLogger(KafkaStreamOffsetTest.class);
	
	public static final String TOPIC_DEFAULT="orderRecord";//devlog
	
	private JavaInputDStream<ConsumerRecord<String, String>> stream=null;
	
	private JavaStreamingContext jssc=null;
	
	//private Map<Integer,Long> partition_offset_map=new ConcurrentHashMap<Integer,Long>();
	
	private final int test_num=12;
	
	@Before
	public void init() {
		
      
		
		//initialize some message in kafka     
	    init_data_in_kafka(test_num);
		this.logger.info("=======初始化完毕=======");
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReader"); 
		conf.set("spark.streaming.kafka.allowNonConsecutiveOffsets", "true");
        jssc = new JavaStreamingContext(conf, Durations.seconds(5));
       
        
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.3.0.83:9092,10.3.0.82:9092,10.3.0.84:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "fooGroup");
        kafkaParams.put("isolation.level", "read_committed");
        kafkaParams.put("auto.offset.reset", "earliest");//earliest latest
        kafkaParams.put("enable.auto.commit", false);


        Collection<String> topics =Arrays.asList(TOPIC_DEFAULT);
        stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
      
	
		
	}
	/**
	 * Add transactinal to producer
	 * @param number
	 */
	private void init_data_in_kafka(int number) {
		KafkaProducer producer=createProducer();
	    //producer.initTransactions();
		number=number<=0?10:number;
		//producer.beginTransaction();
		 for(int i=0;i<number;i++) {
			 ProducerRecord record=new ProducerRecord(TOPIC_DEFAULT,"this is order for some product"+i);
			// producer.send(record);
			 
			 RecordMetadata metadata;
			try {
				metadata = (RecordMetadata) producer.send(record).get();
				  System.out.println("Record sent with key " + i + " to partition " + metadata.partition()
	              + " with offset " + metadata.offset());
				  //For principle, record the max offset of message.
				  /**
				  int partition_id=metadata.partition();
			      if(partition_offset_map.containsKey(partition_id)) {
			    	 Long old_value=partition_offset_map.get(partition_id);
			    	 if( metadata.offset()> old_value.longValue()) {
			    		 partition_offset_map.put(partition_id, metadata.offset());
			    	 }
				     
			      }else {
			    	  partition_offset_map.put(partition_id, metadata.offset());
			      }
			      */
				  
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		 }
	   //producer.commitTransaction();
	   
	   try {
		Thread.sleep(2000);
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		  
	  producer.close();
		 
	}
	private KafkaProducer createProducer() {
		   Map<String, Object> kafkaParams = new HashMap<>();
	        // - 10.3.0.83:9092
	       // - 10.3.0.82:9092
	       // - 10.3.0.84:9092
	        kafkaParams.put("bootstrap.servers", "10.3.0.83:9092,10.3.0.82:9092,10.3.0.84:9092");
	        kafkaParams.put("key.serializer", StringSerializer.class);
	        kafkaParams.put("value.serializer", StringSerializer.class);
	        //kafkaParams.put("group.id", "fooGroup");
	        //kafkaParams.put("enable.idempotence", "true");
	       // kafkaParams.put("transactional.id", "order-produce-001");
	        kafkaParams.put("auto.offset.reset", "latest");
	        kafkaParams.put("enable.auto.commit", false);
	        KafkaProducer producer=new KafkaProducer<>(kafkaParams);
	    
	       return producer;
		
	}
	@After
	public void destroy() {
		
		
		if(null!=this.jssc) {
			this.jssc.close();//close spark stream context
		}
		//Reset to zero of counter
		CountPartiton.resetCount(0);
	
		
	}

	
	public void test_read_exact_offset_in_kafka() {
		
				
		  
		   stream.foreachRDD(rdd -> {
	        	  OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
	        	
	        	
			/*
			 * for(OffsetRange off:offsetRanges) { logger.info("===="+off.topic() + " " +
			 * off.partition() + " " + off.fromOffset() + " " + off.untilOffset()); //Offset
			 * should equal
			 * 
			 * }
			 */
	        	  
	        	  
	        	  rdd.foreach(record -> {
	        		   OffsetRange o = offsetRanges[record.partition()];
	        	
	        		   System.out.println("---------------------------");
		        	    System.out.println(
		        	      o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
		              System.out.println("Record Key " + record.key());
		              System.out.println("Record value " + record.value());
		              System.out.println("Record partition " + record.partition());
		              System.out.println("Record offset " + record.offset());
		              
		           
		              //----==Handle business logic,such as caculate
		              
	        	  });
	        	  
	        	
	        	  // some time later, after outputs have completed
	        	  ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
	        	  
	        
	        	  this.logger.info("=======loop over=======");
	        	  //
	        	 
	        	  
	        
	        	});
            
            
             jssc.start();
     	    try {
     	    	jssc.awaitTermination();
     			//jssc.awaitTerminationOrTimeout(50000);
     		} catch (InterruptedException e) {
     			
     			e.printStackTrace();
     		}
     	   
		    
	}
	
	
	@Test
	public void test_loop_partition() {
		
		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			
			
			
			
			rdd.foreachPartition(partition -> {
				
				//OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
				//System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
				CountPartiton countservice=CountPartiton.getInstance();
				while(partition.hasNext()) {
                	
                	ConsumerRecord record=partition.next();
                
                	 System.out.println("======Record========" + record.key()+",===="+record.value());
                	 countservice.count();
                }
				
				
			});
			
			  // some time later, after outputs have completed
      	  ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
      	  
         
      	  this.logger.info("=======loop over======="+CountPartiton.getCountAll());
     
		});
		
		  
        jssc.start();
	    try {
	    	//jssc.awaitTermination();
			jssc.awaitTerminationOrTimeout(50000);
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	    this.logger.info("=======will exit stream======="+CountPartiton.getCountAll());
	    Assert.assertEquals(test_num, CountPartiton.getCountAll());
		 
	}


	

}
