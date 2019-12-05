/**
 * 
 */
package com.saas.bigdata;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author yangliu
 *
 */
public class FlinkStreamHandler {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args)  {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	    Properties properties = new Properties();
	    //
	    properties.setProperty("bootstrap.servers", "10.3.0.83:9092,10.3.0.82:9092");
	    properties.setProperty("group.id", "fooGroup");
       
        properties.setProperty("auto.offset.reset", "earliest");//earliest latest
        properties.setProperty("enable.auto.commit", "false");
		
		FlinkKafkaConsumer consumer=new FlinkKafkaConsumer("orderRecord",new SimpleStringSchema(),properties);
		DataStreamSource<String> streamsource = env.addSource(consumer);
		DataStream<String> stream=streamsource.map((MapFunction<String, String>) mapper-> mapper);
		stream.print();
		
		// execute the transformation pipeline
		try {
			env.execute("=======Excute read from kafka==============");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
