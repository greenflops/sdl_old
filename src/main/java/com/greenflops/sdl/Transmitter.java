package com.greenflops.sdl;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Transmitter {
	
	private String bs = "";
	private String topic = "";
	private Producer<String, String> producer = null;
	
	public Transmitter(String servers, String channel){
		bs = servers;
		topic = channel;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", bs);
		props.put("acks", "0");
		props.put("compression.type", "none");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	    try {
	    	producer = new KafkaProducer<String, String>(props);
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	public void publish(String msg){
		producer.send(new ProducerRecord<String, String>(topic, msg));
	}
		
	public void set(String msg){
		publish(msg);
	}
	
}
