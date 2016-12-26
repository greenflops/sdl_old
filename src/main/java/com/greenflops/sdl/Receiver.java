package com.greenflops.sdl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Receiver {
	private String bs = "";
	private String topic = "";
	private KafkaConsumer<String, String> consumer = null;
	private ObjectMapper mapper = null;
	
	public Receiver(String servers, String channel){
		bs = servers;
		topic = channel;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", bs);
	    props.put("group.id", "group-1");
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("auto.offset.reset", "earliest");
	    props.put("session.timeout.ms", "30000");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

	    try {
	    	consumer = new KafkaConsumer<String, String>(props);
	    	mapper = new ObjectMapper();
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	public String subscribe(){
		consumer.subscribe(Arrays.asList(topic));
	    while (true) {
	        ConsumerRecords<String, String> records = consumer.poll(100);
	            for (ConsumerRecord<String, String> record : records) {
	              JsonNode msg = null;
				try {
					msg = mapper.readTree(record.value());
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				String data = msg.get("type").asText();
	              switch (data) {
                  case "table1":
                  case "table2":
                	  return data;
                  default:
                      throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
	              }
	          }
	      }
	}

	public String get(String table){
		return "toto";
	}

}
