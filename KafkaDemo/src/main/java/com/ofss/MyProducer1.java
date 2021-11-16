package com.ofss;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer1 {

	public static void main(String[] args) {
		String topic_name = "test";
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); //Broker details
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Data type for keys
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Data type for values
		
		Producer<String, String> producer = new KafkaProducer<String, String>(props); // kafka API
		
		
		// Multiple messages
//		for(int i=0; i<100; i++) {
//			ProducerRecord<String, String> myMsg = new ProducerRecord<String, String>(topic_name, String.valueOf(i) , "Message No" +i);
//			producer.send(myMsg);
//		}
		
		Scanner sc = new Scanner(System.in);
		System.out.println("Please enter your messages & stop to end!!");
		String userInput = "";
		while(!userInput.equals("stop")) {
			userInput = sc.next();
			
			if (!userInput.equals("stop"))
			{
				ProducerRecord<String, String> myMsg=new ProducerRecord<String,String>(topic_name, null, userInput);
				producer.send(myMsg);
			}
		}
		
		System.out.println("All the 100 messages produced to the topic->" + topic_name);
		producer.close();
		// kafka-console-producer.bat --broker-list localhost:9092 --topic test
	}

}
