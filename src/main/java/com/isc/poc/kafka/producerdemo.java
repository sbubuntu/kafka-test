package com.isc.poc.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class producerdemo {
	public static void main(String args[])
	{
		String bootstrapserver="127.0.0.1:9092";
		Properties properites=new Properties();
		properites.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
		properites.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properites.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		//to create a producer
		KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properites);
		//to create a producer record
		ProducerRecord<String,String> record=new ProducerRecord<String,String>("first_topic","hello_world");
		producer.send(record);
		//flush and close
		producer.close();
	}

}
