package com.isc.poc.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class consumerdemo {
	public static void main(String args[])
	{
		final Logger logger=LoggerFactory.getLogger(producerdemo2_callback.class);
		Properties properites=new Properties();
		String bootstrapserver="127.0.0.1:9092";
		String groupid="Application2";
		properites.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
		properites.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properites.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properites.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
		properites.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		//create consumer
		KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properites);
		
		//subscribe consumer to topic
		consumer.subscribe(Collections.singleton("first_topic"));
		//Poll for new record
		while(true)
		{
			ConsumerRecords <String, String> records=consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String,String> record:records)
			{
				logger.info("key"+record.key()+ ",value"+record.value());
			}
		}
		
	}

}
