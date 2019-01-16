package com.isc.poc.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class producerdemo2_callback {
	public static void main(String args[])
	{
		final Logger loggervar=LoggerFactory.getLogger(producerdemo2_callback.class);
		String bootstrapserver="127.0.0.1:9092";
		Properties properites=new Properties();
		properites.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
		properites.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properites.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		//to create a producer
		KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properites);
		//to create a producer record
		
		for(int i=0;i<10;i++){
			
		ProducerRecord<String,String> record=new ProducerRecord<String,String>("first_topic","hello_world"+Integer.toString(i));
		producer.send(record, new Callback()
		{
			public void onCompletion(RecordMetadata recordMetadata, Exception e)
			{
		//execute everytime a record is successfully sent.
				if (e!=null)
				{
				//error
				loggervar.error("error while producing",e);
				}
				else 
				{
				loggervar.info("received metadata. \n" +
					"Topic:"+recordMetadata.topic()+"\n" +
					 "Partition:"+ recordMetadata.partition());
				
				}
			}
		});
		}
		
		//flush and close
		producer.close();
	}

}
