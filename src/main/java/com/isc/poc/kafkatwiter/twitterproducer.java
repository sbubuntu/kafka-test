package com.isc.poc.kafkatwiter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class twitterproducer {
	public twitterproducer()
	{}
	
	public static void main(String[] args)
	{
	new twitterproducer.run();
	
		
	}
	public void run()
	{
				//twitter client
		// Attempts to establish a connection.
		Client client=twitterclient();
		client.connect();
				//create kafka producer
				//loop to send  tweets to kafka
	}
	
	String consumerKey="RDcO1RkxiW6IHDGedLiMXRVQY";
	String consumerSecret="LuGPxJO6o5v78j7JIIP63G9AwxrMqvrkLmiN7Lj1YeFDgfqhKM";
	String token="91750871-bnl7sdjF4vYW9JGWLjs6iUXTJLtntguuZy13UPClo";
	String secret="XHbs6ARFmlLvTZpkVCVKiGpLC02op8CPWTGpMIMjqNGpP";
	
	public Client twitterclient()
	{
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("kafka");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret,token,secret);
	    
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));                         // optional: use this if you want to process client events

				Client hosebirdClient = builder.build();
				return hosebirdClient;
				
	}
}
