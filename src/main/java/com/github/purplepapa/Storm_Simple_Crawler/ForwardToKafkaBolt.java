package com.github.purplepapa.Storm_Simple_Crawler;

import java.util.Map;
import java.util.Properties;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ForwardToKafkaBolt extends BaseRichBolt {
	/**
         * 
         */
	private static final long serialVersionUID = 1L;
	private Producer<String, String> producer;
	private String zkConnect, serializerClass, topic;
	OutputCollector _collector;

	public ForwardToKafkaBolt(String zkConnect, String serializerClass,
			String topic) {
		this.zkConnect = zkConnect;
		this.serializerClass = serializerClass;
		this.topic = topic;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		Properties props = new Properties();
		props.put("metadata.broker.list", zkConnect);
		props.put("serializer.class", serializerClass);
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String msg = (String) input.getValue(0);
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(
				topic, msg);
		producer.send(data);
		System.out.println("forward2kafka:"+data);
		String host = "localhost";
		int port = 6379;
		String filterName = "urlbloomfilter";
		// Open a Redis-backed Bloom filter
		BloomFilter<String> bfr = new FilterBuilder(1000, 0.01)
				.name(filterName).redisBacked(true).redisHost(host)
				.redisPort(port).buildBloomFilter();

		if (!bfr.contains(msg)) {
			bfr.add(msg);
			System.out.println("FORWARD NOT DUP:" + msg);
		} else {
			System.out.println("FORWARD MAY DUP:" + msg);
		}
//		producer.close();
		_collector.ack(input);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	// emit tuples to kafka queue broker

}
