package com.github.purplepapa.Storm_Simple_Crawler;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;


public class URLDeduplicatorBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4971329030781055622L;
	private OutputCollector collector;

	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String input = (String) tuple.getValue(0);
		System.out.println("in url dedup:" + input);
		String host = "localhost";
		int port = 6379;
		String filterName = "urlbloomfilter";
		// Open a Redis-backed Bloom filter
		BloomFilter<String> bfr = new FilterBuilder(1000, 0.01)
				.name(filterName).redisBacked(true).redisHost(host)
				.redisPort(port).buildBloomFilter();

		if (!bfr.contains(input)) {
			bfr.add(input);
			System.out.println("NOT DUP:" + input);
		} else {
			System.out.println("MAY DUP:" + input);
		}

		collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("word", "count"));
	}

}
