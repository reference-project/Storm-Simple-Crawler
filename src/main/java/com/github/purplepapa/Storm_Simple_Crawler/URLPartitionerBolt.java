package com.github.purplepapa.Storm_Simple_Crawler;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class URLPartitionerBolt extends BaseRichBolt {
	private OutputCollector collector;

	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
