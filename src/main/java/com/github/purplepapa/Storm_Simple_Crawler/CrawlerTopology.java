package com.github.purplepapa.Storm_Simple_Crawler;

import java.util.UUID;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.github.purplepapa.Storm_Simple_Crawler.bolts.*;

public class CrawlerTopology {
	private static final String CRAWL_SPOUT_ID = "crawl-spout";
	private static final String URLDEDUP_BOLT_ID = "urldedup-spout";
	private static final String PARTITION_BOLT_ID = "partition-bolt";
	private static final String FETCH_BOLT_ID = "fetch-bolt";
	private static final String PARSE_BOLT_ID = "parse-bolt";
	private static final String TOPOLOGY_NAME = "crawl-topology";

	public static void main(String[] args)  throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		
		System.out.println("in m:");

		String topicName = "crawl1";

		BrokerHosts hosts = new ZkHosts("localhost:9092");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/"
				+ topicName, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		builder.setSpout(CRAWL_SPOUT_ID, kafkaSpout);
		// kafka crawl spout --> URLDeduplicatorBolt
		builder.setBolt(URLDEDUP_BOLT_ID, new URLDeduplicatorBolt())
				.shuffleGrouping(CRAWL_SPOUT_ID);
		// URLDedupliatorBolt --> URLPartitionerBolt
//		builder.setBolt(PARTITION_BOLT_ID, new URLPartitionerBolt())
//				.fieldsGrouping(URLDEDUP_BOLT_ID, new Fields("host"));
//		// URLPartitionerBolt --> SimpleFetcherBolt
//		builder.setBolt(FETCH_BOLT_ID, new SimpleFetcherBolt())
//				.shuffleGrouping(PARTITION_BOLT_ID);
//		// SimpleFetcherBolt --> ParserBolt
//		builder.setBolt(PARSE_BOLT_ID, new ParserBolt()).shuffleGrouping(
//				FETCH_BOLT_ID);


		Config config = new Config();
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config,
					builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology(args[0], config,
					builder.createTopology());
		}

	}

}
