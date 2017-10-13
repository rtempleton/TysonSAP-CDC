package com.github.rtempleton.tysonsappoc;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rtempleton.poncho.StormUtils;
import com.github.rtempleton.poncho.io.PhoenixUpsertBolt;
import com.github.rtempleton.poncho.io.SimpleHDFSWriter;

public class SAPCDCTopology {
	
	private static final Logger Log = LoggerFactory.getLogger(SAPCDCTopology.class);
	private static final String thisName = SAPCDCTopology.class.getSimpleName();
	private final Properties topologyConfig;

	public SAPCDCTopology(String properties) {
		topologyConfig = StormUtils.readProperties(properties);
	}
	
	
	public static void main(String[] args) throws Exception{
		SAPCDCTopology topology = new SAPCDCTopology(args[0]);
		topology.submit(topology.compose());
	}
	
	@SuppressWarnings("unchecked")
	protected TopologyBuilder compose() throws Exception{
		
		TopologyBuilder builder = new TopologyBuilder();
		
		final String bootStrapServers = StormUtils.getRequiredProperty(topologyConfig, "kafka.bootStrapServers");
		final String topic = StormUtils.getRequiredProperty(topologyConfig, "kafka.topic");
		final String consumerGroupId = StormUtils.getRequiredProperty(topologyConfig, "kafka.consumerGroupId");
		
		
		@SuppressWarnings("rawtypes")
		KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder(bootStrapServers, topic)
		        .setGroupId(consumerGroupId)
		        .setOffsetCommitPeriodMs(10_000)
		        .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
		        .setMaxUncommittedOffsets(1000000)
		        .setSecurityProtocol("SASL_PLAINTEXT")
		        .setRetry(new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
		                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10)))
		        .build();
		
		// 1. spout reads data
		KafkaSpout<String, String> spout = new KafkaSpout<String, String>(spoutConf);
		builder.setSpout("spout", spout, 1);
		
		
		//2. bolt to detect and route records based on the originating table
		RecordRouter router = new RecordRouter(topologyConfig, StormUtils.getOutputfields(spout));
		builder.setBolt("router", router).localOrShuffleGrouping("spout");
		
		
		//3. fan out parser bolts for each record type as determined by step 2
		RecordParser EKKOParser = new RecordParser(topologyConfig, "EKKO");
		builder.setBolt("EKKOParser", EKKOParser).localOrShuffleGrouping("router", "EKKO");
		
		RecordParser EKPOParser = new RecordParser(topologyConfig, "EKPO");
		builder.setBolt("EKPOParser", EKPOParser).localOrShuffleGrouping("router", "EKPO");
		
		RecordParser EKETParser = new RecordParser(topologyConfig, "EKET");
		builder.setBolt("EKETParser", EKETParser).localOrShuffleGrouping("router", "EKET");
		
		RecordParser EKPVParser = new RecordParser(topologyConfig, "EKPV");
		builder.setBolt("EKPVParser", EKPVParser).localOrShuffleGrouping("router", "EKPV");
		
		RecordParser EKBEParser = new RecordParser(topologyConfig, "EKBE");
		builder.setBolt("EKBEParser", EKBEParser).localOrShuffleGrouping("router", "EKBE");
		
		
		//4. Phoenix upsert bolts take parser records from their respsective parser to insert records into the appropriate table
		PhoenixUpsertBolt EKKOupsert = new PhoenixUpsertBolt(topologyConfig, "EKKO", StormUtils.getOutputfields(EKKOParser));
		builder.setBolt("EKKOupsert", EKKOupsert).localOrShuffleGrouping("EKKOParser");
		
		PhoenixUpsertBolt EKPOupsert = new PhoenixUpsertBolt(topologyConfig, "EKPO", StormUtils.getOutputfields(EKPOParser));
		builder.setBolt("EKPOupsert", EKPOupsert).localOrShuffleGrouping("EKPOParser");
		
		PhoenixUpsertBolt EKETupsert = new PhoenixUpsertBolt(topologyConfig, "EKET", StormUtils.getOutputfields(EKETParser));
		builder.setBolt("EKETupsert", EKETupsert).localOrShuffleGrouping("EKETParser");
		
		PhoenixUpsertBolt EKPVupsert = new PhoenixUpsertBolt(topologyConfig, "EKPV", StormUtils.getOutputfields(EKPVParser));
		builder.setBolt("EKPVupsert", EKPVupsert).localOrShuffleGrouping("EKPVParser");
		
		PhoenixUpsertBolt EKBEupsert = new PhoenixUpsertBolt(topologyConfig, "EKBE", StormUtils.getOutputfields(EKBEParser));
		builder.setBolt("EKBEupsert", EKBEupsert).localOrShuffleGrouping("EKBEParser");
		
		
		//5. Fork from upserts collect all the "failed" upsert records and send them to HDFS
		if(topologyConfig.getProperty("security.principal")!=null && topologyConfig.getProperty("security.keytab")!=null) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("hdfs.keytab.file",topologyConfig.getProperty("security.keytab"));
			map.put("hdfs.kerberos.principal",topologyConfig.getProperty("security.principal"));
			topologyConfig.put("hdfs.config", map);
		}
		SimpleHDFSWriter failedWriter = new SimpleHDFSWriter(topologyConfig, null);
		builder.setBolt("failedWriter", failedWriter)
			.localOrShuffleGrouping("EKKOupsert", "failed")
			.localOrShuffleGrouping("EKPOupsert", "failed")
			.localOrShuffleGrouping("EKETupsert", "failed")
			.localOrShuffleGrouping("EKPVupsert", "failed")
			.localOrShuffleGrouping("EKBEupsert", "failed");
		
		
		//6. windowing bolt to get distinct key from step 4 records successfully inserted
		String secs = StormUtils.getRequiredProperty(topologyConfig, "distinctKey.windowSecs");
		BaseWindowedBolt distinctKey = new DistinctKeyBolt(topologyConfig)
				.withTumblingWindow(Duration.seconds(Integer.parseInt(secs)));
		builder.setBolt("distinctKey", distinctKey)
			.localOrShuffleGrouping("EKKOupsert")
			.localOrShuffleGrouping("EKPOupsert")
			.localOrShuffleGrouping("EKETupsert")
			.localOrShuffleGrouping("EKPVupsert")
			.localOrShuffleGrouping("EKBEupsert");
		
		
		//7. SQL bolt to execute golden record query using Key from step 6
		IRichBolt goldenRec = new GenGoldenRecord(topologyConfig, "GOLDREC");
		builder.setBolt("goldenRec", goldenRec).localOrShuffleGrouping("distinctKey");
		
		
//		IRichBolt logger = new ConsoleLoggerBolt(topologyConfig, StormUtils.getOutputfields(distinctKey));
//		builder.setBolt("consoleLogger", logger)
//		.localOrShuffleGrouping("EKPOParser");
//		.localOrShuffleGrouping("EKKOupsert")
//		.localOrShuffleGrouping("EKPOupsert");
//		.localOrShuffleGrouping("EKETupsert")
//		.localOrShuffleGrouping("EKPVupsert")
//		.localOrShuffleGrouping("EKBEupsert");
		
		
		
		return builder;
	}
	
	protected void submit(TopologyBuilder builder){

		try {
			StormSubmitter.submitTopology(thisName, topologyConfig, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (AuthorizationException e) {
			e.printStackTrace();
			System.exit(-1);	
		}

	}

}
