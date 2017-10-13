package com.github.rtempleton.tysonsappoc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Test;

import com.github.rtempleton.poncho.test.AbstractTestCase;


public class TestTysonPOC extends AbstractTestCase {
	
	private final String TOPOLOGY_CONFIG = getResourcePath("config.properties");

	@Test
	public void topologyTest() throws Exception{
		
		SAPCDCTopology topo = new SAPCDCTopology(TOPOLOGY_CONFIG);
		TopologyBuilder builder = topo.compose();
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		
		cluster.submitTopology("topology", conf, builder.createTopology());
		
		int seconds = 120;
		Thread.sleep(seconds*1000);
		
		KillOptions ko = new KillOptions();
		ko.set_wait_secs(10);
		cluster.killTopologyWithOpts("topology", ko);
		
		Thread.sleep(seconds*1000);
		
		try {
			cluster.shutdown();
		}catch(Exception e) {
			e.printStackTrace(System.out);
		}
		
		System.out.println("Finsihed!!!");
	}

	
	
	
}
