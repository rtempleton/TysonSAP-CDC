package com.github.rtempleton.tysonsappoc;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;

import com.github.rtempleton.poncho.StormUtils;

public class DistinctKeyBolt extends BaseWindowedBolt {


	private static final long serialVersionUID = 1L;
	
	private final String keyField;
	private OutputCollector collector = null;
	

	public DistinctKeyBolt(Properties props) {
		keyField = StormUtils.getRequiredProperty(props, "distinctKey.fieldName");
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		
		HashSet<String> table = new HashSet<>(inputWindow.get().size());
		
		for(Tuple input : inputWindow.get()) {
			String s = input.getStringByField(keyField);
//			System.out.println(s);
			table.add(input.getStringByField(keyField));
		}
		
		for (String key : table) {
			collector.emit(new Values(key));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(keyField));
	}


	@Override
	public void cleanup() {
		
		
	}

	@Override
	public TimestampExtractor getTimestampExtractor() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}
	
	

}
