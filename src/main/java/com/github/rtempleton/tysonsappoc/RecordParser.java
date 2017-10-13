package com.github.rtempleton.tysonsappoc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rtempleton.poncho.StormUtils;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

public class RecordParser implements IRichBolt {


	private static final long serialVersionUID = 1L;
	private static final Logger Log = LoggerFactory.getLogger(RecordParser.class);
	
	final List<String> fields;
	final String path = "$['message']['data']";
	private OutputCollector collector;
			
			
	public RecordParser(Properties props, String parserName) {
		String outputFields = StormUtils.getRequiredProperty(props, parserName + ".output");
		fields = Arrays.asList(outputFields.split(","));
	}

	@Override
	public void cleanup() {
		

	}

	@Override
	public void execute(Tuple input) {
		String value = input.getStringByField("value");
		DocumentContext jsonContext = JsonPath.parse(value);
		Map<String, Object> doc = jsonContext.read(path);
		
		List<Object> vals = new ArrayList<Object>(fields.size());
		for(String field : fields) {
			vals.add(doc.get(field));
		}
		
		collector.emit(input, vals);
		collector.ack(input);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(fields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
