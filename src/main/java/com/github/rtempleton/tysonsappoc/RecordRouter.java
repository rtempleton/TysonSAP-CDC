package com.github.rtempleton.tysonsappoc;

import java.util.ArrayList;
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

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

public class RecordRouter implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger Log = LoggerFactory.getLogger(RecordRouter.class);
	
	private final List<String> inputFields;
	private int parseField = -1;
	private final String jsonTableNamePath = "$['message']['data']['Table']";
	
	private OutputCollector collector;
	

	public RecordRouter(Properties props, List<String> inputFields) {
		this.inputFields = inputFields;
		for(int i=0;i<inputFields.size();i++) {
			//hardcoding the name of the Kafka field here
			if(inputFields.get(i).equals("value"));
			parseField = i;
		}
		if (parseField==-1) {
			Log.error("The parse field was not found in the list of input fields");
			System.exit(-1);
		}
	}

	@Override
	public void cleanup() {
		

	}

	@Override
	public void execute(Tuple input) {
		String value = input.getString(parseField);
		
		DocumentContext jsonContext = JsonPath.parse(value);
		String tableName = jsonContext.read(jsonTableNamePath);
		
		switch (tableName) {
		
			case "EKKO":
				collector.emit("EKKO", input, input.getValues());
				break;
				
			case "EKPO":
				collector.emit("EKPO", input, input.getValues());
				break;
				
			case "EKET":
				collector.emit("EKET", input, input.getValues());
				break;
				
			case "EKPV":
				collector.emit("EKPV", input, input.getValues());
				break;
				
			case "EKBE":
				collector.emit("EKBE", input, input.getValues());
				break;
				
			default:
				Log.error("Routing error - Unknown table name: " + tableName);
				Log.error(value);
				break;
		}
		
		collector.ack(input);

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	/*
	 * Name all the output streams that this bolt could possible direct records to
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		final ArrayList<String> outputFields = new ArrayList<String>();
		outputFields.addAll(inputFields);
		Fields out = new Fields(outputFields);
		declarer.declareStream("EKKO", out);
		declarer.declareStream("EKPO", out);
		declarer.declareStream("EKET", out);
		declarer.declareStream("EKPV", out);
		declarer.declareStream("EKBE", out);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
