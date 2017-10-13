package com.github.rtempleton.tysonsappoc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rtempleton.poncho.StormUtils;

public class GenGoldenRecord implements IRichBolt {


	private static final long serialVersionUID = 1L;
	private static final Logger Log = LoggerFactory.getLogger(GenGoldenRecord.class);
	
	private OutputCollector collector = null;
	
	private final String jdbcurl;
	private Connection con = null;
	private final String keyField;
	private final String query = "UPSERT INTO GOLDENREC\n" + 
			"select ekpo.EBELN as PO_DOC, \n" + 
			"                ekpo.EBELP as PO_DOC_ITEM_NUM,\n" + 
			"                ekbe.BELNR as DELV_DOC,\n" + 
			"                ekpo.LOEKZ as PO_LINE_DELETE_IND,\n" + 
			"                ekpo.AEDAT as PO_LINE_MOD_DATE,\n" + 
			"                ekpo.MATNR as MTRL,\n" + 
			"                ekpo.WERKS as RECV_PLANT,\n" + 
			"                ekpo.LGORT as STO_LINE_STOR_LOC,\n" + 
			"                ekpo.MENGE as PO_LINE_QTY,\n" + 
			"                ekpo.MEINS as PO_LINE_QTY_UOM,\n" + 
			"                ekpo.BPRME as PO_LINE_NET_PRICE_AMT_UOM,\n" + 
			"                ekpo.NETPR as PO_LINE_NET_PRICE_AMT,\n" + 
			"                ekpo.ELIKZ as PO_LINE_COMPLETE_IND,\n" + 
			"                ekpo.PSTYP as PO_LINE_CAT,\n" + 
			"                ekpo.EVERS as DELV_DOC_SHIP_INSTRUCTIONS,\n" + 
			"                t027b.EVTXT as STO_SHIP_INSTRN,\n" + 
			"                ekpo.BSTAE as CONFIRMATION_CONTROL_KEY,\n" + 
			"                t163m.BSBEZ as STO_SHIP_DESCR,\n" + 
			"                ekpo.EGLKZ as STO_LINE_FINAL_DELV_IND,\n" + 
			"                ekpo.KZTLF as STO_LINE_PART_DELV_IND,\n" + 
			"                eket.ETENR as PO_LINE_SCHED_LINE_ITEM,\n" + 
			"                eket.EINDT as PO_LINE_SCHED_DELV_DATE,\n" + 
			"                eket.MENGE as PO_LINE_SCHED_QTY,\n" + 
			"                eket.WEMNG as PO_LINE_SCHED_DELV_QTY,\n" + 
			"                eket.UZEIT as PO_LINE_SCHED_DELV_TIME,\n" + 
			"                ekpv.LPRIO as STO_LINE_DELV_PRIORITY,\n" + 
			"                ekpv.VSBED as STO_LINE_SHIP_CONDITION,\n" + 
			"                ekko.BSART as PO_TYPE,\n" + 
			"                ekko.LOEKZ as PO_DELETE_IND,\n" + 
			"                ekko.AEDAT as PO_CREATE_DATE,\n" + 
			"                ekko.ERNAM as PO_CREATE_USERID,\n" + 
			"                ekko.LIFNR as VEND_NAME1,\n" + 
			"                ekko.WAERS as PO_CRN,\n" + 
			"                ekko.BEDAT as PO_DOC_DATE,\n" + 
			"                ekko.RESWK as SHIP_PLANT,\n" + 
			"                case when ekpo.MENGE = 0 then 0 else ekpo.NETPR/ekpo.MENGE end as NET_PRICE_UNIT_AMT,\n" + 
			"                round(ekpo.MENGE * (ekpo.NETPR/ekpo.MENGE)) as STO_LINE_SCHED_LINE_NET_PRICE_AMT\n" + 
			"from eket\n" + 
			"join ekpo on (eket.EBELN = ekpo.EBELN and eket.EBELP = ekpo.EBELP)\n" + 
			"join ekko on (ekko.EBELN = ekpo.EBELN and ekko.BSART in ('UB', 'STO'))\n" + 
			"left outer join ekpv on (ekpv.EBELN = ekpo.EBELN and ekpv.EBELP = ekpo.EBELP)\n" + 
			"left outer join t163m on (ekpo.BSTAE = t163m.BSTAE and t163m.SPRAS = 'E')\n" + 
			"left outer join t027b on (ekpo.EVERS = t027b.EVERS and t027b.SPRAS = 'E')\n" + 
			"left outer join ekbe on (ekpo.EBELN = ekbe.EBELN and ekpo.EBELP = ekbe.EBELP)\n" + 
			"where ekbe.BEWTP = 'L'\n" + 
			"and ekbe.MENGE != 0\n" + 
			"and ekbe.VGABE = '8'\n" + 
			"and ekpo.EBELN = ?";
	

	public GenGoldenRecord(Properties props, String boltName) {
		jdbcurl = StormUtils.getRequiredProperty(props, boltName + ".jdbcUrl");
		keyField = StormUtils.getRequiredProperty(props, "distinctKey.fieldName");
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
		
		Connection con = null;
		PreparedStatement stmt = null;
		
		try {
			con = DriverManager.getConnection(jdbcurl);
			con.setAutoCommit(false);
			stmt = con.prepareStatement(query);
			stmt.setString(1, input.getStringByField(keyField));
			//need to test out what kind of results this returns and if we can test the querys effectiveness
			stmt.execute();
			con.commit();
			
		}catch(SQLException e) {
			Log.error("Error generating golden record using the key and value: " + keyField + " " + input.getStringByField(keyField));
			Log.error(e.getMessage());
			
		}finally {
			collector.ack(input);
			try {
				
				if(stmt!=null)
					stmt.close();
				if(con!=null)
					con.close();
				
			}catch(SQLException e) {
				//nothing
			}
		}

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		//No outputs to declare

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
