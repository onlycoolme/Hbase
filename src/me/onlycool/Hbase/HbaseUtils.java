package me.onlycool.Hbase;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import cn.onlycool.me.pojo.DUST_DETAIL;
import cn.onlycool.me.pojo.Hbase_DUST_DETAIL;

public class HbaseUtils {
	private static Configuration conf;
	private static HttpSolrClient hsc;
	private static DUST_DETAIL detail;
	private static HTable table;
	static{
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");  
		conf.set("hbase.zookeeper.quorum", "Hbase");  
		conf.set("hbase.master", "Hbase:60000"); 
		hsc=new HttpSolrClient("http://10.8.29.19:8080/solr/collection1");
		detail=new DUST_DETAIL();
		try{
			table=new HTable(conf,"DUST_DETAIL");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public  static void insert(Hbase_DUST_DETAIL dust){
		try{	
			
			Put put=new Put(dust.getID().getBytes());
			detail.setId(dust.getID());
        	if(dust.getDEVICEID()!=null&&!dust.getDEVICEID().equals("")){
        		put.add("cf1".getBytes(),"DEVICEID".getBytes(),dust.getDEVICEID().getBytes());	
        		detail.setDEVICEID(dust.getDEVICEID());
        	}else{
        		put.add("cf1".getBytes(),"DEVICEID".getBytes(),"".getBytes());
        	}
        	if(dust.getHUMIDITY()!=null&&!dust.getHUMIDITY().equals("")){
        		put.add("cf1".getBytes(),"HUMIDITY".getBytes(),dust.getHUMIDITY().getBytes());	
        	}else{
        		put.add("cf1".getBytes(),"HUMIDITY".getBytes(),"".getBytes());
        	}
        	if(dust.getIP()!=null&&!dust.getIP().equals("")){
        		put.add("cf1".getBytes(),"IP".getBytes(),dust.getIP().getBytes());	
        		detail.setIP(dust.getIP());
        	}else{
        		put.add("cf1".getBytes(),"IP".getBytes(),"".getBytes());
        	}
        	if(dust.getPM1()!=null&&!dust.getPM1().equals("")){
        		put.add("cf1".getBytes(),"PM1".getBytes(),dust.getPM1().getBytes());	
        	}else{
        		put.add("cf1".getBytes(),"PM1".getBytes(),"".getBytes());
        	}
        	if(dust.getPM10()!=null&&!dust.getPM10().equals("")){
        		put.add("cf1".getBytes(),"PM10".getBytes(),dust.getPM10().getBytes());	
        	}else{
        		put.add("cf1".getBytes(),"PM10".getBytes(),"".getBytes());
        	}
        	if(dust.getPM2P5()!=null&&!dust.getPM2P5().equals("")){
        		put.add("cf1".getBytes(),"PM2P5".getBytes(),dust.getPM2P5().getBytes());	
        	}else{
        		put.add("cf1".getBytes(),"PM2P5".getBytes(),"".getBytes());
        	}
        	if(dust.getPROJECTID()!=null&&!dust.getPROJECTID().equals("")){
        		put.add("cf1".getBytes(),"PROJECTID".getBytes(),dust.getPROJECTID().getBytes());	
        		detail.setPROJECTID(dust.getPROJECTID());
        	}else{
        		put.add("cf1".getBytes(),"PROJECTID".getBytes(),"".getBytes());
        	}
        	if(dust.getTEMPERATURE()!=null&&!dust.getTEMPERATURE().equals("")){
        		put.add("cf1".getBytes(),"TEMPERATURE".getBytes(),dust.getTEMPERATURE().getBytes());	
        	}else{
        		put.add("cf1".getBytes(),"TEMPERATURE".getBytes(),"".getBytes());
        	}
        	if(dust.getTESTTIME()!=null&&!dust.getTESTTIME().equals("")){
        		put.add("cf1".getBytes(),"TESTTIME".getBytes(),dust.getTESTTIME().getBytes());
        		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        		Date date2=sdf.parse(dust.getTESTTIME());
        		detail.setTESTTIME(date2);
        	}else{
        		put.add("cf1".getBytes(),"TESTTIME".getBytes(),"".getBytes());
        	}
        	if(dust.getWINDOWDIRECTION()!=null&&!dust.getWINDOWDIRECTION().equals("")){
        		put.add("cf1".getBytes(),"WINDOWDIRECTION".getBytes(),dust.getWINDOWDIRECTION().getBytes());	
        	}else{
        		put.add("cf1".getBytes(),"WINDOWDIRECTION".getBytes(),"".getBytes());
        	}
        	if(dust.getWINDOWSPEED()!=null&&!dust.getWINDOWSPEED().equals("")){
        		put.add("cf1".getBytes(),"WINDOWSPEED".getBytes(),dust.getWINDOWSPEED().getBytes());	
        	}else{
        		put.add("cf1".getBytes(),"WINDOWSPEED".getBytes(),"".getBytes());
        	}
        	table.put(put);
        	hsc.addBean(detail);
    		hsc.commit();
		}catch(Exception e){
			
		}
	}
	public static List<Hbase_DUST_DETAIL>  Query(String Filed,String value,int start,int num){
		List<Hbase_DUST_DETAIL> list=new ArrayList<Hbase_DUST_DETAIL>();
		Hbase_DUST_DETAIL hdd=new Hbase_DUST_DETAIL();
		SolrQuery sq=new SolrQuery();
		sq.set("q",Filed+":"+value);
		sq.setStart(start);
		sq.setRows(num);
		QueryResponse result=null;
		try{
			result = hsc.query(sq);
		}catch(Exception e){
			e.printStackTrace();
		}
		SolrDocumentList results = result.getResults();
		
		//通过solr查到Id
		for (SolrDocument solrDocument : results) {
			  hdd.setID(solrDocument.get("id").toString());
			  list.add(hdd);
		}
		//通过id获取数据到bean
		for(Hbase_DUST_DETAIL it:list){
			String id=it.getID();
			Get scan = new Get(id.getBytes());
			try{
				Result r = table.get(scan); 
	            System.out.println("获得到rowkey:" + new String(r.getRow())); 
	            for (KeyValue kv : r.raw()) { 
	            /*	System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.", 
	                        Bytes.toString(kv.getValue()),
	                        kv.getTimestamp()));*/
	            	if(Bytes.toString(kv.getQualifier()).equals("DEVICEID")){
                    	it.setDEVICEID(Bytes.toString(kv.getValue()));
                    }
	            	if(Bytes.toString(kv.getQualifier()).equals("HUMIDITY")){
	            		it.setHUMIDITY(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("IP")){
	            		it.setIP(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("PM1")){
	            		it.setPM1(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("PM10")){
	            		it.setPM10(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("PM2P5")){
	            		it.setPM2P5(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("PROJECTID")){
	            		it.setPROJECTID(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("TEMPERATURE")){
	            		it.setTEMPERATURE(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("TESTTIME")){
	            		it.setTESTTIME(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("WINDOWDIRECTION")){
	            		it.setWINDOWDIRECTION(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("WINDOWSPEED")){
	            		it.setWINDOWSPEED(Bytes.toString(kv.getValue()));
	            	}
	            }  
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return list;
	}
	public static List<Hbase_DUST_DETAIL> QueryByTime(Date FromDate,Date ToDate,int start,int num){
		List<Hbase_DUST_DETAIL> list=new ArrayList<Hbase_DUST_DETAIL>();
		Hbase_DUST_DETAIL hdd=new Hbase_DUST_DETAIL();
		SolrQuery sq=new SolrQuery();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");  
		Calendar cal=Calendar.getInstance();
		cal.setTime(ToDate);
		cal.set(Calendar.HOUR_OF_DAY,24);
		cal.set(Calendar.MINUTE,59);
		cal.set(Calendar.SECOND,59);
		ToDate=cal.getTime();
		String time = "TESTTIME:["+sdf.format(FromDate)+" TO "+sdf.format(ToDate)+"]";  
		sq.setQuery(time);
		sq.setStart(start);
		sq.setRows(num);
		QueryResponse result=null;
		try{
			result = hsc.query(sq);
		}catch(Exception e){
			e.printStackTrace();
		}
		SolrDocumentList results = result.getResults();
		
		//通过solr查到Id
		for (SolrDocument solrDocument : results) {
			  hdd.setID(solrDocument.get("id").toString());
			  list.add(hdd);
		}
		//通过id获取数据到bean
		for(Hbase_DUST_DETAIL it:list){
			String id=it.getID();
			Get scan = new Get(id.getBytes());
			try{
				Result r = table.get(scan); 
	            System.out.println("获得到rowkey:" + new String(r.getRow())); 
	            for (KeyValue kv : r.raw()) { 
	            /*	System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.", 
	                        Bytes.toString(kv.getValue()),
	                        kv.getTimestamp()));*/
	            	if(Bytes.toString(kv.getQualifier()).equals("DEVICEID")){
                    	it.setDEVICEID(Bytes.toString(kv.getValue()));
                    }
	            	if(Bytes.toString(kv.getQualifier()).equals("HUMIDITY")){
	            		it.setHUMIDITY(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("IP")){
	            		it.setIP(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("PM1")){
	            		it.setPM1(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("PM10")){
	            		it.setPM10(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("PM2P5")){
	            		it.setPM2P5(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("PROJECTID")){
	            		it.setPROJECTID(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("TEMPERATURE")){
	            		it.setTEMPERATURE(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("TESTTIME")){
	            		it.setTESTTIME(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("WINDOWDIRECTION")){
	            		it.setWINDOWDIRECTION(Bytes.toString(kv.getValue()));
	            	}
	            	if(Bytes.toString(kv.getQualifier()).equals("WINDOWSPEED")){
	            		it.setWINDOWSPEED(Bytes.toString(kv.getValue()));
	            	}
	            }  
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return list;
	}
	
	public static void main(String[] args) {
		/*Hbase_DUST_DETAIL dust=new Hbase_DUST_DETAIL();
		dust.setDEVICEID("dsa");
		dust.setHUMIDITY("dsadsa");
		dust.setID("测试插入");
		dust.setPM1("dsadsa");
		dust.setPM10("dadsa");
		dust.setTESTTIME(new Date().toLocaleString());
		HbaseUtils.insert(dust);*/
		/*List<Hbase_DUST_DETAIL> list =HbaseUtils.Query("DEVICEID", "1193378686", 0, 2);
		for(Hbase_DUST_DETAIL detaill:list){
			System.out.println(detaill);
		}*/
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date date=null;
		try{
			date=sdf.parse("2016-02-20 00:00:00");
			System.out.println(date.toLocaleString());
		}catch(Exception e){
			e.printStackTrace();
		}
		List<Hbase_DUST_DETAIL> list =HbaseUtils.QueryByTime(date, date, 0, 3);
		for(Hbase_DUST_DETAIL detaill:list){
			System.out.println(detaill);
		}
		
	}
	/*public static  void delete(){
		
	}
	public static void update(){
		
	}*/
}
