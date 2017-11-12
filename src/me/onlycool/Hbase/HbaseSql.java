package me.onlycool.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseSql {
	/*@Test
	public void test1() throws Exception{
	
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","DataWorks.Master,DataWorks.Node1,DataWorks.Node2");
		//HBaseAdmin admin=new HBaseAdmin(conf);
		//System.out.println(admin.isAborted());
		HTable table=new HTable(conf,"tb_item3");
		Scan scan=new Scan();
		//FilterList filterList=new FilterList();
	//	scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"));
	//	scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"));
		SingleColumnValueFilter a = new SingleColumnValueFilter(Bytes.toBytes("cf"),
		        Bytes.toBytes("status"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("1")));
		//Filter filter=new filterList.addFilter(a);
				
		  scan.setFilter(a);
		  ResultScanner rs = table.getScanner(scan);
	       System.out.println("rs========");
	        for (Result r : rs) {
	        	System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
	            for (KeyValue kv : r.raw()) {
	            	System.out.println("bbbbbbbbbbbbbbbbbbbbbb");
	                System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.", 
	                        Bytes.toString(kv.getRow()), 
	                        Bytes.toString(kv.getFamily()), 
	                        Bytes.toString(kv.getQualifier()), 
	                        Bytes.toString(kv.getValue()),
	                        kv.getTimestamp()));
	            }
	        }
	        
	        rs.close();
	}*/
	@Test
	public void test2()throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","DataWorks.Master,DataWorks.Node1,DataWorks.Node2");
		//HBaseAdmin admin=new HBaseAdmin(conf);
		//System.out.println(admin.isAborted());
		HTable table=new HTable(conf,"DUST_DETAIL");
		Scan scan=new Scan();
		//FilterList filterList=new FilterList();
	//	scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"));
	//	scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"));
		SingleColumnValueFilter a = new SingleColumnValueFilter(Bytes.toBytes("DETAIL"),
		        Bytes.toBytes("DEVICEID"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("6C611292H")));
		//Filter filter=new filterList.addFilter(a);
				
	 //    scan.setBatch(100);
		 scan.setFilter(a);
	//	scan.setStartRow("0087774918c044828ece09180b84ba2e".getBytes());
	//	scan.setStopRow("0087774918c044828ece09180b84ba2e".getBytes());
		  ResultScanner rs = table.getScanner(scan);
	       System.out.println("rs========");
	        for (Result r : rs) {
	        	//System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
	            for (KeyValue kv : r.raw()) {
	            	//System.out.println("bbbbbbbbbbbbbbbbbbbbbb");
	                System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.", 
	                        Bytes.toString(kv.getRow()), 
	                        Bytes.toString(kv.getFamily()), 
	                        Bytes.toString(kv.getQualifier()), 
	                        Bytes.toString(kv.getValue()),
	                        kv.getTimestamp()));
	            }
	        }
	        
	        rs.close();
	}
}
