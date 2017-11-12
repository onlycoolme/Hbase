package me.onlycool.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class PhoneTest {
	//创建表
	@Test
	public void test1() throws Exception{
		Configuration config = HBaseConfiguration.create();  
		config.set("hbase.zookeeper.quorum","DataWorks.Master,DataWorks.Node1,DataWorks.Node2");
		System.out.println("aaaaa");
		HBaseAdmin admin = new HBaseAdmin(config); 
		String table = "t_cdr";
		
		if(admin.isTableAvailable(table)){
			admin.disableTable(table);
			admin.deleteTable(table);
		}else{
			HTableDescriptor t=new HTableDescriptor(table.getBytes());
			HColumnDescriptor cf1 = new HColumnDescriptor("cf1".getBytes());
//			cf1.setMaxVersions(10);
			t.addFamily(cf1);
			//admin.addColumn(table, cf1);
			admin.createTable(t);
		}
		admin.close();
		
	}
	//插入数据
	@Test
	public void test2()throws Exception{
		Configuration config=HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum","DataWorks.Master,DataWorks.Node1,DataWorks.Node2");
		HTable table=new HTable(config,"tb_cdr");
		String RowKey="15539037963_"+System.currentTimeMillis();
		Put put=new Put(RowKey.getBytes());
		put.add("cf1".getBytes(),"dest".getBytes(),"15539037963".getBytes());
		put.add("cf1".getBytes(),"type".getBytes(),"1".getBytes());
		put.add("cf1".getBytes(),"time".getBytes(),"2017-10-12 16:25:22".getBytes());
		table.put(put);
		table.close();
	}
	//通过列查询数据
	@Test
	public void test3()throws Exception{
		Configuration config=HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum","DataWorks.Master,DataWorks.Node1,DataWorks.Node2");
		HTable table=new HTable(config,"t_cdr");
		String RowKey="15539037963_1507604674846";
		Get get=new Get(RowKey.getBytes());
		Result res=table.get(get);
		Cell c1=res.getColumnLatestCell("cf1".getBytes(),"type".getBytes());
		Cell c2=res.getColumnLatestCell("cf1".getBytes(),"dest".getBytes());
		Cell c3=res.getColumnLatestCell("cf1".getBytes(),"time".getBytes());
		//CellScanner scan=res.listCells();
		String value;
		for(Cell c:res.listCells()){
			value=Bytes.toString(c.getQualifierArray(),c.getValueOffset(),c.getValueLength());
			System.out.println(value);
			System.out.println();
		}
		//rowkey按照字典排序
		//查询所有的空的scan
		/*Scan scan=new Scan();
		scan.setStartRow("15539037963_1507189588668".getBytes());
		scan.setStopRow("15539037963_1507189611673".getBytes());
		ResultScanner Rescan=table.getScanner(scan);
		for(Result r:Rescan){
			//可以在这里面做分页
		}*/
		table.close();
	}
}
