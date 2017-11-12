package me.onlycool.Coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class SolrIndexCoprocessorObserver extends BaseRegionObserver{
	private static Logger log = Logger.getLogger(SolrIndexCoprocessorObserver.class);

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		super.postPut(e, put, edit, durability);
		String rowKey = Bytes.toString(put.getRow());
		//从HBase中读取指定列信息数据作为solr索引
        //信息标识
		Cell DEVICEID1 = put.get(Bytes.toBytes("cf1"),Bytes.toBytes("DEVICEID")).get(0);
		String DEVICEID = new String(CellUtil.cloneValue(DEVICEID1));
		
		Cell IP1 = put.get(Bytes.toBytes("cf1"),Bytes.toBytes("IP")).get(0);
		String IP = new String(CellUtil.cloneValue(IP1));
		//短信内容
		Cell PROJECTID1 = put.get(Bytes.toBytes("cf1"),Bytes.toBytes("PROJECTID")).get(0);
		String PROJECTID = new String(CellUtil.cloneValue(PROJECTID1));
		//短信下发时间
		Cell TESTTIME1 = put.get(Bytes.toBytes("cf1"),Bytes.toBytes("TESTTIME")).get(0);
		String TESTTIME = new String(CellUtil.cloneValue(TESTTIME1));
		
	}
	
}
