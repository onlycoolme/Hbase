package me.onlycool.solr;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.junit.Test;

import cn.onlycool.me.pojo.DUST_DETAIL;



public class sorlTest {
	@Test
	public void testAdd1() throws Exception{
	
		//通过bean插入
		HttpSolrClient hsc=new HttpSolrClient("http://10.8.29.19:8080/solr/collection1");
		List<DUST_DETAIL> details=new ArrayList<DUST_DETAIL>();
		details.add(new DUST_DETAIL("123sdsdsadsaadsa3dsa","j2121","dsadsa","上海",new Date()));
		details.add(new DUST_DETAIL("124dsadsadsadsa3ewq","jack221","dsadsa","上海",new Date()));
		hsc.addBeans(details);
		hsc.commit();
		hsc.close();
		SolrPingResponse response=hsc.ping();
		System.out.println(response.getResponseHeader());
		hsc.close();
		
	}
			
}
