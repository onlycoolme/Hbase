package me.onlycool.Hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import cn.onlycool.me.pojo.DUST_DETAIL;
public class CurrentOracle2Hbase {
	
	//private static AtomicInteger startIndex=new AtomicInteger(1);
	private static int startIndex=1;
	private static Connection con2;
	//private int startIndex=1;
	private static int MaxNum=1000;
	private static Configuration conf;
	//private static HTable table;
	private static  int loopSize;
	static{
		conf = HBaseConfiguration.create();  
		conf.set("hbase.zookeeper.property.clientPort", "2181");  
		conf.set("hbase.zookeeper.quorum", "Hbase");  
		conf.set("hbase.master", "Hbase:60000"); 
		
		try{
			//table=new HTable(conf,"DUST_DETAIL");
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con2 = DriverManager.getConnection("jdbc:oracle:thin:@10.8.29.8:1521:dsplat","dsplat", "1234");
			  PreparedStatement ps3=con2.prepareStatement("select count(*) vcount from DUST_DETAIL");
			  ResultSet rs2=ps3.executeQuery();
			  while(rs2.next()){
				  loopSize=rs2.getInt("vcount")%MaxNum;
			  }
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public void importData(){
		//startIndex.addAndGet(1000);
		Connection con = null;
	    PreparedStatement stmt = null;
	    try
	    {
	        Class.forName("oracle.jdbc.driver.OracleDriver"); // 驱动包不用单独下载，因为驱动在$JAVA_HOME/jre/lib/rt.jar 下的sun.jdbc.odbc.JdbcOdbcDriver,是jdk自己带的jar包
	        con = DriverManager.getConnection("jdbc:oracle:thin:@10.8.29.8:1521:dsplat","dsplat", "1234");
	        con.setAutoCommit(false); 
	       // PreparedStatement ps=con.prepareStatement("select count(*) vcount from DUST_DETAIL");
	        PreparedStatement ps=con.prepareStatement("SELECT * FROM(SELECT A.*, ROWNUM RN  FROM (SELECT * FROM DUST_DETAIL) A  )  WHERE RN BETWEEN "+startIndex+" AND "+MaxNum+startIndex+" ");
	        ResultSet Rs= ps.executeQuery();
	        System.out.println(Rs);
	        while (Rs.next()){
	            // 当结果集不为空时
	            //System.out.println("数据:" +Rs.getLong("vcount"));
	        	System.out.println("ID=:" +Rs.getString("ID"));
	        	System.out.println("DEVICEID:" +Rs.getString("DEVICEID"));
	        	System.out.println("IP:" +Rs.getString("IP"));
	    	}
	    } 
	    catch(Exception e){
	        e.printStackTrace();
	        try
	        {
	            con.rollback();
	        }
	        catch (Exception ex)
	        {
	            ex.printStackTrace();
	        }
	    }finally{
	        try {
	        if(con != null) con.close();
	    }catch(Exception e) {e.printStackTrace();
	    
	    }
		
	    }
	}	
	public static void insert2Hbase()throws Exception{
		/*//String RowKey="15539037963_"+System.currentTimeMillis();
		Put put=new Put(RowKey.getBytes());
		put.add("cf1".getBytes(),"dest".getBytes(),"15539037963".getBytes());
		put.add("cf1".getBytes(),"type".getBytes(),"1".getBytes());
		put.add("cf1".getBytes(),"time".getBytes(),"2017-10-12 16:25:22".getBytes());
		table.put(put);
		table.close();*/
	}
	public static void main(String[] args) throws InterruptedException {
		 // 创建一个固定大小的线程池
        ExecutorService service = Executors.newFixedThreadPool(1);
        for (int i = 0; i < loopSize; i++) {
            getData2Hbase run=new CurrentOracle2Hbase.getData2Hbase(startIndex);
            startIndex+=MaxNum;
            // 在未来某个时间执行给定的命令
            
            service.submit(run);
           
        }
        // 关闭启动线程
        service.shutdown();
        // 等待子线程结束，再继续执行下面的代码
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        System.out.println("all thread complete");
    }
	
	public void createTable()throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf = HBaseConfiguration.create();  
		conf.set("hbase.zookeeper.property.clientPort", "2181");  
		conf.set("hbase.zookeeper.quorum", "Hbase");  
		conf.set("hbase.master", "Hbase:60000"); 
		HBaseAdmin admin = new HBaseAdmin(conf); 
		String table = "DUST_DETAIL";
		
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
	
	static class getData2Hbase implements Runnable{
		private int startIndex2;
		private int MaxNum=10000;
		
		public getData2Hbase(int startIndex) {
			super();
			this.startIndex2 = startIndex;
		}

		@Override
		public void run() {
			DUST_DETAIL detail=new DUST_DETAIL();
			Connection con = null;
		    PreparedStatement stmt = null;
		    HttpSolrClient hsc;
		    try{
		    	hsc=new HttpSolrClient("http://10.8.29.19:8080/solr/collection1");
		        Class.forName("oracle.jdbc.driver.OracleDriver"); // 驱动包不用单独下载，因为驱动在$JAVA_HOME/jre/lib/rt.jar 下的sun.jdbc.odbc.JdbcOdbcDriver,是jdk自己带的jar包
		        con = DriverManager.getConnection("jdbc:oracle:thin:@10.8.29.8:1521:dsplat","dsplat", "1234");
		        con.setAutoCommit(false); 
		        System.out.println("启动一个线程当前的startIndex="+startIndex2);
		        PreparedStatement ps2=con.prepareStatement("select count(*) vcount from DUST_DETAIL");
		        HTable table=new HTable(conf,"DUST_DETAIL");
		        PreparedStatement ps=con.prepareStatement("SELECT * FROM(SELECT A.*, ROWNUM RN  FROM (SELECT * FROM DUST_DETAIL) A  )  WHERE RN BETWEEN "+startIndex2+" AND "+MaxNum+startIndex2+" ");
		        ResultSet Rs= ps.executeQuery();
		        while (Rs.next()){
		        	Put put=null;
		        	if(Rs.getString("ID")!=null&&!Rs.getString("ID").equals("")){
		        		String RowKey=Rs.getString("ID");
		        		put=new Put(RowKey.getBytes());
		        		detail.setId(RowKey);
		        	}else{
		        		continue;
		        	}
		        	
		        	if(Rs.getString("DEVICEID")!=null&&!Rs.getString("DEVICEID").equals("")){
		        		
		        		put.add("cf1".getBytes(),"DEVICEID".getBytes(),Rs.getString("DEVICEID").getBytes());	        		
		        		detail.setDEVICEID(Rs.getString("DEVICEID"));
		        	}else{
		        		put.add("cf1".getBytes(),"DEVICEID".getBytes(),"".getBytes());
		        	}
		        	/*put.add("cf1".getBytes(),"DEVICEID".getBytes(),Rs.getString("DEVICEID").getBytes());*/
		        	
		        	if(Rs.getString("HUMIDITY")!=null&&!Rs.getString("HUMIDITY").equals("")){
		        		put.add("cf1".getBytes(),"HUMIDITY".getBytes(),Rs.getString("HUMIDITY").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"HUMIDITY".getBytes(),"".getBytes());
		        	}
		        	
		        	if(Rs.getString("IP")!=null&&!Rs.getString("IP").equals("")){
		        		detail.setIP(Rs.getString("IP"));
		        		put.add("cf1".getBytes(),"IP".getBytes(),Rs.getString("IP").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"IP".getBytes(),"".getBytes());
		        	}
		        	
		        	if(Rs.getString("PM1")!=null&&!Rs.getString("PM1").equals("")){
		        		put.add("cf1".getBytes(),"PM1".getBytes(),Rs.getString("PM1").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"PM1".getBytes(),"".getBytes());
		        	}
		        	
		        	if(Rs.getString("PM10")!=null&&!Rs.getString("PM10").equals("")){
		        		put.add("cf1".getBytes(),"PM10".getBytes(),Rs.getString("PM10").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"PM10".getBytes(),"".getBytes());
		        	}
		        	if(Rs.getString("PM2P5")!=null&&!Rs.getString("PM2P5").equals("")){
		        		put.add("cf1".getBytes(),"PM2P5".getBytes(),Rs.getString("PM2P5").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"PM2P5".getBytes(),"".getBytes());
		        	}
		        	if(Rs.getString("PROJECTID")!=null&&!Rs.getString("PROJECTID").equals("")){
		        		detail.setPROJECTID(Rs.getString("PROJECTID"));
		        		put.add("cf1".getBytes(),"PROJECTID".getBytes(),Rs.getString("PROJECTID").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"PROJECTID".getBytes(),"".getBytes());
		        	}
		        	if(Rs.getString("TEMPERATURE")!=null&&!Rs.getString("TEMPERATURE").equals("")){
		        		put.add("cf1".getBytes(),"TEMPERATURE".getBytes(),Rs.getString("TEMPERATURE").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"TEMPERATURE".getBytes(),"".getBytes());
		        	}
		        	if(Rs.getString("TESTTIME")!=null&&!Rs.getString("TESTTIME").equals("")){
		        		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		        		Date date2=sdf.parse(Rs.getString("TESTTIME"));
		        		detail.setTESTTIME(date2);
		        		put.add("cf1".getBytes(),"TESTTIME".getBytes(),Rs.getString("TESTTIME").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"TESTTIME".getBytes(),"".getBytes());
		        	}
		        	if(Rs.getString("WINDOWDIRECTION")!=null&&!Rs.getString("WINDOWDIRECTION").equals("")){
		        		put.add("cf1".getBytes(),"WINDOWDIRECTION".getBytes(),Rs.getString("WINDOWDIRECTION").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"WINDOWDIRECTION".getBytes(),"".getBytes());
		        	}
		        	if(Rs.getString("WINDOWSPEED")!=null&&!Rs.getString("WINDOWSPEED").equals("")){
		        		put.add("cf1".getBytes(),"WINDOWSPEED".getBytes(),Rs.getString("WINDOWSPEED").getBytes());
		        	}else{
		        		put.add("cf1".getBytes(),"WINDOWSPEED".getBytes(),"".getBytes());
		        	}
		        	//put.add("cf1".getBytes(),"DEVICEID".getBytes(),"1234".getBytes());
		        	
		        	table.put(put);
		        	hsc.addBean(detail);
		    		hsc.commit();
		        	
		    	}
		      table.close();
		      hsc.close();
		    }catch(Exception e){
		        e.printStackTrace();
		        try{
		            con.rollback();
		        }
		        catch (Exception ex){
		            ex.printStackTrace();
		        }
		     }finally{
		        try {
		        	//if(con!= null) con.close();
		    }catch(Exception e) {
		    	e.printStackTrace();
		    }
			
		}
		    		
    }		
}
}
