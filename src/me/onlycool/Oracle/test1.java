package me.onlycool.Oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class test1 {
	@Test
	public void testConnetct(){
		
		Connection con = null;
	    PreparedStatement stmt = null;
	    try
	    {
	        Class.forName("oracle.jdbc.driver.OracleDriver"); // 驱动包不用单独下载，因为驱动在$JAVA_HOME/jre/lib/rt.jar 下的sun.jdbc.odbc.JdbcOdbcDriver,是jdk自己带的jar包
	        con = DriverManager.getConnection("jdbc:oracle:thin:@10.8.29.8:1521:dsplat","dsplat", "1234");
	        con.setAutoCommit(false); 
	       // PreparedStatement ps=con.prepareStatement("select count(*) vcount from DUST_DETAIL");
	        PreparedStatement ps=con.prepareStatement("SELECT * FROM(SELECT A.*, ROWNUM RN  FROM (SELECT * FROM DUST_DETAIL) A  )  WHERE RN BETWEEN 1 AND 10");
	        ResultSet Rs= ps.executeQuery();
	        System.out.println(Rs);
	        while (Rs.next()){
	            // 当结果集不为空时
	            //System.out.println("数据:" +Rs.getLong("vcount"));
	        	System.out.println("ID=:" +Rs.getString("ID"));
	        	System.out.println("DEVICEID:" +Rs.getString("DEVICEID"));
	        	System.out.println("IP:" +Rs.getString("IP"));
	        	System.out.println("TESTTIME:" +Rs.getString("TESTTIME"));
	        	SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        		Date date2=sdf.parse(Rs.getString("TESTTIME"));
        		System.out.println(date2.toLocaleString());
	    	}
	        
	       // System.out.println(rs.first());
	        /*for (int i=2; i<100 ;i++ )
	        {
	            stmt = con.prepareStatement("insert into t  values(?,?)" );
	            stmt.setInt(1,i);
	            stmt.setString(2, "tom"  + i);
	            stmt.executeUpdate();
	        }
	        con.commit();*/
	    
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
}
