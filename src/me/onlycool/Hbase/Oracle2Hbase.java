package me.onlycool.Hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Oracle2Hbase{
	private static Connection con = null;
    private static PreparedStatement ps = null;
    private static String table="dsplat";
	static{
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver"); // ���������õ������أ���Ϊ������$JAVA_HOME/jre/lib/rt.jar �µ�sun.jdbc.odbc.JdbcOdbcDriver,��jdk�Լ�����jar��
			con = DriverManager.getConnection("jdbc:oracle:thin:@10.8.29.8:1521:dsplat","dsplat", "1234");
			con.setAutoCommit(false); 
	        PreparedStatement ps=con.prepareStatement("select count(*) vcount from DUST_DETAIL");
	        String sql1="select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE,COLUMN_ID��from user_tab_columns where table_name =UPPER('"+table+"')";
	       CreateTable2Hbase(); 
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	//ͨ��Oracle��Hbase�ﴴ����
	private static boolean CreateTable2Hbase(){
		
		return false;
	} 
	
}
