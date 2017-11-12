package cn.onlycool.me.pojo;

import java.util.Calendar;
import java.util.Date;

import org.apache.solr.client.solrj.beans.Field;

public class DUST_DETAIL {
	@Field
	private String id;
	@Field
	private String DEVICEID;
	@Field
	private String IP;
	@Field
	private String PROJECTID;
	@Field
	private Date TESTTIME;
	public String getId() {
		return id;
	}
	 
	public void setId(String id) {
		this.id = id;
	}
	public String getDEVICEID() {
		return DEVICEID;
	}
	public void setDEVICEID(String DEVICEID) {
		this.DEVICEID = DEVICEID;
	}
	public String getIP() {
		return IP;
	}
	public void setIP(String IP) {
		this.IP = IP;
	}
	
	public String getPROJECTID() {
		return PROJECTID;
	}
	public void setPROJECTID(String PROJECTID) {
		this.PROJECTID = PROJECTID;
	}
	public Date getTESTTIME() {
		return TESTTIME;
	}
	//@Field
	public void setTESTTIME(Date TESTTIME) {
		this.TESTTIME = StringToDate(TESTTIME);
	}
	public DUST_DETAIL(String id, String DEVICEID, String IP, String PROJECTID, Date TESTTIME) {
		super();
		this.id = id;
		this.DEVICEID = DEVICEID;
		this.IP = IP;
		this.PROJECTID = PROJECTID;
		this.TESTTIME = TESTTIME;
	}
	public DUST_DETAIL() {
		super();
	}
	public  Date StringToDate(Date date){
		Calendar cal=Calendar.getInstance();
		try{
			cal.setTime(date);
			cal.set(Calendar.HOUR_OF_DAY,cal.get(Calendar.HOUR_OF_DAY)+8);
		}catch(Exception e){
			e.printStackTrace();
		}
		return cal.getTime();
		//2016-01-24 12:46:00
	}
	
	
	
}
