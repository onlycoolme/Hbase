package cn.onlycool.me.pojo;

public class Hbase_DUST_DETAIL {
	private String ID;
	private String DEVICEID;
	private String HUMIDITY;
	private String IP;
	private String PM1;
	private String PM10;
	private String PM2P5;
	private String PROJECTID;
	private String TEMPERATURE;
	private String TESTTIME;
	
	private String WINDOWDIRECTION;
	private String WINDOWSPEED;
	public Hbase_DUST_DETAIL(String iD, String dEVICEID, String hUMIDITY, String iP, String pM1, String pM10,
			String pM2P5, String pROJECTID, String tEMPERATURE, String tESTTIME, String wINDOWDIRECTION,
			String wINDOWSPEED) {
		super();
		ID = iD;
		DEVICEID = dEVICEID;
		HUMIDITY = hUMIDITY;
		IP = iP;
		PM1 = pM1;
		PM10 = pM10;
		PM2P5 = pM2P5;
		PROJECTID = pROJECTID;
		TEMPERATURE = tEMPERATURE;
		TESTTIME = tESTTIME;
		WINDOWDIRECTION = wINDOWDIRECTION;
		WINDOWSPEED = wINDOWSPEED;
	}
	
	public Hbase_DUST_DETAIL() {
		
	}

	public String getID() {
		return ID;
	}
	public void setID(String iD) {
		ID = iD;
	}
	public String getDEVICEID() {
		return DEVICEID;
	}
	public void setDEVICEID(String dEVICEID) {
		DEVICEID = dEVICEID;
	}
	public String getHUMIDITY() {
		return HUMIDITY;
	}
	public void setHUMIDITY(String hUMIDITY) {
		HUMIDITY = hUMIDITY;
	}
	public String getIP() {
		return IP;
	}
	public void setIP(String iP) {
		IP = iP;
	}
	public String getPM1() {
		return PM1;
	}
	public void setPM1(String pM1) {
		PM1 = pM1;
	}
	public String getPM10() {
		return PM10;
	}
	public void setPM10(String pM10) {
		PM10 = pM10;
	}
	public String getPM2P5() {
		return PM2P5;
	}
	public void setPM2P5(String pM2P5) {
		PM2P5 = pM2P5;
	}
	public String getPROJECTID() {
		return PROJECTID;
	}
	public void setPROJECTID(String pROJECTID) {
		PROJECTID = pROJECTID;
	}
	public String getTEMPERATURE() {
		return TEMPERATURE;
	}
	public void setTEMPERATURE(String tEMPERATURE) {
		TEMPERATURE = tEMPERATURE;
	}
	public String getTESTTIME() {
		return TESTTIME;
	}
	public void setTESTTIME(String tESTTIME) {
		TESTTIME = tESTTIME;
	}
	public String getWINDOWDIRECTION() {
		return WINDOWDIRECTION;
	}
	public void setWINDOWDIRECTION(String wINDOWDIRECTION) {
		WINDOWDIRECTION = wINDOWDIRECTION;
	}
	public String getWINDOWSPEED() {
		return WINDOWSPEED;
	}
	public void setWINDOWSPEED(String wINDOWSPEED) {
		WINDOWSPEED = wINDOWSPEED;
	}
	@Override
	public String toString() {
		return "Hbase_DUST_DETAIL [ID=" + ID + ", DEVICEID=" + DEVICEID + ", HUMIDITY=" + HUMIDITY + ", IP=" + IP
				+ ", PM1=" + PM1 + ", PM10=" + PM10 + ", PM2P5=" + PM2P5 + ", PROJECTID=" + PROJECTID + ", TEMPERATURE="
				+ TEMPERATURE + ", TESTTIME=" + TESTTIME + ", WINDOWDIRECTION=" + WINDOWDIRECTION + ", WINDOWSPEED="
				+ WINDOWSPEED + "]";
	}
}
