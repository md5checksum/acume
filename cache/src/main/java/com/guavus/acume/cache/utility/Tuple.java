package com.guavus.acume.cache.utility;

/**
 * @author archit.thakur
 */

public class Tuple {
	
	private long startTime = 0l;
	private long endTime = 0l;
	private String cubeName = "";
	private String binsource = null;
	
	public String getBinsource() {
		return binsource;
	}
	public void set(long startTime, long endTime, String cubeName, String binsource) {
		this.startTime= startTime;
		this.endTime = endTime;
		this.binsource = binsource;
		this.cubeName = cubeName;
	}
	public void setBinsource(String binsource) {
		this.binsource = binsource;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public String getCubeName() {
		return cubeName;
	}
	public void setCubeName(String tableName) {
		this.cubeName = tableName;
	}
	
}


