package com.guavus.acume.cache.utility;

/**
 * @author archit.thakur
 */

public class Tuple {
	
	private long startTime = 0l;
	private long endTime = 0l;
	private String cubeName = "";
	
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


