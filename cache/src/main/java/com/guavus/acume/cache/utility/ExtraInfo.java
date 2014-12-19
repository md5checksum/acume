package com.guavus.acume.cache.utility;

/**
 * @author archit.thakur
 *
 */
public class ExtraInfo {

	private String binsource, timegranularity, timezone;
	
	public ExtraInfo() {
		
	}
	
	public ExtraInfo(String binsource, String timegranularity, String timezone) { 
		
		this.binsource = binsource;
		this.timegranularity = timegranularity;
		this.timezone = timezone;
	}
	
	public String getBinsource() {
		return binsource;
	}
	public void setBinsource(String binsource) {
		this.binsource = binsource;
	}
	public String getTimegranularity() {
		return timegranularity;
	}
	public void setTimegranularity(String timegranularity) {
		this.timegranularity = timegranularity;
	}
	public String getTimezone() {
		return timezone;
	}
	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}
}
