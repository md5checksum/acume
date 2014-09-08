package com.guavus.acume.flex;

public class ZoneInfoParams {
	private String startYear;
	private String endYear;
	
	public ZoneInfoParams() {
	}
	
	public ZoneInfoParams(String startYear, String endYear) {
		super();
		this.startYear = startYear;
		this.endYear = endYear;
	}
	public String getStartYear() {
		return startYear;
	}
	public void setStartYear(String startYear) {
		this.startYear = startYear;
	}
	public String getEndYear() {
		return endYear;
	}
	public void setEndYear(String endYear) {
		this.endYear = endYear;
	}

}
