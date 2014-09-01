package com.guavus.equinox.flex;

import java.util.List;

public class TimeZoneInfo{
	private List<List<String>> rules;
	private long utcOffset;
	private String id;
	private String name;
	private String fullName;
	private String dstName;
	private String dstFullName;
	
	public TimeZoneInfo(List<List<String>> rules, long utcOffset, String id,
			String name, String fullName, String dstName, String dstFullName) {
		super();
		this.rules = rules;
		this.utcOffset = utcOffset;
		this.id = id;
		this.name = name;
		this.fullName = fullName;
		this.dstName = dstName;
		this.dstFullName = dstFullName;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFullName() {
		return fullName;
	}
	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	public String getDstName() {
		return dstName;
	}
	public void setDstName(String dstName) {
		this.dstName = dstName;
	}
	public String getDstFullName() {
		return dstFullName;
	}
	public void setDstFullName(String dstFullName) {
		this.dstFullName = dstFullName;
	}
	public List<List<String>> getRules() {
		return rules;
	}
	public void setRules(List<List<String>> rules) {
		this.rules = rules;
	}
	public long getUtcOffset() {
		return utcOffset;
	}
	public void setUtcOffset(long utcOffset) {
		this.utcOffset = utcOffset;
	}
	@Override
	public String toString() {
		String result = "";
		for(List<String> tempRule : rules){
			result+=tempRule.toString()+"\n";
		}
		result += id+"  "+name+"  "+fullName+"  "+dstName+"  "+dstFullName+"  "+utcOffset;
		return result;
	}

}