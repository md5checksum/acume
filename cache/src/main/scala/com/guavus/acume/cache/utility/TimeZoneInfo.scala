package com.guavus.acume.cache.utility

import java.util.List
import scala.reflect.BeanProperty
import scala.collection.JavaConversions._

class TimeZoneInfo(@BeanProperty var rules: List[List[String]], @BeanProperty var utcOffset: Long, @BeanProperty var id: String, @BeanProperty var name: String, @BeanProperty var fullName: String, @BeanProperty var dstName: String, @BeanProperty var dstFullName: String) {

  override def toString(): String = {
    var result = ""
    for (tempRule <- rules) {
      result += tempRule.toString + "\n"
    }
    result += id + "  " + name + "  " + fullName + "  " + dstName + "  " + dstFullName + "  " + utcOffset
    result
  }

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

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
*/
}