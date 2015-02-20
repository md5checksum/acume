package com.guavus.acume.cache.utility

import scala.reflect.{BeanProperty, BooleanBeanProperty}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

class Tuple {

  @BeanProperty
  var startTime: Long = 0l

  @BeanProperty
  var endTime: Long = 0l

  @BeanProperty
  var cubeName: String = ""

  @BeanProperty
  var binsource: String = null

  
  var singleEntityKeyValueList: ListBuffer[HashMap[String, Any]] = ListBuffer[HashMap[String, Any]]()
  
  
  def setSingleEntityKeyValueList(list : ListBuffer[HashMap[String, Any]]) {
    singleEntityKeyValueList = list
  }
  
  def getSingleEntityKeyValueList() = {
    singleEntityKeyValueList
  }

  def addSingleEntityKeyValueMap(keyvaluemap: HashMap[String, Any]) {
    (keyvaluemap) +: singleEntityKeyValueList 
  }

  def getNewBlankHashMap(): HashMap[String, Any] = {
    val hashmap = new HashMap[String, Any]()
    (hashmap) +: singleEntityKeyValueList
    hashmap
  }

  def set(startTime: Long, endTime: Long, cubeName: String, binsource: String, hashmap: ListBuffer[HashMap[String, Any]]) {
    this.startTime = startTime
    this.endTime = endTime
    this.binsource = binsource
    this.cubeName = cubeName
    this.singleEntityKeyValueList = hashmap
  }

  def set(startTime: Long, endTime: Long, cubeName: String, binsource: String) {
    this.startTime = startTime
    this.endTime = endTime
    this.binsource = binsource
    this.cubeName = cubeName
  }

/*
Original Java:
package com.guavus.acume.cache.utility;

import java.util.HashMap;
import java.util.LinkedList;

|**
 * @author archit.thakur
 *|

public class Tuple {
	
	private long startTime = 0l;
	private long endTime = 0l;
	private String cubeName = "";
	private String binsource = null;
	private LinkedList<HashMap<String, Object>> singleEntityKeyValueList = new LinkedList<HashMap<String, Object>>();
	
	public LinkedList<HashMap<String, Object>> getSingleEntityKeyValueList() {
		return singleEntityKeyValueList;
	}
	public void addSingleEntityKeyValueMap(HashMap<String, Object> keyvaluemap) {
		singleEntityKeyValueList.add(keyvaluemap);
	}
	public void setSingleEntityKeyValueList(LinkedList<HashMap<String, Object>> hashmap) {
		this.singleEntityKeyValueList = hashmap;
	}
	public HashMap<String, Object> getNewBlankHashMap() {
		HashMap<String, Object> hashmap = new HashMap<String, Object>();
		singleEntityKeyValueList.add(hashmap);
		return hashmap;
	}
	
	public void set(long startTime, long endTime, String cubeName, String binsource, LinkedList<HashMap<String, Object>> hashmap) {
		this.startTime= startTime;
		this.endTime = endTime;
		this.binsource = binsource;
		this.cubeName = cubeName;
		this.singleEntityKeyValueList = hashmap;
	}
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



*/
}