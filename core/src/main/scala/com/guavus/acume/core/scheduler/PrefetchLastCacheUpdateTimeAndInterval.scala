package com.guavus.acume.core.scheduler

import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.acume.cache.core.Interval

class PrefetchLastCacheUpdateTimeAndInterval {

  private var cacheEndTime: Long = 0

  @BeanProperty
  var cacheEndTimeMap: scala.collection.mutable.HashMap[Long, Long] = _

  @BeanProperty
  var intervals: scala.collection.mutable.HashSet[Interval] = new scala.collection.mutable.HashSet[Interval]()

  def getCacheLastUpdateTime(): Long = this.cacheEndTime

  def setCacheLastUpdateTime(time: Long) {
    this.cacheEndTime = time
  }

/*
Original Java:
package com.guavus.rubix.scheduler;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.guavus.rubix.cache.Interval;

public class PrefetchLastCacheUpdateTimeAndInterval {

	private long cacheEndTime;
	private Map<Long,Long> cacheEndTimeMap;
	
	private Set<Interval> intervals;
	
	public PrefetchLastCacheUpdateTimeAndInterval(){
		cacheEndTime = 0;
		intervals = new HashSet<Interval>();
	}
	
	public Map<Long,Long> getCacheEndTimeMap() {
		return cacheEndTimeMap;
	}



	public void setCacheEndTimeMap(Map<Long,Long> cacheEndTimeMap) {
		this.cacheEndTimeMap = cacheEndTimeMap;
	}



	public Set<Interval> getIntervals(){
		return this.intervals;
	}
	
	public long getCacheLastUpdateTime(){
		return this.cacheEndTime;
	}
	
	public void setCacheLastUpdateTime(long time){
		this.cacheEndTime = time;
	}
}

*/
}