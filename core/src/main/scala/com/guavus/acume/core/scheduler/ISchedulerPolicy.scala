package com.guavus.acume.core.scheduler


trait ISchedulerPolicy {

  def getIntervalsAndLastUpdateTime(startTime: Long, endTime: Long, cubeConfiguration: PrefetchCubeConfiguration, isFirstTimeRun: Boolean, optionalParams: scala.collection.mutable.HashMap[String, Any]): PrefetchLastCacheUpdateTimeAndInterval

  def getCeilOfTime(time: Long): Long

  def clearState(): Unit

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.scheduler;

import java.util.Map;


|**
 * @author prakhar.goyal
 *
 *|
public interface ISchedulerPolicy {
	|**
	 * This method returns all the time intervals for which prefetch tasks need to be scheduled.
	 * @param startTime
	 * @param endTime
	 * @param cubeConfiguration - May be null
	 * @param isFirstTimeRun
	 * @param optionalParams - May be null
	 * @return PrefetchLastCacheUpdateTimeAndInterval
	 *|
	
	PrefetchLastCacheUpdateTimeAndInterval getIntervalsAndLastUpdateTime(
			long startTime, long endTime,
			PrefetchCubeConfiguration cubeConfiguration, boolean isFirstTimeRun, Map<String, Object> optionalParams);

	|**
	 * Method to get Ceiling value of time.
	 * @param time currentTime.
	 * @return Ceiling value of time.
	 *|
	public long getCeilOfTime(long time);

	public void clearState();
	
}

*/
}