package com.guavus.acume.core.scheduler


trait ISchedulerPolicy {

  def getIntervalsAndLastUpdateTime(startTime: Long, endTime: Long, cubeConfiguration: PrefetchCubeConfiguration, isFirstTimeRun: Boolean, optionalParams: scala.collection.mutable.HashMap[String, Any]): PrefetchLastCacheUpdateTimeAndInterval

  def getCeilOfTime(time: Long): Long

  def clearState(): Unit

}