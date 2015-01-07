package com.guavus.acume.core.scheduler

import scala.collection.mutable.HashMap


trait ISchedulerPolicy {

  def getIntervalsAndLastUpdateTime(startTime: Long, endTime: Long, cubeConfiguration: PrefetchCubeConfiguration, isFirstTimeRun: Boolean, optionalParams: HashMap[String, Any], taskManager: QueryRequestPrefetchTaskManager): PrefetchLastCacheUpdateTimeAndInterval

  def getCeilOfTime(time: Long): Long

  def clearState(): Unit

}