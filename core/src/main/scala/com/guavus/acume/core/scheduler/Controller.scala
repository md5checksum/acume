package com.guavus.acume.core.scheduler

import com.guavus.rubix.cache.Interval
import scala.collection.immutable.HashMap
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller

case class Controller() {
  
  def getFirstBinPersistedTime(binSource : String) : Long = {
    BinAvailabilityPoller.getFirstBinPersistedTime(binSource)
  }
  
  def getLastBinPersistedTime(binSource : String) : Long = {
    BinAvailabilityPoller.getLastBinPersistedTime(binSource)
  }
  
  def getInstaTimeIntervalForBinSource(binSource : String) : Map[Long, (Long, Long)] = {
    BinAvailabilityPoller.getBinSourceToIntervalMap(binSource)
  }
  
  def getInstaTimeInterval() : Map[String, Map[Long, Interval]] = {
	BinAvailabilityPoller.getAllBinSourceToIntervalMap.map(x => {
	  (x._1 , x._2.map(y => {
	    (y._1, new Interval(y._2._1, y._2._2))
	  }))
	})
  }
}