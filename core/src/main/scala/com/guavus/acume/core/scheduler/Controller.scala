package com.guavus.acume.core.scheduler

import com.guavus.acume.cache.core.Interval
import scala.collection.immutable.HashMap
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait

case class Controller(acumeCacheContextTrait : AcumeCacheContextTrait) {

  
  def getFirstBinPersistedTime(binSource : String) : Long = {
    acumeCacheContextTrait.getFirstBinPersistedTime(binSource)
  }
  
  def getLastBinPersistedTime(binSource : String) : Long = {
    acumeCacheContextTrait.getLastBinPersistedTime(binSource)
  }
  
//  def getRubixTimeIntervalForBinSource(binSource : String) : Map[Long, Interval] = {
//    hashmap.getOrElse(binSource, throw new RuntimeException("not found"))
//  }
  
  def getInstaTimeIntervalForBinSource(binSource : String) : Map[Long, (Long, Long)] = {
    acumeCacheContextTrait.getBinSourceToIntervalMap(binSource)
  }
  
  def getInstaTimeInterval() : Map[String, Map[Long, Interval]] = {
	acumeCacheContextTrait.getAllBinSourceToIntervalMap.map(x => {
	  (x._1 , x._2.map(y => {
	    (y._1, new Interval(y._2._1, y._2._2))
	  }))
	})
  }
}