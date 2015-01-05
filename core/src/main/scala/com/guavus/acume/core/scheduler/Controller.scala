package com.guavus.acume.core.scheduler

import com.guavus.acume.cache.core.Interval

object Controller {

  
  def getFirstBinPersistedTime(binSource : String) : Long = {
    0
  }
  
  def getRubixTimeIntervalForBinSource(binSource : String) : Map[Long, Interval] = {
    Map[Long, Interval]()
  }
  
  def getInstaTimeIntervalForBinSource(binSource : String) : Map[Long, Interval] = {
    Map[Long, Interval]()
  }
  
  def getInstaTimeInterval() : Map[String, Map[Long, Interval]] = {
    Map[String, Map[Long, Interval]]()
  }
  
}