package com.guavus.acume.core.scheduler

import com.guavus.acume.cache.core.Interval
import scala.collection.immutable.HashMap

object Controller {

  val hashmap = Map[String, Map[Long, Interval]](
      ("default" -> Map[Long, Interval](3600l -> new Interval(1404723600l, 1404727200l), -1l-> new Interval(1404723600l, 1404727200l)))
//      ("default123" -> Map[Long, Interval](86400l -> new Interval(0l, 86400l), -1l -> new Interval(3600l, 86400l)))
      )
  
  def getFirstBinPersistedTime(binSource : String) : Long = {
    hashmap.getOrElse(binSource, throw new RuntimeException("not found")).getOrElse(-1, throw new RuntimeException("not fond.")).getStartTime
  }
  
  def getRubixTimeIntervalForBinSource(binSource : String) : Map[Long, Interval] = {
    hashmap.getOrElse(binSource, throw new RuntimeException("not found"))
  }
  
  def getInstaTimeIntervalForBinSource(binSource : String) : Map[Long, Interval] = {
    hashmap.getOrElse(binSource, throw new RuntimeException("not found"))
  }
  
  def getInstaTimeInterval() : Map[String, Map[Long, Interval]] = {
    hashmap
  }
}