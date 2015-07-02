package com.guavus.acume.cache.common

import CacheLevel._

/**
 * @author archit.thakur
 *
 */
case class LevelTimestamp(level: CacheLevel, timestamp:Long,var aggregationLevel : CacheLevel = null) {
	
  if(aggregationLevel == null) {
    aggregationLevel = level
  }
  @transient var loadType = LoadType.Insta
  
  def this(level : CacheLevel, timestamp : Long, loadType : LoadType.Value) = {
    this(level, timestamp)
    this.loadType = loadType
  }
  
  def this(level : CacheLevel, timestamp : Long, loadType : LoadType.Value, aggregationLevel : CacheLevel) = {
    this(level, timestamp, aggregationLevel)
    this.loadType = loadType
  }
  
  override def toString = level.localId.toString + "_" + timestamp.toString + "_" + aggregationLevel.localId
}

object LoadType extends Enumeration { 
  
  type LoadType = Value
  val Insta = Value("Insta")
  val DISK = Value("disk")
  val InMemory = Value("InMemory")
  
  
  def getLoadType(name: String): Value = { 
    
    name match { 
      
      case "Insta" => Insta
      case "disk" => DISK
      case "InMemory" => InMemory
      case _ => throw new RuntimeException("This LoadType is not supported.")
    }
  }
}

class AggregationLevel extends Enumeration { 
  
  type AggregationLevel = Value
  val Hourly = Value("1h")
  val Weekly = Value("1w")
  val BiWeekly = Value("2w")
  val Monthly = Value("1m")
  val Yearly = Value("1y")
  
  def getPersistenceLevel(name: String): Value = { 
    
    name match { 
      
      case "1h" => Hourly
      case "1w" => Weekly
      case "2w" => BiWeekly
      case "1m" => BiWeekly
      case "1y" => Yearly
      case _ => throw new RuntimeException("This PersistenceLevel is not supported.")
    }
  }
}