package com.guavus.acume.cache.common

import CacheLevel._

case class LevelTimestamp(level: CacheLevel, timestamp:Long)

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