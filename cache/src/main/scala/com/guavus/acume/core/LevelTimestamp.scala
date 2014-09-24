package com.guavus.acume.core

case class LevelTimestamp(level: CacheLevel, timestamp:Long)

class CacheLevel extends Enumeration { 
  
  //Level of aggregation at Acume-cache level.
  //This should be more flexible by providing more values and taking input from 
  //AcumeConfiguration file that which all levels they want Acume-cache to build on. 	
  type CacheLevel = Value
  val Hourly = Value("1h")
  val Weekly = Value("1w")
  val BiWeekly = Value("2w")
  val Monthly = Value("1m")
  val Yearly = Value("1y")
  
  def getCacheLevel(name: String): Value = { 
    
    name match { 
      
      case "1h" => Hourly
      case "1w" => Weekly
      case "2w" => BiWeekly
      case "1m" => BiWeekly
      case "1y" => Yearly
      case _ => throw new RuntimeException("This CacheLevel is not supported.")
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