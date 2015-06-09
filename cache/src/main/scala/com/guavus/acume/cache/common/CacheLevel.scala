package com.guavus.acume.cache.common

import com.guavus.acume.cache.core.TimeGranularity

/**
 * @author archit.thakur
 *
 */
object CacheLevel extends Enumeration { 
  
	val nameToLevelMap = scala.collection.mutable.HashMap[String, CacheLevel]()
  //Level of aggregation at Acume-cache level.
  //This should be more flexible by providing more values and taking input from 
  //AcumeConfiguration file that which all levels they want Acume-cache to build on.
  val OneMin = new CacheLevel(TimeGranularity.ONE_MINUTE.getGranularity.toInt, "OneMin")
  val FiveMin = new CacheLevel(TimeGranularity.FIVE_MINUTE.getGranularity.toInt, "FiveMin")
  val FifteenMinute = new CacheLevel(TimeGranularity.FIFTEEN_MINUTE.getGranularity.toInt, "FifteenMinute")
  val Hourly = new CacheLevel(TimeGranularity.HOUR.getGranularity.toInt, "Hourly")
  val ThreeHourly = new CacheLevel(TimeGranularity.THREE_HOUR.getGranularity.toInt, "ThreeHourly")
  val FourHourly = new CacheLevel(TimeGranularity.FOUR_HOUR.getGranularity.toInt, "FourHourly")
  val HalfDay = new CacheLevel(TimeGranularity.HALF_DAY.getGranularity.toInt, "HalfDay")
  val Daily = new CacheLevel(TimeGranularity.DAY.getGranularity.toInt, "Daily")
  val TwoDAYS = new CacheLevel(TimeGranularity.TWO_DAYS.getGranularity.toInt, "TwoDAYS")
  val ThreeDAYS = new CacheLevel(TimeGranularity.THREE_DAYS.getGranularity.toInt, "ThreeDAYS")
  val Weekly = new CacheLevel(TimeGranularity.WEEK.getGranularity.toInt, "Weekly")
  val Monthly = new CacheLevel(TimeGranularity.MONTH.getGranularity.toInt, "Monthly")
  
//  val Yearly = new CacheLevel("1y")
  
  class CacheLevel(val localId: Int) extends Val {
    var name : String = _
    def this(localId: Int, name : String) = {
      this(localId)
      this.name = name
      nameToLevelMap.+=(name -> this)
    }
    
    override def toString() = {
	  name
  	}
  }
  
  
  implicit def convertValue(v: Value): CacheLevel = v.asInstanceOf[CacheLevel]
  
  def getCacheLevel(localId: Long): CacheLevel = { 
    
    for(value <- this.values){
      if(value.localId == localId)
        return value
    }
    throw new RuntimeException("The gran " + localId + " is not yet supported in AcumeCache.")
  }
  
}