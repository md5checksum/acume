package com.guavus.acume.cache.common

import com.guavus.acume.cache.core.TimeGranularity

/**
 * @author archit.thakur
 *
 */
object CacheLevel extends Enumeration { 
  
  //Level of aggregation at Acume-cache level.
  //This should be more flexible by providing more values and taking input from 
  //AcumeConfiguration file that which all levels they want Acume-cache to build on.
  val OneMin = new CacheLevel(TimeGranularity.ONE_MINUTE.getGranularity.toInt)
  val FiveMin = new CacheLevel(TimeGranularity.FIVE_MINUTE.getGranularity.toInt)
  val FifteenMinute = new CacheLevel(TimeGranularity.FIFTEEN_MINUTE.getGranularity.toInt)
  val Hourly = new CacheLevel(TimeGranularity.HOUR.getGranularity.toInt)
  val ThreeHourly = new CacheLevel(TimeGranularity.THREE_HOUR.getGranularity.toInt)
  val FourHourly = new CacheLevel(TimeGranularity.FOUR_HOUR.getGranularity.toInt)
  val HalfDay = new CacheLevel(TimeGranularity.HALF_DAY.getGranularity.toInt)
  val Daily = new CacheLevel(TimeGranularity.DAY.getGranularity.toInt)
  val TwoDAYS = new CacheLevel(TimeGranularity.TWO_DAYS.getGranularity.toInt)
  val ThreeDAYS = new CacheLevel(TimeGranularity.THREE_DAYS.getGranularity.toInt)
  val Weekly = new CacheLevel(TimeGranularity.WEEK.getGranularity.toInt)
  val Monthly = new CacheLevel(TimeGranularity.MONTH.getGranularity.toInt)
  
//  val Yearly = new CacheLevel("1y")
  
  class CacheLevel(val localId: Int) extends Val
  
  implicit def convertValue(v: Value): CacheLevel = v.asInstanceOf[CacheLevel]
  
  def getCacheLevel(localId: Long): CacheLevel = { 
    
    for(value <- this.values){
      if(value.localId == localId)
        return value
    }
    throw new RuntimeException("The gran " + localId + " is not yet supported in AcumeCache.")
  }
}