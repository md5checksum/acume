package com.guavus.acume.cache.common

object CacheLevel extends Enumeration { 
  
  //Level of aggregation at Acume-cache level.
  //This should be more flexible by providing more values and taking input from 
  //AcumeConfiguration file that which all levels they want Acume-cache to build on. 	
  val FiveMin = new CacheLevel(300)
  val Hourly = new CacheLevel(3600)
  val Daily = new CacheLevel(86400)
  val Weekly = new CacheLevel(2592000)
  val BiWeekly = new CacheLevel(5184000)
  val Monthly = new CacheLevel(10368000)
//  val Yearly = new CacheLevel("1y")
  
  class CacheLevel(val localId: Int) extends Val
  
  implicit def convertValue(v: Value): CacheLevel = v.asInstanceOf[CacheLevel]
  
  def getCacheLevel(localId: Long): CacheLevel = { 
    
    for(value <- this.values){
      if(value.localId == localId)
        return value
    }
    return Hourly
  }
}