package com.guavus.acume.core

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import java.util.function.Function
import acume.exception.AcumeException
import com.guavus.acume.core.exceptions.AcumeExceptionConstants
import scala.collection.mutable.HashMap

abstract class QueryPoolPolicy(throttleMap : Map[String, Int], acumeContext: AcumeContext) {
  
  def getQueriesClassification(queries : List[String], classificationStats : ClassificationStats) : List[(String, HashMap[String, Any])] = {
    var classificationList: java.util.ArrayList[(String, HashMap[String, Any])] = new java.util.ArrayList()
    queries foreach(query => {
      val classification = getQueryClassification(query, classificationStats)
      classificationList.add(new Tuple2(classification, acumeContext.ac.threadLocal.get()))
      acumeContext.ac.threadLocal.remove()
    })
    classificationList.toList
  }
  
  def getQueryClassification(query : String, classificationStats : ClassificationStats) : String = null
  
  def checkForThrottle(classification : String, classificationStats : ClassificationStats) = throttleMap.get(classification).map(throttleValue => { 
    if(classificationStats.getStatsForClassification(classification).currentRunningQries.get() >= throttleValue)
      throw new AcumeException(AcumeExceptionConstants.TOO_MANY_CONNECTION_EXCEPTION.name)
    else
      null
    }).getOrElse(null)

  def getPoolNameForClassification(classification : String, poolStats : PoolStats) : String = null
  
  def updateInitialStats(poolList: List[String], classificationList: List[String], poolStats: PoolStats, classificationStats: ClassificationStats) {
    
    var poolIterator = poolList.iterator
    
    classificationList foreach(classification => {
      var poolname = poolIterator.next()
      
      if (classification != null && poolname != null) {
        var poolStatAttribute = poolStats.getStatsForPool(poolname)
        var classificationStatAttribute = classificationStats.getStatsForClassification(classification)
        poolStatAttribute.currentRunningQries.addAndGet(1)
        classificationStatAttribute.currentRunningQries.addAndGet(1)
        println("poolname : ", poolStatAttribute.currentRunningQries.get)
        println("classificationStatAttribute : ", classificationStatAttribute.currentRunningQries.get)
      }
    })
  }

  def updateStats(poolname: String, classificationname: String, poolStats: PoolStats, classificationStats: ClassificationStats, starttime: Long, endtime: Long) {
    if (poolname != null && classificationname != null) {
      var poolStatAttribute = poolStats.getStatsForPool(poolname)
      var classificationStatAttribute = classificationStats.getStatsForClassification(classificationname)
      println("poolname delete : ", poolStatAttribute.currentRunningQries.get)
      println("classificationStatAttribute delete : ", classificationStatAttribute.currentRunningQries.get)

      var querytimeDifference = endtime - starttime
      setFinalStatAttribute(poolStatAttribute, querytimeDifference)
      setFinalStatAttribute(classificationStatAttribute, querytimeDifference)

      poolStats.setStatsForPool(poolname, poolStatAttribute)
      classificationStats.setStatsForClassification(classificationname, classificationStatAttribute)
    }
  }

  def updateFinalStats(poolname: String, classificationname: String, poolStats: PoolStats, classificationStats: ClassificationStats, starttime: Long, endtime: Long) {
    if (poolname != null && classificationname != null) {
      var poolStatAttribute = poolStats.getStatsForPool(poolname)
      var classificationStatAttribute = classificationStats.getStatsForClassification(classificationname)
      println("poolname delete : ", poolStatAttribute.currentRunningQries.get)
      println("classificationStatAttribute delete : ", classificationStatAttribute.currentRunningQries.get)

      var querytimeDifference = endtime - starttime
      setFinalStatAttribute(poolStatAttribute, querytimeDifference)
      setFinalStatAttribute(classificationStatAttribute, querytimeDifference)

      poolStats.setStatsForPool(poolname, poolStatAttribute)
      classificationStats.setStatsForClassification(classificationname, classificationStatAttribute)
    }
  }
  
  def setFinalStatAttribute(statAttribute: StatAttributes, querytimeDifference: Long) {
    statAttribute.currentRunningQries.decrementAndGet
    statAttribute.totalNumQueries.addAndGet(1)
    statAttribute.totalTimeDuration.addAndGet(querytimeDifference)
  }
   
}

class MultipleQueryPoolPolicyImpl(throttleMap : Map[String, Int], acumeContext: AcumeContext) extends QueryPoolPolicy(throttleMap, acumeContext) {
  
  override def getQueryClassification(query : String, classificationStats : ClassificationStats) : String = "default"

  override def getPoolNameForClassification(classification : String, poolStats : PoolStats) : String = "default"
  
  override def updateStats(poolname: String, classificationname: String, poolStats: PoolStats, classificationStats: ClassificationStats, starttime: Long, endtime: Long) = null
  
}

class QueryPoolPolicyImpl(throttleMap : Map[String, Int], acumeContext: AcumeContext) extends QueryPoolPolicy(throttleMap, acumeContext) {
  
  override def getQueryClassification(query : String, classificationStats : ClassificationStats) : String = "default"

  override def getPoolNameForClassification(classification : String, poolStats : PoolStats) : String = "default"
  
  override def updateFinalStats(poolname: String, classificationname: String, poolStats: PoolStats, classificationStats: ClassificationStats, starttime: Long, endtime: Long) = null
}

class QueryPoolPolicySchedulerImpl(acumeContext: AcumeContext) extends QueryPoolPolicy(Map.empty, acumeContext) {
  
  override def getQueryClassification(query : String, classificationStats : ClassificationStats) : String = "scheduler"

  override def getPoolNameForClassification(classification : String, poolStats : PoolStats) : String = "scheduler"

}

class PoolStats
{
  @volatile var stats = new ConcurrentHashMap[String, StatAttributes]()
  def getStatsForPool(name : String)=  stats.computeIfAbsent(name, new Function[String, StatAttributes]() {
    def apply(name : String) = {
	  new StatAttributes(new AtomicInteger(0), new AtomicInteger(0), new AtomicLong(0L))
    }
  })
    
  def setStatsForPool(name:String, poolStatAttribute : StatAttributes) = stats.put(name, poolStatAttribute)
}

class ClassificationStats
{
  @volatile var stats = new ConcurrentHashMap[String, StatAttributes]()
  def getStatsForClassification(name : String)=  stats.computeIfAbsent(name, new Function[String, StatAttributes]() {
    def apply(name : String) = {
	  new StatAttributes(new AtomicInteger(0), new AtomicInteger(0), new AtomicLong(0L))
    }
  })
  
  def setStatsForClassification(name:String, classificationStatAttribute : StatAttributes) = stats.put(name, classificationStatAttribute)
}

case class StatAttributes(var currentRunningQries : AtomicInteger, var totalNumQueries : AtomicInteger, var totalTimeDuration : AtomicLong)