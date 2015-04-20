package com.guavus.acume.core

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import java.util.function.Function

abstract class QueryPoolPolicy(throttleMap : Map[String, Int]) {
  
  def getQueryClassification(query : String, classificationStats : ClassificationStats) : String = null
  
  def checkForThrottle(classification : String, classificationStats : ClassificationStats) = throttleMap.get(classification).map(throttleValue => { 
    if(classificationStats.getStatsForClassification(classification).currentRunningQries.get() >= throttleValue)
    	throw new RuntimeException("Application Throttled. Queue Full.")
    else
      null
    }).getOrElse(null)

  def getPoolNameForClassification(classification : String, poolStats : PoolStats) : String = null
   
}

class QueryPoolPolicyImpl(throttleMap : Map[String, Int]) extends QueryPoolPolicy(throttleMap) {
  
  override def getQueryClassification(query : String, classificationStats : ClassificationStats) : String = "default"

  override def getPoolNameForClassification(classification : String, poolStats : PoolStats) : String = "default"
}

class QueryPoolPolicySchedulerImpl() extends QueryPoolPolicy(Map.empty) {
  
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
