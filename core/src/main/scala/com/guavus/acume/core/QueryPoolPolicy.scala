package com.guavus.acume.core

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._

abstract class QueryPoolPolicy() {

  def getQueryClassification(query : String, classificationStats : ClassificationStats) : String = null

  def getPoolNameForClassification(classification : String, poolStats : PoolStats) : String = null

}

class QueryPoolPolicyImpl() extends QueryPoolPolicy()

class PoolStats
{
  @volatile var stats = new ConcurrentHashMap[String, StatAttributes]()
  def getStatsForPool(name : String) =  {
    stats.putIfAbsent(name, new StatAttributes(new AtomicInteger(0), new AtomicInteger(0), new AtomicLong(0L)))
    stats.get(name)
  }

  def setStatsForPool(name:String, poolStatAttribute : StatAttributes) = stats.put(name, poolStatAttribute)
}

class ClassificationStats
{
  @volatile var stats = new ConcurrentHashMap[String, StatAttributes]()
  def getStatsForClassification(name : String)=  {
    stats.putIfAbsent(name, new StatAttributes(new AtomicInteger(0), new AtomicInteger(0), new AtomicLong(0L)))
    stats.get(name)
  }

  def setStatsForClassification(name:String, classificationStatAttribute : StatAttributes) = stats.put(name, classificationStatAttribute)
}

case class StatAttributes(var currentRunningQries : AtomicInteger, var totalNumQueries : AtomicInteger, var totalTimeDuration : AtomicLong)