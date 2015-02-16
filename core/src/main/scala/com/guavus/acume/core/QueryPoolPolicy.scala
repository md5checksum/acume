package com.guavus.acume.core

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

abstract class QueryPoolPolicy() {
  
  def getQueryClassification(query : String, classificationStats : ClassificationStats) : String = null

  def getPoolNameForClassification(classification : String, poolStats : PoolStats) : String = null
  
}

class QueryPoolPolicyImpl() extends QueryPoolPolicy()

class PoolStats
{
  @volatile var stats = Map[String, StatAttributes]()
  def getStatsForPool(name : String)=  stats.getOrElse(name, new StatAttributes(new AtomicInteger(0), new AtomicInteger(0), new AtomicLong(0L)))
  
  def setStatsForPool(name:String, poolStatAttribute : StatAttributes) = stats += (name -> poolStatAttribute)
}

class ClassificationStats
{
  @volatile var stats = Map[String, StatAttributes]()
  def getStatsForClassification(name : String)=  stats.getOrElse(name, new StatAttributes(new AtomicInteger(0), new AtomicInteger(0), new AtomicLong(0L)))
  
  def setStatsForClassification(name:String, classificationStatAttribute : StatAttributes) = stats += (name -> classificationStatAttribute)
}

case class StatAttributes(var currentRunningQries : AtomicInteger, var totalNumQueries : AtomicInteger, var totalTimeDuration : AtomicLong)