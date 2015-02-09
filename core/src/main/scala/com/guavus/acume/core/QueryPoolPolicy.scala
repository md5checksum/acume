package com.guavus.acume.core

abstract class QueryPoolPolicy(acumeconf : AcumeConf) {

  def getPoolNameForQuery(query : String, poolStats : PoolStats) : String = null
  
}

class QueryPoolPolicyImpl(conf : AcumeConf) extends QueryPoolPolicy(conf)

class PoolStats
{
  val stats = Map[String, (Long , Long)]()
  def getStatsForPool(name : String)=  stats.getOrElse(name, (0, 0))
}