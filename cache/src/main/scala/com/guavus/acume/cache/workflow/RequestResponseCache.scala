package com.guavus.acume.cache.workflow

import java.io.StringReader

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.QLType
import com.guavus.acume.cache.utility.SQLParserFactory

import net.sf.jsqlparser.statement.select.Select

/**
 * @author archit.thakur
 *
 */
class RequestResponseCache(acumeCacheContextTrait: AcumeCacheContextTrait, conf: AcumeCacheConf) extends RRCache {

  val cache = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
    .maximumSize(conf.getInt(ConfConstants.rrsize._1, ConfConstants.rrsize._2)).removalListener(new RemovalListener[RRCacheKey, AcumeCacheResponse] {
	  def onRemoval(notification : RemovalNotification[RRCacheKey, AcumeCacheResponse]) {
	    notification.getValue().rowRDD.unpersist(true)
	  }
  }).build(
      new CacheLoader[RRCacheKey, AcumeCacheResponse]() {
        def load(input: RRCacheKey) = {
          val response = acumeCacheContextTrait.executeQuery(input.qlstring, input.qltype)
          val cachedRDD = response.rowRDD.cache
//          cachedRDD.checkpoint
          response
        }
      });

  def getRdd(input: (String, QLType.QLType)) = {
    val sql = SQLParserFactory.getParserManager()
    val statement = sql.parse(new StringReader(input._1)).asInstanceOf[Select]
    
    cache.get(RRCacheKey(statement, input._1, input._2))
  }
}

trait RRCache {
  def getRdd(input: (String, QLType.QLType)): AcumeCacheResponse
}

case class RRCacheKey(select: Select, qlstring: String, qltype: QLType.QLType) extends Equals {
  def canEqual(other: Any) = {
    other.isInstanceOf[com.guavus.acume.cache.workflow.RRCacheKey]
  }
  
  override def equals(other: Any) = {
    other match {
      case that: com.guavus.acume.cache.workflow.RRCacheKey => that.canEqual(RRCacheKey.this) && select.toString.equals(that.select.toString) && qltype == that.qltype
      case _ => false
    }
  }
  
  override def hashCode() = {
    val prime = 41
    prime * (prime + select.toString.hashCode) + qltype.hashCode
  }
}