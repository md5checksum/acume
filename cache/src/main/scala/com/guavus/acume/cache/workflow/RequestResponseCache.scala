package com.guavus.acume.cache.workflow

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import net.sf.jsqlparser.statement.select.PlainSelect
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.utility.SQLParserFactory
import java.io.StringReader
import com.guavus.acume.cache.common.QLType
import net.sf.jsqlparser.statement.select.Select

class RequestResponseCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf) extends RRCache {

  val cache = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
    .maximumSize(conf.getInt(ConfConstants.rrsize._1, ConfConstants.rrsize._2))
    .build(
      new CacheLoader[RRCacheKey, AcumeCacheResponse]() {
        def load(input: RRCacheKey) = {
          acumeCacheContext.utilQL(input.qlstring, input.qltype)
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
      case that: com.guavus.acume.cache.workflow.RRCacheKey => that.canEqual(RRCacheKey.this) && select == that.select && qltype == that.qltype
      case _ => false
    }
  }
  
  override def hashCode() = {
    val prime = 41
    prime * (prime + select.hashCode) + qltype.hashCode
  }
}