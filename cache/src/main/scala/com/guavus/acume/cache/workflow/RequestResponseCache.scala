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

class RequestResponseCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf) extends RRCache {

  val cache = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
    .maximumSize(conf.get(ConfConstants.rrsize._1).toInt)
    .build(
      new CacheLoader[(PlainSelect, QLType.QLType), AcumeCacheResponse]() {
        def load(input: (PlainSelect, QLType.QLType)) = {
          acumeCacheContext.utilQL(input._1.toString, input._2)
        }
      });

  def getRdd(input: (String, QLType.QLType)) = {
    val sql = SQLParserFactory.getParserManager()
    val statement = sql.parse(new StringReader(input._1)).asInstanceOf[PlainSelect]
    cache.get((statement, input._2))
  }
}

trait RRCache {
  def getRdd(input: (String, QLType.QLType)): AcumeCacheResponse
}