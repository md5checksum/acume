package com.guavus.acume.cache.workflow

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap

import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.core.AcumeCacheFactory
import com.guavus.acume.cache.core.CacheIdentifier
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.sql.ISqlCorrector

/**
 * @author archit.thakur
 *
 */
class AcumeCacheContext(cacheSqlContext: SQLContext, cacheConf: AcumeCacheConf) extends AcumeCacheContextTrait(cacheSqlContext, cacheConf) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeCacheContext])
  
  override val dataLoader = DataLoader.getDataLoader(this, cacheConf, null)
  
  override private [acume] def executeQuery(sql: String) = {
    
    // Parse sql
    val originalparsedsql = AcumeCacheContextTraitUtil.parseSql(sql)
    
    // Correct SQL. Remove the queryOptionalParams
    logger.info("AcumeRequest obtained " + sql)
    var correctsql = ISqlCorrector.getSQLCorrector(cacheConf).correctSQL(this, sql, (originalparsedsql._1.toList, originalparsedsql._2))
    var updatedsql = correctsql._1._1
    val queryOptionalParams = correctsql._1._2
    var updatedparsedsql = correctsql._2
    val rt = updatedparsedsql._2
    
    
    var i = ""
    val list = for(l <- updatedparsedsql._1) yield {
      val cube = l.getCubeName
      val binsource = l.getBinsource
      val startTime = l.getStartTime
      val endTime = l.getEndTime
    
      AcumeCacheContextTraitUtil.validateQuery(startTime, endTime, binsource)
      
      val key_binsource = 
        if(binsource != null)
          binsource
      else
        cacheConf.get(ConfConstants.acumecorebinsource)

      i = AcumeCacheContextTraitUtil.getTable(cube)
      updatedsql = updatedsql.replaceAll(s"$cube", s"$i")
      
      
      val id = getCube(CubeKey(cube, key_binsource))
      val idd = new CacheIdentifier()
      idd.put("cube", id.hashCode)
      
      val instance = AcumeCacheFactory.getInstance(this, cacheConf, idd, id)

      if(l.getSingleEntityKeyValueList() == null  || l.getSingleEntityKeyValueList().size == 0) {
    	  instance.createTempTableAndMetadata(List(Map[String, Any]()), startTime, endTime, rt, i,Some(queryOptionalParams))
      } else {
        val singleEntityKeys = (for(singleEntityKeys <- l.getSingleEntityKeyValueList()) yield {
          singleEntityKeys.map(x => (x._1 -> x._2.asInstanceOf[Any])).toMap
        }).toList
        instance.createTempTableAndMetadata(singleEntityKeys, startTime, endTime, rt, i,Some(queryOptionalParams))
      }
      
    }
    
    val klist = list.flatMap(_.timestamps)
    val kfg = cacheSqlContext.sql(updatedsql)
    AcumeCacheResponse(kfg, kfg.rdd, MetaData(-1, klist))
  }
  
}
