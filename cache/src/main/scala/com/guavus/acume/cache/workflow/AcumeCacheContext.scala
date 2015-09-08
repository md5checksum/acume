package com.guavus.acume.cache.workflow

import scala.collection.JavaConversions._

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.{Cube, ConfConstants}
import com.guavus.acume.cache.core.{AcumeCache, AcumeCacheFactory, CacheIdentifier, TimeGranularity} 
import com.guavus.acume.cache.disk.utility.{BinAvailabilityPoller, DataLoader}
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
    
      validateQuery(startTime, endTime, binsource, cacheConf.getDataSourceName, cube)
      
      i = AcumeCacheContextTraitUtil.getTable(cube)
      updatedsql = updatedsql.replaceAll(s"$cube", s"$i")
      
      val id = getCube(CubeKey(cube, binsource))
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

  private [acume] def validateQuery(startTime : Long, endTime : Long, binSource : String) {
    if(startTime < BinAvailabilityPoller.getFirstBinPersistedTime(binSource) || endTime > BinAvailabilityPoller.getLastBinPersistedTime(binSource)){
      throw new RuntimeException("Cannot serve query. StartTime and endTime doesn't fall in the availability range.")
    }
  }

  /**
   * Gets the cache instance object for retreiving acume cache values on which transformations will be applied
   * Used by customExecution path
   * @param startTime time range for which acume cache values are to be retrieved
   *                  start and end times are used for validation purposes here
   * @param endTime
   * @param cube CubeName, binsoource for which acume values are to be retrieved
   * @tparam k
   * @tparam v
   * @return
   */
  override def getCacheInstance[k, v](
      startTime: Long,
      endTime: Long,
      cube: CubeKey): AcumeCache[k, v] = {

    validateQuery(startTime, endTime, cube.binsource)

    val idd = new CacheIdentifier()
    val id = getCube(cube)
    idd.put("cube", id.hashCode)
    AcumeCacheFactory.getInstance(this, cacheConf, idd, id)
  }

  /**
   * Separate function for aggregation flow, could be used for future modifications
   * Currently same as getCacheInstance
   * @param startTime
   * @param endTime
   * @param cube
   * @tparam k
   * @tparam v
   * @return
   */
  override def getAggregateCacheInstance[k , v](
      startTime: Long,
      endTime: Long,
      cube: CubeKey): AcumeCache[k, v] = {

    getCacheInstance(startTime, endTime, cube)
  }
}
