package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.disk.utility.InstaDataLoaderThinAcume
import com.guavus.acume.cache.utility.Utility

/**
 * @author kashish.jain
 *
 */
class AcumeHiveCacheContext(cacheSqlContext: SQLContext, cacheConf: AcumeCacheConf) extends AcumeCacheContextTrait(cacheSqlContext, cacheConf) { 
 
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeHiveCacheContext])
  private val useInsta : Boolean = cacheConf.getBoolean(ConfConstants.useInsta).getOrElse(false)
  
  override val dataLoader : DataLoader = {
    if(!useInsta)
      null
    else
      new InstaDataLoaderThinAcume(this, cacheConf, null)
  }

  override private[acume] def executeQuery(sql: String) = {

    logger.info("AcumeRequest obtained " + sql)

    var (timestamps, correctsql, level) = getTimestampsAndSql(sql)

    var updatedsql = correctsql._1._1
    val updatedparsedsql = correctsql._2
    val cube = updatedparsedsql._1(0).getCubeName
    val binsource = updatedparsedsql._1(0).getBinsource
    val rt = updatedparsedsql._2
    val startTime = updatedparsedsql._1(0).getStartTime
    val endTime = updatedparsedsql._1(0).getEndTime

    if (!useInsta) {
      //Replace the ts with "timestamp"
      val tsRegex = "\\b" + "ts" + "\\b"
      val tsReplacedSql = updatedsql.replaceAll(tsRegex, "timestamp")
      
      executeThinClientQuery(tsReplacedSql, timestamps)
    } else {
      var tableName = AcumeCacheContextTraitUtil.getTable(cube)
      updatedsql = updatedsql.replaceAll(s"$cube", s"$tableName")

      executeThickClientQuery(updatedsql, timestamps, cube, binsource, rt, startTime, endTime, level, tableName)
    }
  }
  
}
