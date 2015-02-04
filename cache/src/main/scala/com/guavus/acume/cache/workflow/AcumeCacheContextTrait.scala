package com.guavus.acume.cache.workflow

import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.AcumeCacheConf
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.common.QLType

/**
 * @author archit.thakur
 * 
 */
trait AcumeCacheContextTrait extends Serializable {
  
  @transient
  private [acume] var rrCacheLoader : RRCache = _

  def isDimension(name: String): Boolean 
  
  def acql(sql: String, qltype: String = null): AcumeCacheResponse
  
  def executeQuery(sql : String, qltype : QLType.QLType) : AcumeCacheResponse
  
  private [acume] def cacheConf : AcumeCacheConf
  
  private [acume] def cacheSqlContext() : SQLContext

  def getDefaultValue(fieldName: String): Any
  
  private[acume] def getCubeList: List[Cube] = {
    throw new NoSuchMethodException("Method not present")
  }

  private[acume] def getFieldsForCube(name: String, binsource: String): List[String] = {
    throw new NoSuchMethodException("Method not present")
  }

  private[acume] def getAggregationFunction(stringname: String): String = {
    throw new NoSuchMethodException("Method not present")
  }

  private[acume] def getCubeListContainingFields(lstfieldNames: List[String]): List[Cube] = {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getFirstBinPersistedTime(binSource : String) : Long =  {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getLastBinPersistedTime(binSource : String) : Long =  {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getBinSourceToIntervalMap(binSource : String) : Map[Long, (Long,Long)] =  {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getAllBinSourceToIntervalMap() : Map[String, Map[Long, (Long,Long)]] =  {
    throw new NoSuchMethodException("Method not present")
  }
}


