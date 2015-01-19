package com.guavus.acume.cache.workflow

import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.AcumeCacheConf
import org.apache.spark.sql.SQLContext

/**
 * @author archit.thakur
 * 
 */
trait AcumeCacheContextTrait extends Serializable {

  private [acume] def getCubeList: List[Cube]
  def isDimension(name: String): Boolean 
  private [acume] def getFieldsForCube(name: String, binsource: String): List[String]

  private [acume] def getAggregationFunction(stringname: String) : String
  def getDefaultValue(fieldName: String): Any
  private [acume] def getCubeListContainingFields(lstfieldNames: List[String]): List[Cube]
  def acql(sql: String, qltype: String): AcumeCacheResponse
  def acql(sql: String): AcumeCacheResponse
  private [acume] def cacheConf : AcumeCacheConf
  
  private [acume] def cacheSqlContext() : SQLContext
  
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


