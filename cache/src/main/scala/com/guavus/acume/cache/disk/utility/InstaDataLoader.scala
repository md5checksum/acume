package com.guavus.acume.cache.disk.utility

import java.util.Arrays
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.Breaks._

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.StructField
import org.apache.spark.sql.StructType
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.LongType

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.DimensionTable
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.insta.BinPersistTimeInfoRequest
import com.guavus.insta.BinPersistTimeInfoRequest
import com.guavus.insta.Insta
import com.guavus.insta.InstaCubeMetaInfo
import com.guavus.insta.InstaRequest
import scala.collection.mutable.HashMap
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import java.util.concurrent.TimeUnit

class InstaDataLoader(@transient acumeCacheContext: AcumeCacheContextTrait, @transient  conf: AcumeCacheConf, @transient acumeCache: AcumeCache[_ <: Any, _ <: Any]) extends DataLoader(acumeCacheContext, conf, null) {

  @transient var insta: Insta = null
  @transient val sqlContext = acumeCacheContext.cacheSqlContext
  @transient var cubeList: List[InstaCubeMetaInfo] = null
  val binSourceToIntervalMap = CacheBuilder.newBuilder().refreshAfterWrite(5, TimeUnit.MINUTES)
    .build(
      new CacheLoader[String, Map[String,Map[Long, (Long,Long)]]]() {
        def load(key: String): Map[String,Map[Long, (Long,Long)]] = {
    val persistTime = insta.getAllBinPersistedTimes
    println(persistTime)
    persistTime.map(binSourceToGranToAvailability => {
      val minGran = binSourceToGranToAvailability._2.filter(_._1 != -1).keys.min
      val granularityToAvailability = binSourceToGranToAvailability._2.map(granToAvailability => {
        if (granToAvailability._1 == -1) {
          (granToAvailability._1, (granToAvailability._2._1, Utility.getNextTimeFromGranularity(granToAvailability._2._2, minGran, Utility.newCalendar)))
        } else {
          (granToAvailability._1, (granToAvailability._2._1, Utility.getNextTimeFromGranularity(granToAvailability._2._2, granToAvailability._1, Utility.newCalendar)))
        }
      })
      (binSourceToGranToAvailability._1,  granularityToAvailability ++ Map(-1L -> granularityToAvailability.get(minGran).get) 
      )
    })
  }
      });
  init
  
  def init() {
    insta = new Insta(acumeCacheContext.cacheSqlContext.sparkContext)
//    insta.init(conf.get(ConfConstants.backendDbName, throw new IllegalArgumentException(" Insta DBname is necessary for loading data from insta")), conf.get(ConfConstants.cubedefinitionxml, throw new IllegalArgumentException(" Insta cubeDefinitionxml is necessary for loading data from insta")))
    insta.init(conf.get(ConfConstants.backendDbName), conf.get(ConfConstants.cubedefinitionxml))
    cubeList = insta.getInstaCubeList
    
  }
  
  override def loadDimensionSet(keyMap : Map[String, Any], businessCube: Cube, startTime : Long, endTime : Long) : SchemaRDD = {
    val dimSet = getBestCubeName(businessCube, startTime, endTime)
    val baseFields = CubeUtil.getDimensionSet(businessCube).map(_.getBaseFieldName)
    val fields = CubeUtil.getDimensionSet(businessCube).map(_.getName)
    val measureFilters = (dimSet.dimensions ++ dimSet.measures).map(x => {
        if (baseFields.contains(x))
          1
        else
          0
      })
      
      val rowFilters = (dimSet.dimensions).map(x => {
        if (baseFields.contains(x))
          keyMap.getOrElse(x, null)
        else
          null
      })
      
      var i = -1
      val baseFieldToAcumeFieldMap = Map[String, String]() ++ (for(acumeField <- fields) yield {
    	 i+=1
        baseFields(i) -> acumeField
      })
      i = -1
      val acumeFieldToBaseFieldMap = Map[String, String]() ++ (for(acumeField <- fields) yield {
    	 i+=1
        acumeField -> baseFields(i)
      })
      val renameToAcumeFields = (for(acumeField <- fields) yield {
        acumeFieldToBaseFieldMap.get(acumeField).get -> acumeField
//        acumeField -> acumeFieldToBaseFieldMap.get(acumeField).get
      }).map(x => x._1 + " as " + x._2).mkString(",")
      
    val instaDimRequest = InstaRequest(startTime, endTime,
        businessCube.binsource, dimSet.cubeName, List(rowFilters), measureFilters)
        print("Firing aggregate query on insta "  + instaDimRequest)
        val dimensionTblTemp = "dimensionDataInstaTemp" + businessCube.cubeName+ endTime
    val newTuplesRdd = insta.getNewTuples(instaDimRequest)
    sqlContext.registerRDDAsTable(newTuplesRdd, dimensionTblTemp)
    sqlContext.sql(s"select $renameToAcumeFields from $dimensionTblTemp")
  }

  override def loadData(keyMap : Map[String, Any], businessCube: Cube, levelTimestamp: LevelTimestamp): SchemaRDD = {
    this.synchronized {
      val endTime = Utility.getNextTimeFromGranularity(levelTimestamp.timestamp, levelTimestamp.level.localId, Utility.newCalendar)
      val dimSet = getBestCubeName(businessCube, levelTimestamp.timestamp, endTime)
      val fields = CubeUtil.getCubeFields(businessCube)
      val baseFields = CubeUtil.getCubeBaseFields(businessCube)
      var i = -1
      val baseFieldToAcumeFieldMap = Map[String, String]() ++ (for(acumeField <- fields) yield {
    	 i+=1
        baseFields(i) -> acumeField
      })
      i = -1
      val acumeFieldToBaseFieldMap = Map[String, String]() ++ (for(acumeField <- fields) yield {
    	 i+=1
        acumeField -> baseFields(i)
      })

      val measureFilters = (dimSet.dimensions ++ dimSet.measures).map(x => {
        if (baseFields.contains(x))
          1
        else
          0
      })
      
      val rowFilters = (dimSet.dimensions).map(x => {
        if (baseFields.contains(x))
          keyMap.getOrElse(x, null)
        else
          null
      })

      val instaMeasuresRequest = InstaRequest(levelTimestamp.timestamp, endTime,
        businessCube.binsource, dimSet.cubeName, List(), measureFilters)
        print("Firing aggregate query on insta "  + instaMeasuresRequest)
      val aggregatedMeasureDataInsta = insta.getAggregatedData(instaMeasuresRequest)
      val aggregatedTblTemp = "aggregatedMeasureDataInstaTemp" + levelTimestamp.level + "_" + levelTimestamp.timestamp
      sqlContext.registerRDDAsTable(aggregatedMeasureDataInsta, aggregatedTblTemp)
      //change schema for this schema rdd
      val renameToAcumeFields = (for(acumeField <- fields) yield {
        acumeFieldToBaseFieldMap.get(acumeField).get -> acumeField
      }).map(x => x._1 + " as " + x._2).mkString(",")
      sqlContext.sql(s"select $renameToAcumeFields from $aggregatedTblTemp") 
    }
  }
  
  
  def getBestCubeName(businessCube: Cube, startTime: Long, endTime: Long): InstaCubeMetaInfo = {
    val fieldnum = Integer.MAX_VALUE;
    var resultantTable = null;
    var bestCube: InstaCubeMetaInfo = null
    for (cube <- cubeList) {
      var isValidCubeGran = false
      breakable {
        for (gran <- cube.aggregationIntervals.filter(_ != -1)) {
          if (startTime == Utility.floorFromGranularity(startTime, gran)) {
            var tempEndTime = Utility.getNextTimeFromGranularity(startTime, gran, Utility.newCalendar)
            while(tempEndTime < endTime) tempEndTime = Utility.getNextTimeFromGranularity(tempEndTime, gran, Utility.newCalendar)
            if(tempEndTime == endTime) {
            	isValidCubeGran = true
            	break
            }
          }
        }
      }
      if (isValidCubeGran && cube.binSource == businessCube.binsource && CubeUtil.getCubeBaseFields(businessCube).toSet.subsetOf((cube.dimensions ++ cube.measures).toSet)) {
        if (bestCube == null) {
          bestCube = cube
        } else {
          if (bestCube.dimensions.size > cube.dimensions.size) {
            bestCube = cube
          } else if (bestCube.dimensions.size == cube.dimensions.size) {
            if (bestCube.measures.size > cube.measures.size) {
              bestCube = cube
            }
          }
        }
      }
    }
    if (bestCube == null) {
      throw new IllegalArgumentException("Could not find any cube for fields " + CubeUtil.getCubeFields(businessCube))
    }
    bestCube
  }

  def getDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, acumeCache: AcumeCache[_ <: Any, _ <: Any]) = {

    val dataLoaderClass = StorageType.getStorageType(conf.get(ConfConstants.storagetype)).dataClass
    val loadedClass = Class.forName(dataLoaderClass)
    val newInstance = loadedClass.getConstructor(classOf[AcumeCacheContext], classOf[AcumeCacheConf], classOf[AcumeCache[_ <: Any, _ <: Any]]).newInstance(acumeCacheContext, conf, acumeCache)
    newInstance.asInstanceOf[DataLoader]
  }
  override def getFirstBinPersistedTime(binSource : String) : Long =  {
	  		val granToIntervalMap = getBinSourceToIntervalMap(binSource)
		  granToIntervalMap.get(-1).getOrElse(throw new IllegalArgumentException("No Data found in insta for default gran -1 and binSource :" + binSource))._1
  }
  
  override def getLastBinPersistedTime(binSource : String) : Long =  {
	  		val granToIntervalMap = getBinSourceToIntervalMap(binSource)
		   granToIntervalMap.get(-1).getOrElse(throw new IllegalArgumentException("No Data found in insta for default gran -1 and binSource :" + binSource))._2
  }
  
  override def getBinSourceToIntervalMap(binSource : String) : Map[Long, (Long,Long)] =  {
		  getAllBinSourceToIntervalMap.getOrElse(binSource, throw new IllegalArgumentException("No Data found for binSource " +  binSource))
  }
  
  override def getAllBinSourceToIntervalMap() : Map[String,Map[Long, (Long,Long)]] =  {
    binSourceToIntervalMap.get("binSourceToIntervalMap")
  }
}
