package com.guavus.acume.cache.disk.utility

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.annotation.migration
import scala.util.control.Breaks._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.CubeTrait
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.insta.BinPersistTimeInfoRequest
import com.guavus.insta.Insta
import com.guavus.insta.InstaCubeMetaInfo
import com.guavus.insta.InstaRequest
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.LevelTimestamp

class InstaDataLoader(@transient acumeCacheContext: AcumeCacheContextTrait, @transient  conf: AcumeCacheConf, @transient acumeCache: AcumeCache[_ <: Any, _ <: Any]) extends DataLoader(acumeCacheContext, conf, null) {

  @transient var insta: Insta = null
  @transient val sqlContext = acumeCacheContext.cacheSqlContext
  @transient var cubeList: List[InstaCubeMetaInfo] = null
  @transient private val logger: Logger = LoggerFactory.getLogger(classOf[InstaDataLoader])
  private var binSourceToIntervalMap = Map[String, Map[Long, (Long, Long)]]()
  @transient private val lockObject = new Object
  
  // Initialize the insta client
  init
    
  val runnable = new Runnable() {
    def run() {
      synchronizedGetAndUpdateMap
    }
  };
  
  private val service : ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  service.scheduleAtFixedRate(runnable, 0, acumeCacheContext.cacheConf.getInt(ConfConstants.instaAvailabilityPollInterval).get, TimeUnit.SECONDS);

  private def synchronizedGetAndUpdateMap() {
   lockObject.synchronized {
     binSourceToIntervalMap = getUpdatedBinSourceToIntervalMap
   }
  }
  
  private def getUpdatedBinSourceToIntervalMap() = {
    val persistTime = insta.getAllBinPersistedTimes
    println(persistTime)
    
    //Filtering out the intervals where starttime and endtime are 0
    var filteredPersistTime = Map[String, Map[Long, (Long, Long)]]()
    persistTime.map(binSrcToGranToAvailability => {
      val granToAvailability = binSrcToGranToAvailability._2
      //Filtering out the intervals where starttime and endtime are 0
      val filteredGranToAvailability : Map[Long, (Long, Long)] = granToAvailability.filter(x => x._2._1 != 0 || x._2._2 != 0)
      filteredPersistTime += (binSrcToGranToAvailability._1 -> filteredGranToAvailability)
    })

    val updatedBinSourceToIntervalMap = filteredPersistTime.map(binSourceToGranToAvailability => {
      val minGran = binSourceToGranToAvailability._2.filter(_._1 != -1).keys.min
      val granularityToAvailability = binSourceToGranToAvailability._2.map(granToAvailability => {
        if (granToAvailability._1 == -1) {
          (granToAvailability._1, (granToAvailability._2._1, Utility.getNextTimeFromGranularity(granToAvailability._2._2, minGran, Utility.newCalendar)))
        } else {
          (granToAvailability._1, (granToAvailability._2._1, Utility.getNextTimeFromGranularity(granToAvailability._2._2, granToAvailability._1, Utility.newCalendar)))
        }
      })
      (binSourceToGranToAvailability._1, granularityToAvailability ++ Map(-1L -> granularityToAvailability.get(minGran).get))
    })
    
    updatedBinSourceToIntervalMap
  }
  
  private def init() {
    //Insta constructor requires HiveContext now, so we have to do explicit typecasting here
    insta = new Insta(acumeCacheContext.cacheSqlContext.asInstanceOf[HiveContext])
    insta.init(conf.get(ConfConstants.backendDbName), conf.get(ConfConstants.cubedefinitionxml))
    cubeList = insta.getInstaCubeList
    
  }
  
  override def loadDimensionSet(keyMap : Map[String, Any], businessCube: CubeTrait, startTime : Long, endTime : Long) : SchemaRDD = {
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
        if (baseFields.contains(x._1))
          keyMap.getOrElse(x._1, null)
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
        businessCube.binSource, dimSet.cubeName, List(rowFilters), measureFilters)
    logger.info("Firing aggregate query on insta "  + instaDimRequest)
    val dimensionTblTemp = "dimensionDataInstaTemp" + businessCube.getAbsoluteCubeName+ endTime
    val newTuplesRdd = insta.getNewTuples(instaDimRequest)
    newTuplesRdd.registerTempTable(dimensionTblTemp)
    sqlContext.sql(s"select $renameToAcumeFields from $dimensionTblTemp")
  }

  override def loadData(keyMap : Map[String, Any], businessCube: CubeTrait, startTime : Long, endTime : Long, level: Long) : SchemaRDD = {
    this.synchronized {
      val dimSet = getBestCubeName(businessCube, startTime, endTime)
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
        if (baseFields.contains(x._1))
          1
        else
          0
      })
      
      val rowFilters = (dimSet.dimensions).map(x => {
        if (baseFields.contains(x._1))
          keyMap.getOrElse(x._1, null)
        else
          null
      })

      val instaMeasuresRequest = InstaRequest(startTime, endTime,
        businessCube.binSource, dimSet.cubeName, List(), measureFilters)
        logger.info("Firing aggregate query on insta "  + instaMeasuresRequest)
      val aggregatedMeasureDataInsta = insta.getAggregatedData(instaMeasuresRequest)
      val aggregatedTblTemp = "aggregatedMeasureDataInstaTemp" + businessCube.cubeName + level + "_" + startTime
      aggregatedMeasureDataInsta.registerTempTable(aggregatedTblTemp)
      AcumeCacheContextTrait.setInstaTempTable(aggregatedTblTemp)
      //change schema for this schema rdd
      val renameToAcumeFields = (for(acumeField <- fields) yield {
        acumeFieldToBaseFieldMap.get(acumeField).get -> acumeField
      }).map(x => x._1 + " as " + x._2).mkString(",")
      sqlContext.sql(s"select $renameToAcumeFields from $aggregatedTblTemp") 
    }
  }
  
  def getBestCubeName(businessCube: CubeTrait, startTime: Long, endTime: Long): InstaCubeMetaInfo = {
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
      if (isValidCubeGran && cube.binSource == businessCube.binSource && CubeUtil.getCubeBaseFields(businessCube).toSet.subsetOf((cube.dimensions.map(_._1) ++ cube.measures.map(_._1)).toSet)) {
        if(cube.cubeName.equalsIgnoreCase(businessCube.cubeName)) {
          return cube
        } else if (bestCube == null) {
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
  
  override def getAllBinSourceToIntervalMap() : Map[String, Map[Long, (Long,Long)]] =  {
    if(binSourceToIntervalMap.isEmpty) {
    	// This means its happening for the first time
      synchronizedGetAndUpdateMap
    }
    binSourceToIntervalMap
  }
}
