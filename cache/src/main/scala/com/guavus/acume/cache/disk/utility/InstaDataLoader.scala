package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.LevelTimestamp
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.common.DimensionTable
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.Utility
import com.guavus.insta.Insta
import com.guavus.insta.InstaCubeMetaInfo
import com.guavus.insta.InstaRequest
import scala.util.control.Breaks._
import java.util.Arrays
import java.util.concurrent.ConcurrentHashMap
import com.guavus.insta.BinPersistTimeInfoRequest
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
//import com.guavus.insta.InstaCubeMetaInfo

class InstaDataLoader(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf) extends DataLoader(acumeCacheContext, conf, null) {

  var insta: Insta = null
  val sqlContext = acumeCacheContext.cacheSqlContext
  var cubeList: List[InstaCubeMetaInfo] = null
  def init() {
    val insta = new Insta(acumeCacheContext.cacheSqlContext.sparkContext)
    insta.init(conf.get(ConfConstants.backendDbName, throw new IllegalArgumentException(" Insta DBname is necessary for loading data from insta")), conf.get(ConfConstants.cubedefinitionxml, throw new IllegalArgumentException(" Insta cubeDefinitionxml is necessary for loading data from insta")))
    cubeList = insta.getInstaCubeList
  }

  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: DimensionTable): Tuple2[SchemaRDD, String] = {
    val endTime = Utility.getNextTimeFromGranularity(levelTimestamp.timestamp, levelTimestamp.level.localId, Utility.newCalendar)
    val dimSet = getBestCubeName(businessCube, levelTimestamp.timestamp, endTime)
    val fields = CubeUtil.getCubeFields(businessCube)
    val dimensionFilters = (dimSet.dimensions).map(x => {
      if (fields.contains(x))
        1
      else
        0
    }) ++ dimSet.measures.map(x => 0)

    var measureFilters = (dimSet.dimensions ++ dimSet.measures).map(x => {
      if (fields.contains(x))
        1
      else
        0
    })
    val instaMeasuresRequest = InstaRequest(levelTimestamp.timestamp, endTime,
      businessCube.binsource, dimSet.cubeName, null, measureFilters)
    val aggregatedMeasureDataInsta = insta.getAggregatedData(instaMeasuresRequest)
    var tempstartTime = 0
    if (metadataMap.get(businessCube) == null) {
      tempstartTime = 0
      metadataMap.put(businessCube, new DataLoadedMetadata(Map(DataLoadedMetadata.dimensionSetStartTime -> "0", DataLoadedMetadata.dimensionSetEndTime -> "0")))
    } else {
      tempstartTime = metadataMap.get(businessCube).get(DataLoadedMetadata.dimensionSetEndTime).toInt
    }
    if (tempstartTime < endTime) {
      val instaDimensionRequest = InstaRequest(tempstartTime, endTime,
        businessCube.binsource, dimSet.cubeName, null, dimensionFilters)
      val aggregatedDataInsta = insta.getAggregatedData(instaDimensionRequest)
      aggregatedDataInsta.registerTempTable("aggregatedDataInsta")
      val unioned = sqlContext.sql("select * from " + dTableName.tblnm + " union all select * from aggregatedDataInsta")
      dTableName.Modify
      unioned.registerTempTable(dTableName.tblnm)
      val dataloaded = metadataMap.get(businessCube)
      dataloaded.put(DataLoadedMetadata.dimensionSetEndTime, endTime.toString)
    }
    (aggregatedMeasureDataInsta, dTableName.tblnm)
  }

  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: DimensionTable, instabase: String, instainstanceid: String): Tuple2[SchemaRDD, String] = {
    loadData(businessCube, levelTimestamp, dTableName)
  }

  def getBestCubeName(businessCube: Cube, startTime: Long, endTime: Long): InstaCubeMetaInfo = {
    val fieldnum = Integer.MAX_VALUE;
    var resultantTable = null;
    var bestCube: InstaCubeMetaInfo = null
    for (cube <- cubeList) {
      var isValidCubeGran = false
      breakable {
        for (gran <- cube.aggregationIntervals) {
          if (startTime == Utility.floorFromGranularity(startTime, gran) && endTime == Utility.getNextTimeFromGranularity(startTime, gran, Utility.newCalendar)) {
            isValidCubeGran = true
            break
          }
        }
      }
      if (isValidCubeGran && cube.binSource == businessCube.binsource && CubeUtil.getCubeFields(businessCube).toSet.subsetOf((cube.dimensions ++ cube.measures).toSet)) {
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

  private val metadataMap = new ConcurrentHashMap[Cube, DataLoadedMetadata]

  private[cache] def getMetadata(key: Cube) = metadataMap.get(key)
  private[cache] def putMetadata(key: Cube, value: DataLoadedMetadata) = metadataMap.put(key, value)
  private[cache] def getOrElseInsert(key: Cube, defaultValue: DataLoadedMetadata): DataLoadedMetadata = {

    if (getMetadata(key) == null) {

      putMetadata(key, defaultValue)
      defaultValue
    } else
      getMetadata(key)
  }

  private[cache] def getOrElseMetadata(key: Cube, defaultValue: DataLoadedMetadata): DataLoadedMetadata = {

    if (getMetadata(key) == null)
      defaultValue
    else
      getMetadata(key)
  }

  def getDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, acumeCache: AcumeCache) = {

    val dataLoaderClass = StorageType.getStorageType(conf.get(ConfConstants.storagetype)).dataClass
    val loadedClass = Class.forName(dataLoaderClass)
    val newInstance = loadedClass.getConstructor(classOf[AcumeCacheContext], classOf[AcumeCacheConf], classOf[AcumeCache]).newInstance(acumeCacheContext, conf, acumeCache)
    newInstance.asInstanceOf[DataLoader]
  }

}