package com.guavus.acume.cache.disk.utility

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.sql.SchemaRDD
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.CubeTrait
import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil
import com.guavus.insta.InstaCubeMetaInfo
import com.guavus.insta.InstaRequest

class InstaDataLoader(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, acumeCache: AcumeCache[_ <: Any, _ <: Any]) extends DataLoader(acumeCacheContext, conf, null) {

  @transient val sqlContext = acumeCacheContext.cacheSqlContext
  @transient var cubeList: List[InstaCubeMetaInfo] = InstaUtil.getInstaClient.getInstaCubeList
  @transient private val logger: Logger = LoggerFactory.getLogger(classOf[InstaDataLoader])
  
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
      }).map(x => x._1 + " as " + x._2).mkString(",")
      
    val instaDimRequest = InstaRequest(startTime, endTime,
        businessCube.superBinSource, dimSet.cubeName, List(rowFilters), measureFilters)
    logger.info("Firing aggregate query on insta "  + instaDimRequest)
    val dimensionTblTemp = "dimensionDataInstaTemp" + businessCube.getAbsoluteCubeName+ endTime
    val newTuplesRdd = InstaUtil.getInstaClient.getNewTuples(instaDimRequest)
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
        businessCube.superBinSource, dimSet.cubeName, List(), measureFilters)
        logger.info("Firing aggregate query on insta "  + instaMeasuresRequest)
      val aggregatedMeasureDataInsta = InstaUtil.getInstaClient.getAggregatedData(instaMeasuresRequest)
      val aggregatedTblTemp = "aggregatedMeasureDataInstaTemp" + businessCube.superCubeName + level + "_" + startTime
      aggregatedMeasureDataInsta.registerTempTable(aggregatedTblTemp)
      AcumeCacheContextTraitUtil.setInstaTempTable(aggregatedTblTemp)
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
      if (isValidCubeGran && cube.binSource == businessCube.superBinSource && CubeUtil.getCubeBaseFields(businessCube).toSet.subsetOf((cube.dimensions.map(_._1) ++ cube.measures.map(_._1)).toSet)) {
        if(cube.cubeName.equalsIgnoreCase(businessCube.superCubeName)) {
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
  
}
