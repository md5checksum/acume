package com.guavus.acume.cache.disk.utility

import org.apache.spark.sql.SchemaRDD

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.CubeTrait
import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.insta.InstaCubeMetaInfo
import com.guavus.insta.InstaRequest

class InstaDataLoaderThinAcume(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, acumeCache: AcumeCache[_ <: Any, _ <: Any]) extends InstaDataLoader(acumeCacheContext, conf, acumeCache) {

  override def loadData(keyMap : Map[String, Any], businessCube: CubeTrait, startTime : Long, endTime : Long, level: Long): SchemaRDD = {
    var dimSet  :InstaCubeMetaInfo = null 
      for(cube <- cubeList) {
      if(cube.cubeName.equalsIgnoreCase(businessCube.superCubeName)) {
        dimSet = cube
      }
    }
    val measureFilters = (dimSet.dimensions ++ dimSet.measures).map(x => {1})

    val instaMeasuresRequest = InstaRequest(startTime, endTime,
      businessCube.superBinSource, businessCube.superCubeName, List(), measureFilters)
    print("Firing aggregate query on insta " + instaMeasuresRequest)
    InstaUtil.getInstaClient.getAggregatedData(instaMeasuresRequest)
  }

}