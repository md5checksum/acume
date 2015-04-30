package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import org.apache.spark.sql.SchemaRDD
import com.guavus.insta.InstaRequest
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.common.CubeTrait
import com.guavus.insta.InstaCubeMetaInfo

class InstaDataLoaderThinAcume(@transient acumeCacheContext: AcumeCacheContextTrait, @transient conf: AcumeCacheConf, @transient acumeCache: AcumeCache[_ <: Any, _ <: Any]) extends InstaDataLoader(acumeCacheContext, conf, acumeCache) {

  override def loadData(keyMap: Map[String, Any], businessCube: CubeTrait, startTime : Long, endTime : Long): SchemaRDD = {
    var dimSet  :InstaCubeMetaInfo = null 
      for(cube <- cubeList) {
      if(cube.cubeName.equalsIgnoreCase(businessCube.cubeName)) {
        dimSet = cube
      }
    }
    val measureFilters = (dimSet.dimensions ++ dimSet.measures).map(x => {1})

    val instaMeasuresRequest = InstaRequest(startTime, endTime,
      businessCube.binSource, businessCube.cubeName, List(), measureFilters)
    print("Firing aggregate query on insta " + instaMeasuresRequest)
    insta.getAggregatedData(instaMeasuresRequest)
  }

}