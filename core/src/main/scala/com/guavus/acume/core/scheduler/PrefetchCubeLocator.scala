package com.guavus.acume.core.scheduler

import scala.collection.JavaConversions._
import com.guavus.qb.cube.schema.QueryBuilderSchema
import com.guavus.qb.cube.schema.ICube
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.utility.Utility

class PrefetchCubeLocator(schemas: List[QueryBuilderSchema]) {

  def getPrefetchCubeConfigurations(): HashMap[String, ArrayBuffer[PrefetchCubeConfiguration]] = {
    val prefetchCubeConfigurationMap = new HashMap[String, ArrayBuffer[PrefetchCubeConfiguration]]()
    for (schema <- schemas) {
      for (cube <- schema.getCubes()) {
        val prefetchCubeConfiguration = new PrefetchCubeConfiguration();
        prefetchCubeConfiguration.setTopCube(cube)
        val out = prefetchCubeConfigurationMap.getOrElseUpdate(cube.getBinSourceValue(), ArrayBuffer[PrefetchCubeConfiguration]())
        out.+=(prefetchCubeConfiguration)
      }
    }
    prefetchCubeConfigurationMap
  }
}