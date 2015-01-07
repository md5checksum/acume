package com.guavus.acume.core.scheduler

import scala.collection.JavaConversions._
import com.guavus.qb.cube.schema.QueryBuilderSchema
import com.guavus.qb.cube.schema.ICube
import com.guavus.acume.cache.utility.Utility
import scala.collection.mutable.ArrayBuffer

class PrefetchCubeLocator(schema : QueryBuilderSchema) {

  def getPrefetchCubeConfigurations(): scala.collection.mutable.HashMap[String, ArrayBuffer[PrefetchCubeConfiguration]] = {
    val prefetchCubeConfigurationMap = new scala.collection.mutable.HashMap[String, ArrayBuffer[PrefetchCubeConfiguration]]()
    for(cube <- schema.getCubes()) {
      val prefetchCubeConfiguration = new PrefetchCubeConfiguration();
      prefetchCubeConfiguration.setTopCube(cube)

      val out = prefetchCubeConfigurationMap.getOrElseUpdate(cube.getBinSourceValue(), ArrayBuffer[PrefetchCubeConfiguration]())
      out.+=(prefetchCubeConfiguration)
//      prefetchCubeConfigurationMap.put(cube.getBinSourceValue(), List(prefetchCubeConfiguration)+)
    }
    prefetchCubeConfigurationMap
  }
}