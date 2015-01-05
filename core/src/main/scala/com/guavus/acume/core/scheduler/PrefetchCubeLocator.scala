package com.guavus.acume.core.scheduler

import scala.collection.JavaConversions._
import com.guavus.qb.cube.schema.QueryBuilderSchema
import com.guavus.qb.cube.schema.ICube
import com.guavus.acume.cache.utility.Utility

class PrefetchCubeLocator(schema : QueryBuilderSchema) {

  def getPrefetchCubeConfigurations(): scala.collection.mutable.HashMap[String, List[PrefetchCubeConfiguration]] = {
    val prefetchCubeConfigurationMap = new scala.collection.mutable.HashMap[String, List[PrefetchCubeConfiguration]]()
    for(cube <- schema.getCubes()) {
      val prefetchCubeConfiguration = new PrefetchCubeConfiguration();
      prefetchCubeConfiguration.setTopCube(cube)
      prefetchCubeConfigurationMap.put(cube.getBinSourceValue(), List(prefetchCubeConfiguration))
    }
    prefetchCubeConfigurationMap
  }
}