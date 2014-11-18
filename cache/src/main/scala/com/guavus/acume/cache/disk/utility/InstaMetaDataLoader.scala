package com.guavus.acume.cache.disk.utility

import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.Cube

/**
 * @author archit.thakur
 *
 */
class InstaMetaDataLoader extends MetaDataLoader {

  override def loadMetaData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: String): SchemaRDD = null
}