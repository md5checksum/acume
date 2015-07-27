package com.guavus.acume.cache.sql

import com.guavus.acume.cache.utility.Tuple
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.workflow.RequestType._
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import scala.collection.mutable.{HashMap => SHashMapMutable}
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait


/**
 * @author archit.thakur
 *
 */

trait ISqlCorrector {
  
  val conf: AcumeCacheConf
  def correctSQL(acumeCacheContextTrait: AcumeCacheContextTrait, unparsedsql: String, parsedsql: Tuple2[List[Tuple], RequestType]): ((String, QueryOptionalParam), (List[Tuple], RequestType))
}

object ISqlCorrector {
  
  val hashmap = new SHashMapMutable[String, ISqlCorrector]
  def getSQLCorrector(conf: AcumeCacheConf) = {

    val sqlcorrectorKey = conf.get(ConfConstants.acumecachesqlcorrector) 
      
    hashmap.get(sqlcorrectorKey) match {
      case Some(sqlcorrector) => sqlcorrector
      case None => val acumecachesqlcorrectorclz = Class.forName(sqlcorrectorKey)
      val sqlcorrector = acumecachesqlcorrectorclz.getConstructor(classOf[AcumeCacheConf]).newInstance(conf).asInstanceOf[ISqlCorrector]
      
      hashmap.put(sqlcorrectorKey, sqlcorrector)
      sqlcorrector
    }
  }
}


