package com.guavus.acume.core.scheduler

import scala.collection.mutable.HashMap
import com.guavus.acume.core.AcumeConf
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.common.ConfConstants

/**
 * @author archit.thakur
 *
 */
abstract class ISchedulerPolicy(acumeConf: AcumeConf) {

  def getIntervalsAndLastUpdateTime(startTime: Long, endTime: Long, cubeConfiguration: PrefetchCubeConfiguration, isFirstTimeRun: Boolean, optionalParams: HashMap[String, Any], taskManager: QueryRequestPrefetchTaskManager): PrefetchLastCacheUpdateTimeAndInterval

  def getCeilOfTime(time: Long): Long

  def clearState(): Unit

}

object ISchedulerPolicy {
  
  val objectgetter = HashMap[String, ISchedulerPolicy]()
  def getISchedulerPolicy(acumeConf: AcumeConf): ISchedulerPolicy = {
    val schedulerpolicykey = ConfConstants.schedulerpolicyclass
    val ischedulerpolicy = objectgetter.getOrElse(schedulerpolicykey, Class.forName(acumeConf.get(schedulerpolicykey)).getConstructor(classOf[AcumeConf]).newInstance(acumeConf)
    .asInstanceOf[ISchedulerPolicy])
    if(!objectgetter.contains(schedulerpolicykey)) {
      objectgetter.put(schedulerpolicykey, ischedulerpolicy)
    }
    ischedulerpolicy
  }
}