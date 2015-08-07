package com.guavus.acume.core.scheduler

import scala.collection.mutable.HashMap
import com.guavus.acume.core.AcumeConf
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.core.AcumeContextTraitUtil

/**
 * @author archit.thakur
 *
 */
abstract class ISchedulerPolicy {

  def getIntervalsAndLastUpdateTime(startTime: Long, endTime: Long, cubeConfiguration: PrefetchCubeConfiguration, isFirstTimeRun: Boolean, optionalParams: HashMap[String, Any], taskManager: QueryRequestPrefetchTaskManager): PrefetchLastCacheUpdateTimeAndInterval

  def getCeilOfTime(time: Long): Long

  def clearState(): Unit

}

object ISchedulerPolicy {
  
  val objectgetter = HashMap[String, ISchedulerPolicy]()
  def getISchedulerPolicy : ISchedulerPolicy = {
    val schedulerpolicykey = ConfConstants.schedulerPolicyClass
    val ischedulerpolicy = objectgetter.getOrElse(schedulerpolicykey, Class.forName(AcumeContextTraitUtil.acumeConf.get(schedulerpolicykey)).getConstructor(classOf[AcumeConf]).newInstance()
    .asInstanceOf[ISchedulerPolicy])
    if(!objectgetter.contains(schedulerpolicykey)) {
      objectgetter.put(schedulerpolicykey, ischedulerpolicy)
    }
    ischedulerpolicy
  }
}