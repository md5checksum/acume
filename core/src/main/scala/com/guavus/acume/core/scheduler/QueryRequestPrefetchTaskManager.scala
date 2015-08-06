package com.guavus.acume.core.scheduler

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import scala.reflect.BeanProperty
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.core.Interval
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.DataService
import com.guavus.acume.threads.NamedThreadPoolFactory
import com.guavus.acume.util.GracefullShutdownExecutor
import com.guavus.qb.cube.schema.QueryBuilderSchema
import scala.collection.mutable.HashMap
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.qb.conf.QBConf
import com.guavus.qb.services.QueryBuilderService
import com.guavus.acume.core.AcumeContextTrait

object QueryRequestPrefetchTaskManager {

  private val MAX_TASK_PRODUCERS = 1
  private val MAX_TASK_COMBINERS = 1
  private val INITIAL_TASK_QUEUE_SIZE = 10000
  val logger = LoggerFactory.getLogger(classOf[QueryRequestPrefetchTaskManager])
}

class QueryRequestPrefetchTaskManager(@BeanProperty var dataService: DataService, @BeanProperty schemas: List[QueryBuilderSchema], acumeContext : AcumeContextTrait, acumeService : AcumeService, schedulerPolicy : ISchedulerPolicy, controller : Controller) {

  val logger = LoggerFactory.getLogger(classOf[QueryRequestPrefetchTaskManager])
  private var consumerCombinerThreadPool: ExecutorService = _
  private var consumerThreadPool: ExecutorService = _
  private var producerThreadPool: ScheduledExecutorService = Executors.newScheduledThreadPool(QueryRequestPrefetchTaskManager.MAX_TASK_PRODUCERS, new NamedThreadPoolFactory("PrefetchTaskProducer"))
  private var scheduledFuture: ScheduledFuture[_] = _
  private var queryPrefetchTaskProducer: QueryPrefetchTaskProducer = new QueryPrefetchTaskProducer(acumeContext, schemas, this, dataService, acumeService, false, schedulerPolicy, controller)
  /*private val acumeConf = acumeContext.acumeConf 
  private [core] val acumeCacheAvailabilityMapPolicy = ICacheAvalabiltyUpdatePolicy.getICacheAvalabiltyUpdatePolicy(acumeConf, acumeContext.hqlContext)*/
  private val combinerpriority = new PriorityBlockingQueue[Runnable](QueryRequestPrefetchTaskManager.INITIAL_TASK_QUEUE_SIZE)

  /*def oldCombinerRunning: Boolean = {
    combinerpriority.toArray().map(_.asInstanceOf[QueryPrefetchTaskCombiner]).filter(_.getIsOlderTask()).size >= 1 ||
      consumerCombinerThreadPool.asInstanceOf[CombinerExecutor].
      getRunningExecutorsRunnable.toArray.map(_.asInstanceOf[QueryPrefetchTaskCombiner]).filter(_.getIsOlderTask()).size >= 1
  }
  
  private[core] def getBinSourceToCacheAvalabilityMap: HashMap[String, HashMap[Long, Interval]] = {
    acumeCacheAvailabilityMapPolicy.getTrueCacheAvailabilityMap
  }*/ 
  
  @BeanProperty
  var binSourceToCacheAvailability: HashMap[String, HashMap[Long, Interval]] = new HashMap[String, HashMap[Long, Interval]]()

  initConsumerThreadPool()

  def getVersion(): Int = queryPrefetchTaskProducer.version.get

  def putBinClassToBinSourceToRubixAvailabiltyMap(value: HashMap[String, HashMap[Long, Interval]] = new HashMap[String, HashMap[Long, Interval]]): Boolean = {
    binSourceToCacheAvailability = value
    true
  }

  def updateBinSourceToRubixAvailabiltyMap(value: HashMap[String, HashMap[Long, Interval]]) {
     binSourceToCacheAvailability = value
  }

  def getBinClassToBinSourceToRubixAvailabiltyMapFromCoordinator() = {
    binSourceToCacheAvailability
  }

  def startPrefetchScheduler() {
    queryPrefetchTaskProducer.clearTaskCacheUpdateTimeMap()
    scheduledFuture = producerThreadPool.scheduleAtFixedRate(queryPrefetchTaskProducer, 0, acumeContext.acumeConf.getSchedulerCheckInterval, TimeUnit.SECONDS)
  }

  private def initConsumerThreadPool() {
    consumerCombinerThreadPool = new GracefullShutdownExecutor(QueryRequestPrefetchTaskManager.MAX_TASK_COMBINERS, QueryRequestPrefetchTaskManager.MAX_TASK_COMBINERS, 0L, TimeUnit.MILLISECONDS, combinerpriority, new NamedThreadPoolFactory("PrefetchScheduler"))
    consumerThreadPool = new GracefullShutdownExecutor(acumeContext.acumeConf.getSchedulerThreadPoolSize, acumeContext.acumeConf.getSchedulerThreadPoolSize, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue[Runnable](QueryRequestPrefetchTaskManager.INITIAL_TASK_QUEUE_SIZE), new NamedThreadPoolFactory("PrefetchScheduler-inner"))
  }

  def submitInnerTask(task: QueryPrefetchTask): Future[_] = consumerThreadPool.submit(task)

  def submitTask(combiner: QueryPrefetchTaskCombiner) {
    consumerCombinerThreadPool.execute(combiner)
  }

  def restartPrefetchScheduler() {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true)
      consumerCombinerThreadPool.asInstanceOf[GracefullShutdownExecutor].graceFullShutdown()
      consumerThreadPool.asInstanceOf[GracefullShutdownExecutor].graceFullShutdown()
      initConsumerThreadPool()
      queryPrefetchTaskProducer.version.incrementAndGet()
      schedulerPolicy.clearState()
//      acumeCacheAvailabilityMapPolicy.onBlockManagerRemoved
      binSourceToCacheAvailability = HashMap[String, HashMap[Long, Interval]]()
//      acumeCacheAvailabilityMapPolicy.reset
      startPrefetchScheduler()
    }
  }

  def shutdownPrefetchScheduler() {
    logger.debug("Shuting down Prefetch Scheduler")
    if (scheduledFuture != null) scheduledFuture.cancel(true)
    consumerCombinerThreadPool.asInstanceOf[GracefullShutdownExecutor].graceFullShutdown()
    consumerThreadPool.asInstanceOf[GracefullShutdownExecutor].graceFullShutdown()
  }
}
