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
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.threads.NamedThreadPoolFactory
import com.guavus.acume.util.GracefullShutdownExecutor
import com.guavus.qb.cube.schema.QueryBuilderSchema

object QueryRequestPrefetchTaskManager {

  private val MAX_TASK_PRODUCERS = 1

  private val INITIAL_TASK_QUEUE_SIZE = 10000

  val logger = LoggerFactory.getLogger(classOf[QueryRequestPrefetchTaskManager])

//  def getInstance(): QueryRequestPrefetchTaskManager = {
//    ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager])
//  }
  
//  def main(args: Array[String]) {
//    
//    val querybuilderservice = new DummyQueryBuilderService
////    val acumeContext: AcumeContextTrait = new DummyContext
////    val dataservice = new DataService(List(querybuilderservice), acumeContext)
//    
//    val querybuilder = new DummyQueryBuilderSchema
//    
//    val acumeconf = new AcumeConf(true, this.getClass.getResourceAsStream("/acume.conf"))
////    acumeconf.set
//    
////    val acumeservice = new AcumeService(dataservice);
//    
//    val schedulerpolicy = new VariableGranularitySchedulerPolicy(acumeconf)
//    
////    val x = new QueryRequestPrefetchTaskManager(dataservice, querybuilder, acumeconf, acumeservice, schedulerpolicy)
////    x.startPrefetchScheduler
//  
//  }
}

class QueryRequestPrefetchTaskManager(@BeanProperty var dataService: DataService, @BeanProperty schema: QueryBuilderSchema, acumeConf : AcumeConf, acumeService : AcumeService, schedulerPolicy : ISchedulerPolicy) {

  private var consumerCombinerThreadPool: ExecutorService = _

  private var consumerThreadPool: ExecutorService = _

  private var producerThreadPool: ScheduledExecutorService = Executors.newScheduledThreadPool(QueryRequestPrefetchTaskManager.MAX_TASK_PRODUCERS, new NamedThreadPoolFactory("PrefetchTaskProducer"))

  private var scheduledFuture: ScheduledFuture[_] = _

  private var queryPrefetchTaskProducer: QueryPrefetchTaskProducer = new QueryPrefetchTaskProducer(acumeConf, schema, this, dataService, acumeService, false, schedulerPolicy)

  @BeanProperty
  var binSourceToRubixAvailability: scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[Long, Interval]] = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[Long, Interval]]()

  initConsumerThreadPool()

  def getVersion(): Int = queryPrefetchTaskProducer.version.get

  def putBinClassToBinSourceToRubixAvailabiltyMap(value: scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[Long, Interval]] = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[Long, Interval]]): Boolean = {
    binSourceToRubixAvailability = value
    true
  }

  def updateBinSourceToRubixAvailabiltyMap(value: scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[Long, Interval]]) {
     binSourceToRubixAvailability = value
  }

  def getBinClassToBinSourceToRubixAvailabiltyMapFromCoordinator() {
   binSourceToRubixAvailability
  }

  def startPrefetchScheduler() {
    queryPrefetchTaskProducer.clearTaskCacheUpdateTimeMap()
    scheduledFuture = producerThreadPool.scheduleAtFixedRate(queryPrefetchTaskProducer, 0, acumeConf.getSchedulerCheckInterval, TimeUnit.SECONDS)
  }

  private def initConsumerThreadPool() {
    consumerCombinerThreadPool = new GracefullShutdownExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new PriorityBlockingQueue[Runnable](QueryRequestPrefetchTaskManager.INITIAL_TASK_QUEUE_SIZE), new NamedThreadPoolFactory("PrefetchScheduler"))
    consumerThreadPool = new GracefullShutdownExecutor(acumeConf.getSchedulerThreadPoolSize, acumeConf.getSchedulerThreadPoolSize, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue[Runnable](QueryRequestPrefetchTaskManager.INITIAL_TASK_QUEUE_SIZE), new NamedThreadPoolFactory("PrefetchScheduler-inner"))
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
      ConfigFactory.getInstance.getBean(classOf[ISchedulerPolicy]).clearState()
      binSourceToRubixAvailability = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[Long, Interval]]()
      startPrefetchScheduler()
    }
  }

  def shutdownPrefetchScheduler() {
//    logger.debug("Shuting down Prefetch Scheduler")
    if (scheduledFuture != null) scheduledFuture.cancel(true)
    consumerCombinerThreadPool.asInstanceOf[GracefullShutdownExecutor].graceFullShutdown()
    consumerThreadPool.asInstanceOf[GracefullShutdownExecutor].graceFullShutdown()
  }
}