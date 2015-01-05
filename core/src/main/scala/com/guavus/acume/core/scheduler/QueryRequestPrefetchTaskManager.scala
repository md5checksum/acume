package com.guavus.acume.core.scheduler

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import org.infinispan.remoting.transport.Address
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.threads.NamedThreadPoolFactory
import QueryRequestPrefetchTaskManager._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import scala.collection.JavaConversions._
import com.guavus.acume.core.DataService
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.core.Interval
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.querybuilder.cube.schema.QueryBuilderSchema
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeService
import com.guavus.acume.util.GracefullShutdownExecutor

object QueryRequestPrefetchTaskManager {

  private val MAX_TASK_PRODUCERS = 1

  private val INITIAL_TASK_QUEUE_SIZE = 10000

  val logger = LoggerFactory.getLogger(classOf[QueryRequestPrefetchTaskManager])

//  def getInstance(): QueryRequestPrefetchTaskManager = {
//    ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager])
//  }
}

class QueryRequestPrefetchTaskManager(@BeanProperty var dataService: DataService, @BeanProperty schema: QueryBuilderSchema, acumeConf : AcumeConf, acumeService : AcumeService, schedulerPolicy : ISchedulerPolicy) {

  private var consumerCombinerThreadPool: ExecutorService = _

  private var consumerThreadPool: ExecutorService = _

  private var producerThreadPool: ScheduledExecutorService = Executors.newScheduledThreadPool(MAX_TASK_PRODUCERS, new NamedThreadPoolFactory("PrefetchTaskProducer"))

  private var scheduledFuture: ScheduledFuture[_] = _

  private var queryPrefetchTaskProducer: QueryPrefetchTaskProducer = new QueryPrefetchTaskProducer(acumeConf, schema, this, dataService, acumeService, true, schedulerPolicy)

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
    consumerCombinerThreadPool = new GracefullShutdownExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new PriorityBlockingQueue[Runnable](INITIAL_TASK_QUEUE_SIZE), new NamedThreadPoolFactory("PrefetchScheduler"))
    consumerThreadPool = new GracefullShutdownExecutor(acumeConf.getSchedulerThreadPoolSize, acumeConf.getSchedulerThreadPoolSize, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue[Runnable](INITIAL_TASK_QUEUE_SIZE), new NamedThreadPoolFactory("PrefetchScheduler-inner"))
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
    logger.debug("Shuting down Prefetch Scheduler")
    if (scheduledFuture != null) scheduledFuture.cancel(true)
    consumerCombinerThreadPool.asInstanceOf[GracefullShutdownExecutor].graceFullShutdown()
    consumerThreadPool.asInstanceOf[GracefullShutdownExecutor].graceFullShutdown()
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.transport.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guavus.rubix.cache.Interval;
import com.guavus.rubix.configuration.ConfigFactory;
import com.guavus.rubix.configuration.IGenericConfig;
import com.guavus.rubix.configuration.RubixProperties;
import com.guavus.rubix.core.distribution.RubixDistribution;
import com.guavus.rubix.query.DataService;
import com.guavus.rubix.threads.NamedThreadPoolFactory;
import com.guavus.rubix.util.DistributedTaskType;
import com.guavus.rubix.util.GenericExecutorTask;
import com.guavus.rubix.util.GracefullShutdownExecutor;
import com.guavus.rubix.util.Utility;
import com.guavus.rubix.workflow.ICubeManager;

|**
 * @author akhil.swain
 * 
 *|
public class QueryRequestPrefetchTaskManager {
	private ExecutorService consumerCombinerThreadPool;
	private ExecutorService consumerThreadPool;
	private DataService dataService;
	private ScheduledExecutorService producerThreadPool;
	private ScheduledFuture<?> scheduledFuture;
	private ICubeManager cubeManager;
	private static final int MAX_TASK_PRODUCERS = 1;
	private static final int INITIAL_TASK_QUEUE_SIZE = 10000;
    private QueryPrefetchTaskProducer queryPrefetchTaskProducer;
    private Map<String,Map<String, Map<Long,Interval>>> binSourceToRubixAvailability;
    public static final Logger logger = LoggerFactory.getLogger(QueryRequestPrefetchTaskManager.class);

    public QueryRequestPrefetchTaskManager(IGenericConfig iGenericConfig, DataService dataService,
			ICubeManager cubeManager) {
		this.dataService = dataService;
		this.cubeManager = cubeManager;
		initConsumerThreadPool();
		this.producerThreadPool = Executors.newScheduledThreadPool(
				MAX_TASK_PRODUCERS, new NamedThreadPoolFactory(
						"PrefetchTaskProducer"));
		binSourceToRubixAvailability=new HashMap<String,Map<String, Map<Long,Interval>>>();
        queryPrefetchTaskProducer = new QueryPrefetchTaskProducer(iGenericConfig, this, dataService);
	}
	
    
    public int getVersion() {
    	return queryPrefetchTaskProducer.version.get();
    }
    
	public Map<String, Map<String, Map<Long, Interval>>> getbinSourceToRubixAvailability() {
		return binSourceToRubixAvailability;
	}

	public boolean putBinClassToBinSourceToRubixAvailabiltyMap(Map<String,Map<String, Map<Long,Interval>>> value){
		binSourceToRubixAvailability =  value;
		return true;
	}
	
	public void updateBinClassToBinSourceToRubixAvailabiltyMap(Map<String,Map<String, Map<Long,Interval>>> value){
		GenericExecutorTask task = new GenericExecutorTask(value,DistributedTaskType.PUT_RUBIX_DATA_AVAILABILTY);
		List<Future<Object>> results = RubixDistribution.getInstance().submitEverywhere(task);
		try {
			for (Future<Object> result : results) {
				// merge the results here
				Address responseFromAddress = (Address)result.get();
				if(responseFromAddress!=null)
					logger.info("Availabilty map update on {}",responseFromAddress);
			}
		}catch (ExecutionException e) {
			Utility.throwIfRubixException(e);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			Utility.throwIfRubixException(e);
			throw new RuntimeException(e);
		}
	}
	
	public void getBinClassToBinSourceToRubixAvailabiltyMapFromCoordinator(){
		GenericExecutorTask task = new GenericExecutorTask(DistributedTaskType.GET_RUBIX_DATA_AVAILABILTY);
		Future<Object> result = RubixDistribution
				.getInstance().submitOnCoordinator(task);
		try {
			@SuppressWarnings("unchecked")
			Map<String, Map<String, Map<Long, Interval>>> value = (Map<String, Map<String, Map<Long, Interval>>>)result.get();
			if(value!=null){
				logger.info("Got availablity map " +value);
				binSourceToRubixAvailability=value;
			}
		}catch (ExecutionException e) {
			Utility.throwIfRubixException(e);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			Utility.throwIfRubixException(e);
			throw new RuntimeException(e);
		}
	}
	
	
    public void startPrefetchScheduler() {
    	try {
    		ConfigFactory.getInstance().getBean(RubixDistribution.class).getCountDownLatch().await();
		} catch (InterruptedException e) {
		}
        queryPrefetchTaskProducer.clearTaskCacheUpdateTimeMap();
        scheduledFuture = producerThreadPool.scheduleAtFixedRate(
            queryPrefetchTaskProducer, 0,
            RubixProperties.DataPrefetchSchedulerCheckInterval.getLongValue(),
            TimeUnit.SECONDS);
    }
    
    private void initConsumerThreadPool(){
    	consumerCombinerThreadPool = new GracefullShutdownExecutor(
				1,
				1, 0L,
				TimeUnit.MILLISECONDS, new PriorityBlockingQueue<Runnable>(
						INITIAL_TASK_QUEUE_SIZE), new NamedThreadPoolFactory(
						"PrefetchScheduler"));
    	
    	consumerThreadPool = new GracefullShutdownExecutor(
				RubixProperties.SchedulerThreadPoolSize.getIntValue(),
				RubixProperties.SchedulerThreadPoolSize.getIntValue(), 0L,
				TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(
						INITIAL_TASK_QUEUE_SIZE), new NamedThreadPoolFactory(
						"PrefetchScheduler-inner"));
    }
    
    public Future submitInnerTask(QueryPrefetchTask task) {
    	return consumerThreadPool.submit(task);
    }
    
	public static QueryRequestPrefetchTaskManager getInstance() {
		return ConfigFactory.getInstance().getBean(QueryRequestPrefetchTaskManager.class);
	}

	public void submitTask(QueryPrefetchTaskCombiner combiner) {
		// NOTE: using execute instead of submit because submit will transform
		// the task to Futuretask which won't work with PriorityBlockingQueue.
		consumerCombinerThreadPool.execute(combiner);
	}

	|**
	 * @return the dataService
	 *|
	public DataService getDataService() {
		return dataService;
	}

	public ICubeManager getCubeManager() {
		return cubeManager;
	}

    public void restartPrefetchScheduler() {
        // cancel already running task and reset the scheduler
		if (scheduledFuture != null) {
			scheduledFuture.cancel(true);
			((GracefullShutdownExecutor) consumerCombinerThreadPool).graceFullShutdown();
			((GracefullShutdownExecutor) consumerThreadPool).graceFullShutdown();
			initConsumerThreadPool();
			queryPrefetchTaskProducer.version.incrementAndGet();
			//clear policy maps
			ConfigFactory.getInstance().getBean(ISchedulerPolicy.class).clearState();
			binSourceToRubixAvailability=new HashMap<String,Map<String, Map<Long,Interval>>>();
			startPrefetchScheduler();
		}
    }
    
    public void shutdownPrefetchScheduler() {
        // cancel already running task and reset the scheduler
    	logger.debug("Shuting down Prefetch Scheduler");
        if(scheduledFuture != null)
        	scheduledFuture.cancel(true);
        ((GracefullShutdownExecutor) consumerCombinerThreadPool).graceFullShutdown();
		((GracefullShutdownExecutor) consumerThreadPool).graceFullShutdown();
    }
}

*/
}