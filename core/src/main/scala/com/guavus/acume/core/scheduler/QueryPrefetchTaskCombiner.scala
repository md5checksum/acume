package com.guavus.acume.core.scheduler

import java.util.Calendar
import java.util.concurrent.Future
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.core.Interval
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.PSUserService
import QueryPrefetchTaskCombiner._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.Collections
import scala.util.control.Breaks._
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.cache.core.Level

object QueryPrefetchTaskCombiner {

  val logger = LoggerFactory.getLogger(classOf[QueryPrefetchTaskCombiner])
}

class QueryPrefetchTaskCombiner(private var isOlderTasks: Boolean, manager: QueryRequestPrefetchTaskManager, @BeanProperty var version: Int, acumeContext : AcumeContextTrait, acumeService : AcumeService, controller : Controller) extends Runnable with Comparable[QueryPrefetchTaskCombiner] {

  @BeanProperty
  var startTime: Long = _

  @BeanProperty
  var endTime: Long = _

  @BeanProperty
  var lastBinTime: Long = _

  @BeanProperty
  var binSource: String = _

  @BeanProperty
  var binClass: String = _

  private var set: java.util.SortedSet[QueryPrefetchTask] = new java.util.TreeSet[QueryPrefetchTask]()

  private var taskManager: QueryRequestPrefetchTaskManager = manager

  @BeanProperty
  var granToIntervalMap: scala.collection.mutable.HashMap[Long, Long] = _

  def getIsOlderTask(): Boolean = isOlderTasks

  def getQueryPrefetchTasks(): java.util.SortedSet[QueryPrefetchTask] = set

  override def run() {
    logger.info("Consuming combiner Task :" + this)
    executeQueryPrefetchTasks()
      this.synchronized {
        this.notify()
      }
    val policy = manager.acumeCacheAvailabilityMapPolicy
    val binSourceToIntervalMap = manager.getBinSourceToCacheAvalabilityMap 
    binSourceToIntervalMap.getOrElseUpdate(getBinSource, new scala.collection.mutable.HashMap[Long, Interval]())
    val map = new java.util.TreeMap[Long, Interval](Collections.reverseOrder[Long]())
    map.putAll(binSourceToIntervalMap.get(getBinSource).get)
    val levelPointMap = Utility.getLevelPointMap(acumeContext.acumeConf.getSchedulerVariableRetentionMap)
    val instance = Utility.newCalendar()
    for ((key, value) <- getGranToIntervalMap) {
      breakable {
        var interval = map.get(key)
        val ceil = Utility.ceilingFromGranularity(getStartTime, key)
        val floor = Utility.floorFromGranularity(getEndTime, key)
        val fromLevel = Math.max(Utility.getStartTimeFromLevel(Math.max(Math.max(value, if ((interval == null)) 0 else interval.getEndTime), floor), key, levelPointMap.get(new Level(key)).get), controller.getFirstBinPersistedTime(binSource))
        var startTime = Utility.ceilingFromGranularity(fromLevel, key)
        val flag = if (getIsOlderTask) {
          if (ceil >= value) {
            true 
          } else if (ceil > startTime) {
            startTime = ceil
            false
          } else false
        } else if (!getIsOlderTask) {
          if (startTime >= value) {
            true
          } else false
        } else
          false
        if (!flag) {
          if (interval == null) {
            if (startTime >= value) {
              break;
            }
            if (!getIsOlderTask) {
              if (ceil >= value) { 
                break;
              }
              interval = new Interval(ceil, value)
            } else {
              interval = new Interval(startTime, value)
            }
            map.put(key, interval)
          } else {
            if (isOlderTasks) {
              map.put(key, new Interval(startTime, interval.getEndTime))
            } else {
              if (interval.getStartTime > startTime) {
                startTime = interval.getStartTime
              }
              map.put(key, new Interval(startTime, value))
            }
          }
        }
      }
    }
    logger.info("Finished Combiner Task fetch: {}", this)
    logger.info("BinReplay: scheduler availability after combiner task for binsource " + binSource + " : {}", map)
//    var mergeMoveResult = true
//    if (RubixProperties.BinReplay.getBooleanValue) {
//      mergeMoveResult = executeMergeAndMove(binClass, binSource, isOlderTasks, map)
//      if (mergeMoveResult) {
//        logger.info("Starting filler for combiner task {}", this)
//        executeFiller()
//      }
//    }
      if (version != taskManager.getVersion) {
        logger.info("Not populating RubixDataAvailability in Distributed cache {} because view has changed" + binSourceToIntervalMap)
      } else {
        binSourceToIntervalMap.+=(getBinSource ->  (scala.collection.mutable.HashMap() ++= map.toMap))
        logger.info("Full Mode RubixDataAvailability: {} ", policy.getTrueCacheAvailabilityMap(this.version))
        logger.info("Combiner Version:" + this.version)
        logger.info("Partial Mode RubixDataAvailability {}", policy.getCacheAvalabilityMap)
      }
    
    if(!manager.oldCombinerRunning)
      manager.acumeCacheAvailabilityMapPolicy.onBackwardCombinerCompleted(this.version)
      
    this.synchronized {
      this.notify()
    }
    if (!isOlderTasks) try {
      Thread.sleep(1000)
    } catch {
      case e: InterruptedException => logger.error("", e)
    }
  }

  private def executeQueryPrefetchTasks() {
    val list = new ArrayBuffer[Future[_]]()
    for (task <- set) {
      list.add(taskManager.submitInnerTask(task))
    }
    for (future <- list) {
      try {
        future.get
      } catch {
        case e: InterruptedException => 
        case e: ExecutionException => 
        case e: CancellationException => 
      }
    }
  }

  override def compareTo(o: QueryPrefetchTaskCombiner): Int = {
    if (this.lastBinTime - this.endTime < o.lastBinTime - o.endTime) {
      -1
    } else if (this.lastBinTime - this.endTime > o.lastBinTime - o.endTime) {
      1
    } else if (this.lastBinTime - this.startTime < o.lastBinTime - o.startTime) {
      -1
    } else if (this.lastBinTime - this.startTime > o.lastBinTime - o.startTime) {
      1
    } else {
      -1
    }
  }

  override def toString(): String = {
    if (logger.isDebugEnabled) {
      "QueryPrefetchTaskCombiner [startTime=" + startTime + ", endTime=" + endTime + ", lastBinTime=" + lastBinTime + ", binSource=" + binSource + ", binClass=" + binClass + ", isOlderTasks=" + isOlderTasks + ", set=" + set + ", version=" + version + ", granToIntervalMap=" + granToIntervalMap + "]"
    } else {
      "QueryPrefetchTaskCombiner [startTime=" + startTime + ", endTime=" + endTime + ", lastBinTime=" + lastBinTime + ", binSource=" + binSource + ", binClass=" + binClass + ", isOlderTasks=" + isOlderTasks + ", task Count=" + set.size + ", version=" + version + ", granToIntervalMap=" + granToIntervalMap + "]"
    }
  }

/*
Original Java:
package com.guavus.rubix.scheduler;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guavus.rubix.cache.Interval;
import com.guavus.rubix.cache.PersistBinTimeStamp;
import com.guavus.rubix.configuration.RubixProperties;
import com.guavus.rubix.core.Controller;
import com.guavus.rubix.core.distribution.RubixDistribution;
import com.guavus.rubix.query.QueryRequestMode;
import com.guavus.rubix.query.remote.flex.RubixService;
import com.guavus.rubix.usermanagement.PSUserService;
import com.guavus.rubix.util.DistributedTaskType;
import com.guavus.rubix.util.GenericExecutorTask;
import com.guavus.rubix.util.Utility;

public class QueryPrefetchTaskCombiner implements Runnable , Comparable<QueryPrefetchTaskCombiner>{
	
	
	public static final Logger logger = LoggerFactory
			.getLogger(QueryPrefetchTaskCombiner.class);
	private long startTime,endTime;
	private long lastBinTime;
	private String binSource;
	private String binClass;
	private boolean isOlderTasks = false;
	private SortedSet<QueryPrefetchTask> set;
	private QueryRequestPrefetchTaskManager taskManager;
	private int version;
	
	
	private Map<Long,Long> granToIntervalMap;
	
	public Map<Long, Long> getGranToIntervalMap() {
		return granToIntervalMap;
	}

	public void setGranToIntervalMap(Map<Long, Long> granToIntervalMap) {
		this.granToIntervalMap = granToIntervalMap;
	}

	public long getLastBinTime() {
		return lastBinTime;
	}

	public void setLastBinTime(long lastBinTime) {
		this.lastBinTime = lastBinTime;
	}

	public String getBinSource() {
		return binSource;
	}

	public void setBinSource(String binSource) {
		this.binSource = binSource;
	}

	public String getBinClass() {
		return binClass;
	}

	public void setBinClass(String binClass) {
		this.binClass = binClass;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public QueryPrefetchTaskCombiner(boolean isOlderTasks, QueryRequestPrefetchTaskManager manager, int version) {
		set = new TreeSet<QueryPrefetchTask>();
		this.isOlderTasks = isOlderTasks;
		this.taskManager = manager;
		this.version = version;
	}
	
	public int getVersion() {
		return version;
	}
	
	public boolean getIsOlderTask() {
		return isOlderTasks;
	}
	
	public SortedSet<QueryPrefetchTask> getQueryPrefetchTasks() {
		return set;
	}

	@Override
	public void run() {
		
		logger.info("Consuming combiner Task :" + this);
		executeQueryPrefetchTasks();
		
		if(!RubixDistribution.getInstance().isCoordinator() || version != taskManager.getVersion()) {
			synchronized (this) {
				this.notify();
			}
			return;
		}
		Map<String, Map<String, Map<Long, Interval>>> rubixBinClassToBinSourceMap = QueryRequestPrefetchTaskManager.getInstance().getBinclassToBinSourceToRubixAvailability();
		Map<String, Map<Long, Interval>> binSourceToIntervalMap = rubixBinClassToBinSourceMap.get(getBinClass());
		
		if(binSourceToIntervalMap == null) {
			binSourceToIntervalMap = new HashMap<String, Map<Long,Interval>>();
			rubixBinClassToBinSourceMap.put(getBinClass(), binSourceToIntervalMap);
		}
		
		if(binSourceToIntervalMap.get(getBinSource()) == null) {
			binSourceToIntervalMap.put(getBinSource(), new HashMap<Long, Interval>());
		}
		
		Map<Long, Interval> map = new TreeMap<Long,Interval>(Collections.reverseOrder());
		map.putAll(binSourceToIntervalMap.get(getBinSource()));
		SortedMap<Long,Integer> levelPointMap = Utility.getLevelPointMap(RubixProperties.SchedulerVariableGranularityMap.getValue());
		Calendar instance = Utility.newCalendar();
		for (Map.Entry<Long, Long> entry : getGranToIntervalMap().entrySet()) {
			Interval interval = map.get(entry.getKey());
			long ceil = Utility.ceilingFromGranularity(getStartTime(), entry.getKey());
			long floor = Utility.floorFromGranularity(getEndTime(), entry.getKey());
			//Calculate max start time till which availability can reach
			long fromLevel = Math.max(Utility.getStartTimeFromLevel(Math.max(Math.max(entry.getValue(),(interval == null) ? 0:interval.getEndTime()),floor), entry.getKey(), levelPointMap.get(entry.getKey())),Controller.getInstance().getFirstBinPersistedTime(binClass, binSource));
			long startTime = Utility.ceilingFromGranularity(fromLevel, entry.getKey());
			if(getIsOlderTask()) {
				if(ceil >= entry.getValue())
					continue;
				if(ceil > startTime)
					startTime = ceil;
			} else if(!getIsOlderTask()) {
				if(startTime >= entry.getValue()) {
					continue;
				}
			}
			
			if(interval == null) {
				if(startTime >= entry.getValue()) {
					continue;
				}
				if(!getIsOlderTask()) {
					if(ceil >= entry.getValue())
						continue;
					interval = new Interval(ceil, entry.getValue());
				} else {
					interval = new Interval(startTime, entry.getValue());
				}
				map.put(entry.getKey(), interval);
			} else {
				if(isOlderTasks) {
					map.put(entry.getKey(), new Interval(startTime, interval.getEndTime()));
				} else {
					if(interval.getStartTime() > startTime) {
						startTime = interval.getStartTime();
					}
					map.put(entry.getKey(), new Interval(startTime, entry.getValue()));
				}
			}
		}
		
		
		logger.info("Finished Combiner Task fetch: {}" , this);
		
		logger.info("BinReplay: scheduler availability after combiner task {}",map);
		boolean mergeMoveResult=true;
		if(RubixProperties.BinReplay.getBooleanValue()){
			//Invoke merge task binclass,binsource,gran, rubix data availabilty interval
			mergeMoveResult = executeMergeAndMove(binClass,binSource,isOlderTasks,map);
			//Invoke filler only if merge and move are successful
			if(mergeMoveResult) {
				logger.info("Starting filler for combiner task {}",this);
				executeFiller();
			}
		}
		if(!mergeMoveResult){
			logger.warn("Not able to move or merge for combiner task {}" ,this);
		}else{
			//update availability
			|*
			 * This is not rubixDataAvailabilty this is Interval for which schduler task has been executed. 
			 * This does not transfer to rubix data availabilty directly in case of bin replay 
			 * as some bins might be missing while schduler executed the tasks on export time.
			 *|
			// this might conflict with clear cache method.
			if(version != taskManager.getVersion()) {
				logger.info("Not populating RubixDataAvailability in Distributed cache {} because view has changed" + rubixBinClassToBinSourceMap);
			} else {
				binSourceToIntervalMap.put(getBinSource(), map);
				logger.info("Putting RubixDataAvailability in Distributed cache {} " , rubixBinClassToBinSourceMap);
				QueryRequestPrefetchTaskManager.getInstance().updateBinClassToBinSourceToRubixAvailabiltyMap(rubixBinClassToBinSourceMap);
				logger.info("BinReplay: UI RubixDataAvaiabilty {}",new PSUserService().getRubixTimeIntervalForBinClass(binClass));
			}
		}
		synchronized (this) {
			this.notify();
		}

		if(!isOlderTasks)
			//Just to ensure that Future tasks run continously.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("",e);
			}
	}

	private void updateRRCache(Set<Object> mergeMoveResult) {
		if(mergeMoveResult == null) {
			logger.error("Timestamps after merge and move are null");
			return;
		}
		RubixService rubixService = RubixService.getInstance();
		rubixService.removeTimestampsFromRRCache(mergeMoveResult);
	}

	private boolean executeMergeAndMove(String binClass2, String binSource2,
			boolean isOlderTasks2, Map<Long, Interval> map) {
		MergerTask mergerTask = new MergerTask(binClass2, binSource2,
				isOlderTasks2, map);
		if(RubixDistribution.getInstance().isSomeRubixOperationInProgress() || version != taskManager.getVersion()){
			logger.warn("Cluster operation is in progress. not executing merger task");
			return false;
		}
		List<Future<Boolean>> results = RubixDistribution.getInstance().submitEverywhere(mergerTask);
		boolean finalResult = true;
		long startTime = System.currentTimeMillis();
		try {
			for (Future<Boolean> result : results) {
				Boolean nodeResult = false;
				nodeResult = (Boolean) result.get();
				if (!nodeResult) {
					finalResult = false;
				}
			}
			logger.info("Merger Task completed on all the nodes. Time taken to merge keys {} ",System.currentTimeMillis()-startTime);
		} catch (ExecutionException e) {
			return false;
			// Utility.throwIfRubixException(e);
			// throw new RuntimeException(e);
		} catch (InterruptedException e) {
			return false;
			// Utility.throwIfRubixException(e);
			// throw new RuntimeException(e);
		}

		Set<Object> timestamps = new HashSet<Object>();
		if (finalResult) {
			if(RubixDistribution.getInstance().isSomeRubixOperationInProgress() || version != taskManager.getVersion()){
				logger.warn("Cluster operation is in progress. not executing mover task");
				return false;
			}
			// merge task have been completed on all the nodes, now execute move task
			// Acquire cluster wide lock for move task and RRCache update task
			boolean isLockAcquired = acquireClusterWideMoveLock();
			if(isLockAcquired) {
				MoveTask moveTask = new MoveTask(binClass2, binSource2);
				startTime = System.currentTimeMillis();
				List<Future<Set<Object>>> moveResults = RubixDistribution.getInstance().submitEverywhere(moveTask);
				try {
					for (Future<Set<Object>> result : moveResults) {
						Set<Object> nodeResult = (Set<Object>) result.get();
						if ((nodeResult != null) && (timestamps != null)) {
							timestamps.addAll(nodeResult);
						} else if(nodeResult == null) {
							timestamps = null;
							releaseClusterWideMoveLock();
							return false;
						}
					}
					long endTime = System.currentTimeMillis();
					logger.info("Move Task completed on all the nodes, Time taken to move keys {}",endTime-startTime);
					updateRRCache(timestamps);
					logger.info("RR cache updated on all the nodes, Time taken {}",System.currentTimeMillis() - endTime);
					releaseClusterWideMoveLock();
				} catch (ExecutionException e) {
					return false;
				} catch (InterruptedException e) {
					return false;
				}
			} else {
				return false;
			}
		}

		return (timestamps!=null);
	}
	
	private boolean acquireClusterWideMoveLock() {

		if(RubixDistribution.getInstance().isSomeRubixOperationInProgress()) {
			return false;
		}
		GenericExecutorTask task = new GenericExecutorTask(DistributedTaskType.GET_CLUSTER_MOVE_LOCK);
		List<Future<Object>> results = RubixDistribution.getInstance().submitEverywhere(task);
		for (Future<Object> result : results) {
			Boolean lockResult = false;
    		try{
				lockResult = (Boolean)result.get();
			} catch(Exception e) {
	    		logger.warn("Exception recieved while getting cluster wide move lock", e);
	   		}
 			if(!lockResult) {
 				releaseClusterWideMoveLock();
  				return false;
  			}
	   	}
 		logger.info("Cluster Wide Move lock acquired");

		return true;
	}
	
	private boolean releaseClusterWideMoveLock() {
		GenericExecutorTask task = new GenericExecutorTask(DistributedTaskType.RELEASE_CLUSTER_MOVE_LOCK);
    	List<Future<Object>> results = RubixDistribution.getInstance().submitEverywhere(task);
    	Boolean finalResult = true;
		try {
			for (Future<Object> result : results) {
				Boolean lockResult = (Boolean)result.get();
				if(!lockResult) {
					finalResult = false;
				}
			}
			logger.info("Cluster Wide Move lock released with status {}", finalResult);
			return finalResult;
		}catch (ExecutionException e) {
			Utility.throwIfRubixException(e);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			Utility.throwIfRubixException(e);
			throw new RuntimeException(e);
		}
	}
	
	private void executeQueryPrefetchTasks() {
		List<Future> list = new ArrayList<Future>();
		for (QueryPrefetchTask task : set) {
			list.add(taskManager.submitInnerTask(task));
		}
		
		for (Future future: list) {
			try {
				future.get();
			} catch (InterruptedException e) {
			} catch (ExecutionException e) {
			}catch( CancellationException e){
			}
		}
	}
	
	private void executeFiller() {
		
		for(QueryPrefetchTask queryPrefetchTask : set) {
			queryPrefetchTask.getRequest().getQueryRequest().setQueryRequestModeRecursively(QueryRequestMode.FILLER);
		}
		if(version != taskManager.getVersion()){
			logger.info("No need to executer filler as version have changed");
		}
		executeQueryPrefetchTasks();
		logger.info("Filler Task completed on all the nodes ");
	}

	@Override
	public int compareTo(QueryPrefetchTaskCombiner o) {
		if(this.lastBinTime - this.endTime< o.lastBinTime - o.endTime ) {
			return -1;
		} else if(this.lastBinTime - this.endTime > o.lastBinTime - o.endTime) {
			return 1;
		} else if(this.lastBinTime - this.startTime < o.lastBinTime - o.startTime) {
			return -1;
		} else if(this.lastBinTime - this.startTime>o.lastBinTime - o.startTime) {
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public String toString() {
		if(logger.isDebugEnabled()) {
			 return "QueryPrefetchTaskCombiner [startTime=" + startTime
				+ ", endTime=" + endTime + ", lastBinTime=" + lastBinTime
				+ ", binSource=" + binSource + ", binClass=" + binClass
				+ ", isOlderTasks=" + isOlderTasks + ", set=" + set
				+ ", version=" + version + ", granToIntervalMap="
				+ granToIntervalMap + "]";
		} else {
			return "QueryPrefetchTaskCombiner [startTime=" + startTime
			+ ", endTime=" + endTime + ", lastBinTime=" + lastBinTime
			+ ", binSource=" + binSource + ", binClass=" + binClass
			+ ", isOlderTasks=" + isOlderTasks + ", task Count=" + set.size()
			+ ", version=" + version + ", granToIntervalMap="
			+ granToIntervalMap + "]";
		}
	}
	
}

*/
}