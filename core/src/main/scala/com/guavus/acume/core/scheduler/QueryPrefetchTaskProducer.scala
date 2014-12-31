package com.guavus.acume.core.scheduler

import java.util.Calendar
import java.util.concurrent.atomic.AtomicInteger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.guavus.acume.workflow.RequestDataType
import QueryPrefetchTaskProducer._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import scala.collection.JavaConversions._
import com.guavus.acume.core.AcumeConf
import com.guavus.querybuilder.cube.schema.ICube
import com.guavus.acume.core.DataService
import com.guavus.querybuilder.cube.schema.QueryBuilderSchema
import com.guavus.rubix.query.remote.flex.QueryRequest
import com.guavus.acume.cache.core.Interval
import scala.collection.immutable.IntMap.Bin
import com.guavus.acume.cache.core.EvictionDetails
import com.guavus.rubix.query.remote.flex.SortDirection
import com.guavus.acume.cache.utility.Utility
import com.guavus.rubix.query.remote.flex.SortDirection
import com.guavus.rubix.query.remote.flex.SortDirection
import com.guavus.acume.core.PSUserService
import com.guavus.acume.cache.core.TimeGranularity
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import com.guavus.querybuilder.cube.schema.FieldType
import com.guavus.rubix.query.remote.flex.NameValue
import com.guavus.acume.core.AcumeService

object QueryPrefetchTaskProducer {

  val BIN_SOURCE = "binSource"

  val LAST_BIN_TIME = "lastBinTime"

  val VERSION = "version"

  val logger = LoggerFactory.getLogger(classOf[QueryPrefetchTaskProducer])

}

class QueryPrefetchTaskProducer(acumeConf : AcumeConf, schema : QueryBuilderSchema, private var taskManager: QueryRequestPrefetchTaskManager, private var dataService: DataService, acumeService : AcumeService, saveRequests : Boolean, policy : ISchedulerPolicy) extends Runnable {

  private var lastCacheUpdateTimeMap: scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[PrefetchCubeConfiguration, Long]]] = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[PrefetchCubeConfiguration, Long]]]()

  private var cubeLocator: PrefetchCubeLocator = new PrefetchCubeLocator(schema)

  @BeanProperty
  var requestLists: ArrayBuffer[PrefetchTaskRequest] = new ArrayBuffer[PrefetchTaskRequest]()

  var version: AtomicInteger = new AtomicInteger(0)

  private def generateTopRequest(topCube: ICube, startTime: Long, endTime: Long, binSourceName: String, level: java.lang.Long): QueryRequest = {
    val topQuery = new QueryRequest()
    topQuery.setStartTime(startTime)
    topQuery.setEndTime(endTime)
    topQuery.setResponseDimensions(new java.util.ArrayList(topCube.getFields.filter(_.getType().equals(FieldType.DIMENSION)).map(_.getName())))
    val outputMeasures = topCube.getFields().filter(_.getType().equals(FieldType.MEASURE)).map(_.getName())
    topQuery.setResponseMeasures(new java.util.ArrayList(outputMeasures))
    val binSource = binSourceName
    topQuery.setBinSource(if (binSource != null) binSource else "")
    topQuery.setTimeGranularity(topCube.getTimeGranularityValue)
    topQuery.setOffset(0)
    topQuery.setLength(-1)
    val paramerterMap = new scala.collection.mutable.ArrayBuffer[NameValue]()
    paramerterMap += new NameValue("RUBIX_CACHE_COMPRESSION_INTERVAL", String.valueOf(level))
    topQuery.setParamMap(new java.util.ArrayList(paramerterMap))
    topQuery
  }
//
//  private def iDimensionToIGenericDimensionFilters(list: List[Map[IDimension, Integer]]): Collection[Map[IGenericDimension, Any]] = {
//    val filters = Lists.newArrayList()
//    if (list != null) {
//      for (map <- list) {
//        val filter = Maps.newHashMap()
//        for ((key, value) <- map) {
//          val genericDimension = genericDimensionReverseMap.get(key)
//          if (genericDimension == null) {
//            throw new IllegalArgumentException("Generic Dimension not found for :" + key)
//          }
//          filter.put(genericDimension, value)
//        }
//        filters.add(filter)
//      }
//    }
//    filters
//  }
//
//  private def generateProfileRequest(subQuery: QueryRequest, profileCube: ICube, startTime: Long, endTime: Long, topCube: ICube, level: java.lang.Long): QueryRequest = {
//    val superQuery = new QueryRequest()
//    superQuery.setStartTime(startTime)
//    superQuery.setEndTime(endTime)
//    superQuery.setLength(-1)
//    superQuery.setTimeGranularity(profileCube.getTimeGranularity)
//    val superParamerterMap = new HashMap[IKey, Any]()
//    superParamerterMap.put(ReqParamKey.RUBIX_CACHE_COMPRESSION_INTERVAL, level)
//    superQuery.setParamerterMap(superParamerterMap)
//    superQuery.setResponseDimensions(getGenericDimensionIDs(profileCube.getDistributionKeys))
//    superQuery.setCubeContextDimensions(getGenericDimensionIDs(profileCube.getOutputDimensions))
//    var sortDirection: SortDirection = null
//    var sortProperty: ICubeProperty = null
//    val binSource = subQuery.getBinSource
//    superQuery.setBinSource(if (binSource != null) binSource else BinSource.getDefault)
//    superQuery.setResponseMeasures(profileCube.getOutputMeasures)
//    superQuery.setFilterRequest(FilterRequest.newEmptyFilterRequest())
//    subQuery = new QueryRequest(subQuery)
//    subQuery.setResponseDimensions(getGenericDimensionIDs(Utility.getNonStaticKeys(profileCube)))
//    subQuery.setLength(MAX_SINGLE_ENTITY_SIZE)
//    val topCubePrefetchConfiguration = topCube.getPrefetchConfiguration
//    if (topCubePrefetchConfiguration != null) {
//      sortProperty = topCubePrefetchConfiguration.getSortProperty
//      sortDirection = topCubePrefetchConfiguration.getSortDirection
//    }
//    val outputMeasures = topCube.getOutputMeasures
//    sortDirection = if (sortDirection != null) sortDirection else SortDirection.DSC
//    subQuery.setSortDirection(sortDirection)
//    sortProperty = if (sortProperty != null) sortProperty else outputMeasures.iterator().next()
//    sortProperty = if (sortProperty.isInstanceOf[IDimension]) getGenericDimensionID(sortProperty.asInstanceOf[IDimension]) else sortProperty
//    subQuery.setSortProperty(sortProperty)
//    if (!(subQuery.getResponseMeasures.contains(subQuery.getSortProperty) || subQuery.getResponseDimensions.contains(subQuery.getSortProperty))) {
//      subQuery.setSortProperty(subQuery.getResponseMeasures.iterator().next())
//    }
//    val subQueryParamerterMap = new HashMap[IKey, Any]()
//    subQueryParamerterMap.put(ReqParamKey.RUBIX_CACHE_COMPRESSION_INTERVAL, level)
//    subQuery.setParamerterMap(subQueryParamerterMap)
//    superQuery.setSubQuery(subQuery)
//    superQuery.setQueryRequestMode(QueryRequestMode.SCHEDULER)
//    subQuery.setQueryRequestMode(QueryRequestMode.SCHEDULER)
//    superQuery
//  }
//
//  private def getGenericDimensionIDs(outputDimensions: LinkedHashSet[IDimension]): Set[IGenericDimension] = {
//    val genericDimensions = new LinkedHashSet[IGenericDimension]()
//    for (dimension <- outputDimensions) {
//      genericDimensions.add(getGenericDimensionID(dimension))
//    }
//    genericDimensions
//  }
//
//  private def getGenericDimensionID(dimension: IDimension): IGenericDimension = {
//    val genericDimension = genericDimensionReverseMap.get(dimension)
//    if (genericDimension == null) {
//      throw new IllegalArgumentException("Generic Dimension not found for :" + dimension)
//    }
//    genericDimension
//  }

//  private def hasLocalCube(prefetchCubeConfiguration: PrefetchCubeConfiguration): Boolean = {
//    if (Utility.isNullOrEmpty(prefetchCubeConfiguration.getTopCube.getDistributionKeys)) return true
//    for (profileCube <- prefetchCubeConfiguration.getProfileCubes if Utility.isNullOrEmpty(profileCube.getDistributionKeys)) return true
//    false
//  }

  override def run() {
    try {
      var isFirstRun = false
      val version = this.version.get
      val tempLastCacheUpdateTimeMap = lastCacheUpdateTimeMap
      if (version != this.version.get) {
        logger.info("skipping task creation as view has changed version changed  from {} to {}", version, this.version.get)
        return
      }
      if (lastCacheUpdateTimeMap.size == 0) {
        isFirstRun = true
      }
      val combinerSet = new java.util.TreeSet[QueryPrefetchTaskCombiner]()
      var binSourcesToIntervalsMap = Controller.getInstaTimeInterval
      val binSourceToCubeConfigurations = cubeLocator.getPrefetchCubeConfigurations
      if (logger.isDebugEnabled) {
        logger.debug("prefetch cube configuration is==>" + binSourceToCubeConfigurations)
      }
      for ((key, value) <- binSourceToCubeConfigurations) {
        var binSourceToCacheTime = tempLastCacheUpdateTimeMap.get(key).getOrElse({null})
        if (binSourceToCacheTime == null) {
          binSourceToCacheTime = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[PrefetchCubeConfiguration, Long]]()
          tempLastCacheUpdateTimeMap.put(key, binSourceToCacheTime)
        }
        if (!isFirstRun) {
          binSourcesToIntervalsMap = Controller.getInstaTimeInterval
        }
        val intervalMap = binSourcesToIntervalsMap.get(key).getOrElse({throw new IllegalStateException("StartTime for binsource " + key + " can not be null")})
          var cubeConfigurationToCacheTime = binSourceToCacheTime.get(key).getOrElse({null})
          if (cubeConfigurationToCacheTime == null) {
            cubeConfigurationToCacheTime = new scala.collection.mutable.HashMap[PrefetchCubeConfiguration, Long]()
            binSourceToCacheTime.put(key, cubeConfigurationToCacheTime)
          }
          var startTime = intervalMap.get(-1).getOrElse({throw new IllegalStateException("StartTime for binsource " + key + " can not be null")}).getStartTime
          startTime = policy.getCeilOfTime(startTime)
          val endTime = intervalMap.get(-1).getOrElse({throw new IllegalStateException("EndTime for binsource " + key + " can not be null")})getEndTime//if (prefetcherLatestTime != -1) prefetcherLatestTime else binSourceToIntervalMap.get(Controller.DEFAULT_AGGR_INTERVAL).getEndTime
          val map = new java.util.TreeMap[Long, QueryPrefetchTaskCombiner]()
          var tempEndTime = getNextEndTime(startTime, endTime)
          while (startTime < endTime) {
            val combiner = new QueryPrefetchTaskCombiner(isFirstRun, taskManager, version, acumeConf, acumeService)
            for (prefetchCubeConfiguration <- value) {
              val lastCacheUpdatedTime = cubeConfigurationToCacheTime.get(prefetchCubeConfiguration).getOrElse({null}).asInstanceOf[Long]
              if (lastCacheUpdatedTime != null && lastCacheUpdatedTime != 0 && tempEndTime < lastCacheUpdatedTime) {
                tempEndTime = lastCacheUpdatedTime
                //continue
              } else {
              if (binSourceToCacheTime == null) {
                binSourceToCacheTime = scala.collection.mutable.HashMap()
              }
              val optionalParam = new scala.collection.mutable.HashMap[String, Any]()
              optionalParam.put(BIN_SOURCE, key)
              optionalParam.put(LAST_BIN_TIME, value)
              optionalParam.put(VERSION, version)
              val lastCacheUpdateAndPrefetchIntervals = policy.getIntervalsAndLastUpdateTime(startTime, tempEndTime, prefetchCubeConfiguration, isFirstRun, optionalParam)
              val prefetchIntervals = lastCacheUpdateAndPrefetchIntervals.getIntervals
              map.put(startTime, combiner)
              for (eachInterval <- prefetchIntervals) {
                val cubeGranularity = prefetchCubeConfiguration.getTopCube.getTimeGranularityValue()
                if (eachInterval.getGranularity >= cubeGranularity && eachInterval.getStartTime == Utility.floorFromGranularity(eachInterval.getStartTime, cubeGranularity) && eachInterval.getEndTime == Utility.floorFromGranularity(eachInterval.getEndTime, cubeGranularity)) {
                  val taskRequests = createPrefetchTaskRequests(prefetchCubeConfiguration, eachInterval.getStartTime, eachInterval.getEndTime, key, key, eachInterval.getGranularity, null)
                  for (taskRequest <- taskRequests) {
                    logger.debug("Queueing Task for :" + taskRequest)
                    if (!saveRequests) {
                      if (isFirstRun) {
                        if (taskRequest.getStartTime >= startTime) {
                          val set = map.get(startTime)
                          if (set != null) {
                            set.getQueryPrefetchTasks.add(new QueryPrefetchTask(dataService, taskRequest, version, taskManager, acumeConf))
                          } else {
                            combiner.getQueryPrefetchTasks.add(new QueryPrefetchTask(dataService, taskRequest, version, taskManager, acumeConf))
                            map.put(startTime, combiner)
                          }
                        } else {
                          val headMap = map.headMap(taskRequest.getStartTime + TimeGranularity.ONE_MINUTE.getGranularity)
                          val otherCombiner = headMap.get(headMap.lastKey())
                          if (otherCombiner.getGranToIntervalMap.get(eachInterval.getGranularity).get < eachInterval.getEndTime) {
                            otherCombiner.getGranToIntervalMap.put(eachInterval.getGranularity, eachInterval.getEndTime)
                          }
                          otherCombiner.getQueryPrefetchTasks.add(new QueryPrefetchTask(dataService, taskRequest, version, taskManager, acumeConf))
                        }
                      } else {
                        combiner.getQueryPrefetchTasks.add(new QueryPrefetchTask(dataService, taskRequest, version, taskManager, acumeConf))
                      }
                    } else {
                      requestLists.add(taskRequest)
                    }
                  }
                }
              }
              combiner.setStartTime(startTime)
              combiner.setEndTime(tempEndTime)
              combiner.setBinSource(key)
              combiner.setGranToIntervalMap(lastCacheUpdateAndPrefetchIntervals.getCacheEndTimeMap)
              combiner.setLastBinTime(if (isFirstRun) endTime else tempEndTime)
              if (lastCacheUpdateAndPrefetchIntervals.getCacheLastUpdateTime != 0) {
                cubeConfigurationToCacheTime.put(prefetchCubeConfiguration, lastCacheUpdateAndPrefetchIntervals.getCacheLastUpdateTime)
              }
              if (version != this.version.get) {
                return
              }
            }
            }
            if (isFirstRun && !saveRequests) {
              combinerSet.add(combiner)
            } else if (combiner.getQueryPrefetchTasks.size != 0 && !saveRequests) {
              combiner.synchronized {
                taskManager.submitTask(combiner)
                combiner.wait()
              }
            }
            startTime = tempEndTime
            tempEndTime = getNextEndTime(startTime, endTime)
          }
      }
      var iterator = combinerSet.iterator()
      while (iterator.hasNext) {
        val queryPrefetchTaskCombiner = iterator.next().asInstanceOf[QueryPrefetchTaskCombiner]
        if (queryPrefetchTaskCombiner.getQueryPrefetchTasks.size != 0) taskManager.submitTask(queryPrefetchTaskCombiner)
      }
    } catch {
      case t: Throwable => logger.error("Error while producing scheduling task.", t)
    }
  }

  private def getNextEndTime(startTime: Long, endTime: Long): Long = {
    val instance = Utility.newCalendar()
    val tempEndTime = Utility.getNextTimeFromGranularity(startTime, acumeConf.getSchedulerMaxSegmentDurationCombinePoints, instance)
    if (tempEndTime > endTime) {
      return endTime
    }
    tempEndTime
  }

//  private def isTimeRangeValid(binClass: String, binSource: String, level: Long, cube: ICube, aggrGranToLastBinInterval: Map[Long, Interval], endTime: Long, startTime: Long): Boolean = {
//    if (Utility.isTreeCache(cube)) {
//      val forcePopulateFromAggrInterval = if (cube.getPrefetchConfiguration != null) cube.getPrefetchConfiguration.isForcePopulationOn else false
//      if (forcePopulateFromAggrInterval) {
//        val binTimeGran = Bin.getBin(binClass).getTimeGranularity.getGranularity
//        var aggrInterval = level
//        if (binTimeGran == level) {
//          aggrInterval = Controller.DEFAULT_AGGR_INTERVAL
//        }
//        val lastBinInterval = aggrGranToLastBinInterval.get(aggrInterval)
//        if (lastBinInterval == null) {
//          return false
//        } else if (endTime > lastBinInterval.getEndTime || startTime < lastBinInterval.getStartTime) {
//          return false
//        }
//      }
//      val lastBinEndtime = aggrGranToLastBinInterval.get(Controller.DEFAULT_AGGR_INTERVAL).getEndTime
//      val evictionDetails = getEvictionDetails(cube)
//      var allRetentionPoints = evictionDetails.getAllRetentionPoints
//
//      if (!RubixCacheFactory.useFlash()) {
//        allRetentionPoints = evictionDetails.getMemoryAndDiskRetentionPoints
//      }
//      if (allRetentionPoints.containsKey(level)) {
//        val numPoints = allRetentionPoints.get(level)
//        val rangeStartTime = TreeRubixCacheVariableRetentionPolicy.getRangeStartTime(lastBinEndtime, level, numPoints)
//        if (endTime <= rangeStartTime) {
//          return false
//        }
//      } else {
//        return false
//      }
//    }
//    true
//  }
//
  private def createPrefetchTaskRequests(prefetchCubeConfiguration: PrefetchCubeConfiguration, startTime: Long, endTime: Long, binSource: String, binClass: String, level: java.lang.Long, aggrGranToLastBinInterval: Map[Long, Interval]): List[PrefetchTaskRequest] = {
    val taskRequests = new ArrayBuffer[PrefetchTaskRequest]()
    val topCube = prefetchCubeConfiguration.getTopCube
    val topRequest = generateTopRequest(topCube, startTime, endTime, binSource, level)
        taskRequests.add(makePrefetchTaskRequest(topCube, topRequest))
    taskRequests.toList
  }
//
//  private def getEvictionDetails(cube: ICube): EvictionDetails = {
//    var key = cube.getCacheIdentifier.getKeyOfFirstEntry
//    if (cube.getCacheType.isSingleEntity) {
//      key += SingleEntityRubixCache.SINGLE_ENTITY_CACHE_IDENTIFIER
//    }
//    CacheUtils.getEvictionDetails(key, true, cube.getTimeGranularity.getGranularity)
//  }
//
  private def makePrefetchTaskRequest(cube: ICube, request: QueryRequest): PrefetchTaskRequest = {
    val topPrefetchTaskRequest = new PrefetchTaskRequest()
    topPrefetchTaskRequest.setQueryRequest(request)
      topPrefetchTaskRequest.setRequestDataType(RequestDataType.TimeSeries)
    topPrefetchTaskRequest
  }
//
  def clearTaskCacheUpdateTimeMap() {
    lastCacheUpdateTimeMap = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[PrefetchCubeConfiguration, Long]]]()
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.scheduler;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.guavus.rubix.cache.CacheType;
import com.guavus.rubix.cache.EvictionDetails;
import com.guavus.rubix.cache.Interval;
import com.guavus.rubix.cache.RubixCacheFactory;
import com.guavus.rubix.cache.SingleEntityRubixCache;
import com.guavus.rubix.cache.TimeGranularity;
import com.guavus.rubix.cache.policy.TreeRubixCacheVariableRetentionPolicy;
import com.guavus.rubix.cache.util.CacheUtils;
import com.guavus.rubix.configuration.Bin;
import com.guavus.rubix.configuration.BinSource;
import com.guavus.rubix.configuration.ConfigFactory;
import com.guavus.rubix.configuration.IGenericConfig;
import com.guavus.rubix.configuration.RubixProperties;
import com.guavus.rubix.core.Controller;
import com.guavus.rubix.core.ICube;
import com.guavus.rubix.core.ICubeProperty;
import com.guavus.rubix.core.IDimension;
import com.guavus.rubix.core.IMeasure;
import com.guavus.rubix.core.PrefetchConfiguration;
import com.guavus.rubix.core.distribution.RubixDistribution;
import com.guavus.rubix.parameters.FilterObject;
import com.guavus.rubix.parameters.FilterRequest;
import com.guavus.rubix.query.DataService;
import com.guavus.rubix.query.IGenericDimension;
import com.guavus.rubix.query.QueryRequest;
import com.guavus.rubix.query.QueryRequestMode;
import com.guavus.rubix.query.SortDirection;
import com.guavus.rubix.util.Utility;
import com.guavus.rubix.workflow.IKey;
import com.guavus.rubix.workflow.ReqParamKey;
import com.guavus.rubix.workflow.RequestDataType;


|**
 * @author akhil.swain
 * 
 *|
public class QueryPrefetchTaskProducer implements Runnable {
    public static final String BIN_SOURCE = "binSource";
    public static final String LAST_BIN_TIME = "lastBinTime";
    public static final String VERSION = "version";
	public static final Logger logger = LoggerFactory.getLogger(QueryPrefetchTaskProducer.class);
    public static final long PREFETCH_TIME_PARTITION_GAP = RubixProperties.DataPrefetchSchedulerMaximumRequestInterval.getLongValue();
    public static final int MAX_SINGLE_ENTITY_SIZE = RubixProperties.DataPrefetchSchedulerSingleEntitiesCount.getIntValue();
    // static long lastCacheUpdatedTime;
    private Map<String,Map<String,Map<PrefetchCubeConfiguration, Long>>> lastCacheUpdateTimeMap = new HashMap<String, Map<String,Map<PrefetchCubeConfiguration, Long>>>();
    private QueryRequestPrefetchTaskManager taskManager;
    private static Map<IDimension, IGenericDimension> genericDimensionReverseMap = new HashMap<IDimension, IGenericDimension>();
    private PrefetchCubeLocator cubeLocator;
    private List<PrefetchTaskRequest> requestLists = new ArrayList<PrefetchTaskRequest>();
    
    public List<PrefetchTaskRequest> getRequestLists() {
    	return requestLists;
    }
    
   private boolean saveRequests = false;
    private DataService dataService;
    public AtomicInteger version = new AtomicInteger(0);
    
    public QueryPrefetchTaskProducer(IGenericConfig iGenericConfig,
        QueryRequestPrefetchTaskManager taskManager, DataService dataService) {
        this.taskManager = taskManager;
        this.cubeLocator = new PrefetchCubeLocator(taskManager.getCubeManager());
        genericDimensionReverseMap = iGenericConfig.getDimensionToGenericDimensionMap();
        this.dataService = dataService;
    }
    
    public QueryPrefetchTaskProducer(IGenericConfig iGenericConfig,
            QueryRequestPrefetchTaskManager taskManager, boolean saveRequests) {
            this.taskManager = taskManager;
            this.cubeLocator = new PrefetchCubeLocator(taskManager.getCubeManager());
            genericDimensionReverseMap = iGenericConfig.getDimensionToGenericDimensionMap();
            this.saveRequests = saveRequests;
        }
	
    private QueryRequest generateTopRequest(ICube topCube, long startTime,
        long endTime,String binSourceName, Long level) {
        QueryRequest topQuery = new QueryRequest();
        topQuery.setStartTime(startTime);
        topQuery.setEndTime(endTime);
        
        //only request for the dimensions which are in distribution key, so that the request is disjoint
        //and it brings the minimum possible tuples
        topQuery.setResponseDimensions(getGenericDimensionIDs(topCube.getDistributionKeys()));
        topQuery.setCubeContextDimensions(getGenericDimensionIDs(topCube.getOutputDimensions()));
        LinkedHashSet<IMeasure> outputMeasures = new LinkedHashSet<IMeasure>(topCube.getOutputMeasures());
        topQuery.setResponseMeasures(outputMeasures);
        PrefetchConfiguration prefetchConfiguration = topCube.getPrefetchConfiguration();
        SortDirection sortDirection = null;
        BinSource binSource = BinSource.valueOf(binSourceName);
        ICubeProperty sortProperty = null;
        List<Map<IDimension, Integer>> prefetchKeys = null;
        if (prefetchConfiguration != null) {
            prefetchKeys = prefetchConfiguration.getKeys();
            sortProperty = prefetchConfiguration.getSortProperty();
            sortDirection = prefetchConfiguration.getSortDirection();
        }
        
        topQuery.setBinSource(binSource != null ? binSource : BinSource.getDefault());
        Collection<Map<IGenericDimension, Object>> filters = iDimensionToIGenericDimensionFilters(prefetchKeys);
        topQuery.setFilterRequest(FilterRequest.getFilterRequestFromMap(filters));
        topQuery.setTimeGranularity(topCube.getTimeGranularity());
        topQuery.setOffset(0);
        topQuery.setLength(-1);
        sortDirection = sortDirection != null ? sortDirection
            : SortDirection.DSC;
        topQuery.setSortDirection(sortDirection);
        sortProperty = sortProperty != null ? sortProperty
            : outputMeasures.iterator()
                .next();
        sortProperty = sortProperty instanceof IDimension ? getGenericDimensionID((IDimension) sortProperty)
            : sortProperty;
      //Incase sort property is on some measure which is derived from ByteBuffer
        if(sortProperty instanceof IMeasure && ((IMeasure)sortProperty).getType().isDerived()) {
        	HashSet<IMeasure> newResponseMeasures = new HashSet<IMeasure>();
        	newResponseMeasures.add((IMeasure)sortProperty);
            topQuery.setResponseMeasures(newResponseMeasures);
        }
        topQuery.setSortProperty(sortProperty);
        Map<IKey, Object> paramerterMap = new HashMap<IKey, Object>();
        paramerterMap.put(ReqParamKey.RUBIX_CACHE_COMPRESSION_INTERVAL, level);
        topQuery.setParamerterMap(paramerterMap);
        topQuery.setQueryRequestMode(QueryRequestMode.SCHEDULER);
        return topQuery;
    }

    private Collection<Map<IGenericDimension, Object>> iDimensionToIGenericDimensionFilters(
        List<Map<IDimension, Integer>> list) {
        Collection<Map<IGenericDimension, Object>> filters = Lists.newArrayList();
        if (list != null) {
            for (Map<IDimension, Integer> map : list) {
                Map<IGenericDimension, Object> filter = Maps.newHashMap();
                for (Entry<IDimension, Integer> e : map.entrySet()) {
                    IGenericDimension genericDimension = genericDimensionReverseMap.get(e.getKey());
                    if (genericDimension == null) {
                        throw new IllegalArgumentException(
                            "Generic Dimension not found for :" + e.getKey());
                    }
                    filter.put(genericDimension, e.getValue());
                }
                filters.add(filter);
            }
        }
        return filters;
    }

    private QueryRequest generateProfileRequest(QueryRequest subQuery,
        ICube profileCube, long startTime, long endTime, ICube topCube, Long level) {
        QueryRequest superQuery = new QueryRequest();
        superQuery.setStartTime(startTime);
        superQuery.setEndTime(endTime);
        superQuery.setLength(-1);
        superQuery.setTimeGranularity(profileCube.getTimeGranularity());
        Map<IKey, Object> superParamerterMap = new HashMap<IKey, Object>();
        superParamerterMap.put(ReqParamKey.RUBIX_CACHE_COMPRESSION_INTERVAL, level);
        superQuery.setParamerterMap(superParamerterMap);
        
        //only request for the dimensions which are in distribution key, so that the request is disjoint
        //and it brings the minimum possible tuples
        superQuery.setResponseDimensions(getGenericDimensionIDs(profileCube.getDistributionKeys()));
        superQuery.setCubeContextDimensions(getGenericDimensionIDs(profileCube.getOutputDimensions()));
        
        SortDirection sortDirection = null;
        ICubeProperty sortProperty = null;
        
        BinSource binSource = subQuery.getBinSource();
        

        superQuery.setBinSource(binSource != null ? binSource : BinSource.getDefault());
        superQuery.setResponseMeasures(profileCube.getOutputMeasures());
        superQuery.setFilterRequest(FilterRequest.newEmptyFilterRequest());
        subQuery = new QueryRequest(subQuery);
        subQuery.setResponseDimensions(getGenericDimensionIDs( Utility.getNonStaticKeys( profileCube)));
        subQuery.setLength(MAX_SINGLE_ENTITY_SIZE);

        //get sort direction and property from topcube
        PrefetchConfiguration topCubePrefetchConfiguration = topCube.getPrefetchConfiguration();
		if(topCubePrefetchConfiguration != null) {
        	sortProperty = topCubePrefetchConfiguration.getSortProperty();
            sortDirection = topCubePrefetchConfiguration.getSortDirection();
        }
		LinkedHashSet<IMeasure> outputMeasures = topCube.getOutputMeasures();
        sortDirection = sortDirection != null ? sortDirection
                : SortDirection.DSC;
        subQuery.setSortDirection(sortDirection);
        sortProperty = sortProperty != null ? sortProperty
            : outputMeasures.iterator()
                .next();
        sortProperty = sortProperty instanceof IDimension ? getGenericDimensionID((IDimension) sortProperty)
            : sortProperty;
        subQuery.setSortProperty(sortProperty);
            
        if (!(subQuery.getResponseMeasures()
            .contains(subQuery.getSortProperty()) || subQuery.getResponseDimensions()
            .contains(subQuery.getSortProperty()))) {
            subQuery.setSortProperty(subQuery.getResponseMeasures()
                .iterator()
                .next());
        }
        Map<IKey, Object> subQueryParamerterMap = new HashMap<IKey, Object>();
        subQueryParamerterMap.put(ReqParamKey.RUBIX_CACHE_COMPRESSION_INTERVAL, level);
        subQuery.setParamerterMap(subQueryParamerterMap);
        superQuery.setSubQuery(subQuery);
        superQuery.setQueryRequestMode(QueryRequestMode.SCHEDULER);
        subQuery.setQueryRequestMode(QueryRequestMode.SCHEDULER);
        
        return superQuery;
    }
	
    private Set<IGenericDimension> getGenericDimensionIDs(
        LinkedHashSet<IDimension> outputDimensions) {
        Set<IGenericDimension> genericDimensions = new LinkedHashSet<IGenericDimension>();
        for (IDimension dimension : outputDimensions) {
            genericDimensions.add(getGenericDimensionID(dimension));
        }
        return genericDimensions;
    }

    private IGenericDimension getGenericDimensionID(IDimension dimension) {
        IGenericDimension genericDimension = genericDimensionReverseMap.get(dimension);
        if (genericDimension == null) {
            throw new IllegalArgumentException(
                "Generic Dimension not found for :" + dimension);
        }
        return genericDimension;
    }
    
    private boolean hasLocalCube(PrefetchCubeConfiguration prefetchCubeConfiguration){
    	if(Utility.isNullOrEmpty(prefetchCubeConfiguration.getTopCube().getDistributionKeys()))
    		return true;
    	
    	for(ICube profileCube: prefetchCubeConfiguration.getProfileCubes()){
    		if(Utility.isNullOrEmpty(profileCube.getDistributionKeys()))
    			return true;
    	}
    	
    	return false;
    }
    
    @Override
	public void run() {
		try {
			boolean isFirstRun = false;
			int version = this.version.get();
			Map<String, Map<String, Map<PrefetchCubeConfiguration, Long>>> tempLastCacheUpdateTimeMap = lastCacheUpdateTimeMap;
			if(version != this.version.get()) {
				logger.info("skipping task creation as view has changed version changed  from {} to {}", version, this.version.get());
				return;
			}
			if (lastCacheUpdateTimeMap.size() == 0) {
				isFirstRun = true;
			}
			SortedSet<QueryPrefetchTaskCombiner> combinerSet = new TreeSet<QueryPrefetchTaskCombiner>();
			Map<String, Map<String, Map<Long,Interval>>> binClassToBinSourceMap = Controller
					.getInstance().getBinClassToBinSourcesJobTime();
			Map<String, List<PrefetchCubeConfiguration>> binClassToCubeConfigurations = cubeLocator
					.getPrefetchCubeConfigurations();

			if (logger.isDebugEnabled()) {
				logger.debug("prefetch cube configuration is==>"
						+ binClassToCubeConfigurations);
			}
			for (Map.Entry<String, List<PrefetchCubeConfiguration>> cubeConfigurations : binClassToCubeConfigurations
					.entrySet()) {
				Map<String, Map<PrefetchCubeConfiguration, Long>> binSourceToCacheTime = tempLastCacheUpdateTimeMap
						.get(cubeConfigurations.getKey());
				if (binSourceToCacheTime == null) {
					binSourceToCacheTime = new HashMap<String, Map<PrefetchCubeConfiguration, Long>>();
					tempLastCacheUpdateTimeMap.put(cubeConfigurations.getKey(),
							binSourceToCacheTime);
				}
				if (!isFirstRun) {
					binClassToBinSourceMap = Controller.getInstance()
							.getBinClassToBinSourcesJobTime();
				}
				Map<String, Map<Long,Interval>> binSourceToIntervalMap = binClassToBinSourceMap
						.get(cubeConfigurations.getKey());
				for (Map.Entry<String, Map<Long,Interval>> binSourceToInterval : binSourceToIntervalMap
						.entrySet()) {
					Map<PrefetchCubeConfiguration, Long> cubeConfigurationToCacheTime = binSourceToCacheTime
							.get(binSourceToInterval.getKey());
					if (cubeConfigurationToCacheTime == null) {
						cubeConfigurationToCacheTime = new HashMap<PrefetchCubeConfiguration, Long>();
						binSourceToCacheTime.put(binSourceToInterval.getKey(),
								cubeConfigurationToCacheTime);
					}

					long startTime = binSourceToInterval.getValue().get(Controller.DEFAULT_AGGR_INTERVAL).getStartTime();
					
					startTime = ConfigFactory.getInstance()
							.getBean(ISchedulerPolicy.class)
							.getCeilOfTime(startTime);
					long prefetcherLatestTime = RubixProperties.DataPrefetchSchedulerLatestSETime
							.getLongValue();
					long endTime = prefetcherLatestTime != -1 ? prefetcherLatestTime
							: binSourceToInterval.getValue().get(Controller.DEFAULT_AGGR_INTERVAL).getEndTime();

					SortedMap<Long, QueryPrefetchTaskCombiner> map = new TreeMap<Long, QueryPrefetchTaskCombiner>();
					for (long tempEndTime = getNextEndTime(startTime, endTime); startTime < endTime; startTime = tempEndTime, tempEndTime = getNextEndTime(startTime, endTime)) {
						QueryPrefetchTaskCombiner combiner = new QueryPrefetchTaskCombiner(
								isFirstRun, taskManager, version);

						for (PrefetchCubeConfiguration prefetchCubeConfiguration : cubeConfigurations
								.getValue()) {
							Long lastCacheUpdatedTime = cubeConfigurationToCacheTime
									.get(prefetchCubeConfiguration);
							if (lastCacheUpdatedTime != null
									&& lastCacheUpdatedTime != 0
									&& tempEndTime < lastCacheUpdatedTime) {
								tempEndTime = lastCacheUpdatedTime;
								continue;
							}
//							if (lastCacheUpdatedTime != null
//									&& lastCacheUpdatedTime != 0) {
//								startTime = lastCacheUpdatedTime;
//							}

							// if RubixProperties.RunSchedulerOnlyOnCoordinator
							// is
							// true,
							// then run schedule distributed cubes only on the
							// co-ordinator
							if (RubixProperties.RunSchedulerOnlyOnCoordinator
									.getBooleanValue()
									&& !RubixDistribution.getInstance()
											.isCoordinator()
									&& !hasLocalCube(prefetchCubeConfiguration))
								continue;

							String binClass = cubeConfigurations.getKey();
							if (!Utility.validString(binClass)) {
								binClass = prefetchCubeConfiguration
										.getTopCube().getTimeGranularity()
										.getName();
							}

							if (binSourceToCacheTime == null) {
								binSourceToCacheTime = Maps.newHashMap();
							}

							Map<String, Object> optionalParam = new HashMap<String, Object>();
							optionalParam.put(BIN_SOURCE,
									binSourceToInterval.getKey());
							optionalParam.put(LAST_BIN_TIME,
									binSourceToInterval.getValue());
							optionalParam.put(VERSION,
									version);
							PrefetchLastCacheUpdateTimeAndInterval lastCacheUpdateAndPrefetchIntervals = ConfigFactory
									.getInstance()
									.getBean(ISchedulerPolicy.class)
									.getIntervalsAndLastUpdateTime(startTime,
											tempEndTime,
											prefetchCubeConfiguration,
											isFirstRun, optionalParam);
							Set<Interval> prefetchIntervals = lastCacheUpdateAndPrefetchIntervals
									.getIntervals();
							map.put(startTime,
									combiner);
							for (Interval eachInterval : prefetchIntervals) {
								// TODO : This Does not work For Month gran cube
								// in
								// month of Feb. Currently no solution has month
								// gran cube
								long cubeGranularity = prefetchCubeConfiguration
										.getTopCube().getTimeGranularity()
										.getGranularity();
								if (eachInterval.getGranularity() >= cubeGranularity
										&& eachInterval.getStartTime() == Utility.floorFromGranularity(eachInterval.getStartTime(),cubeGranularity)
										&& eachInterval.getEndTime() == Utility.floorFromGranularity(eachInterval.getEndTime(),cubeGranularity)) {
									List<PrefetchTaskRequest> taskRequests = createPrefetchTaskRequests(
											prefetchCubeConfiguration,
											eachInterval.getStartTime(),
											eachInterval.getEndTime(),
											binSourceToInterval.getKey(), cubeConfigurations.getKey(), eachInterval.getGranularity(),binSourceToInterval.getValue());

									for (PrefetchTaskRequest taskRequest : taskRequests) {

										logger.debug("Queueing Task for :"
												+ taskRequest);
										if (!saveRequests) {
											if (isFirstRun) {
												//add the Task in its combiner for that we have to find the combiner for task.
												if (taskRequest
														.getQueryRequest()
														.getStartTime() >= startTime) {
													QueryPrefetchTaskCombiner set = map
															.get(startTime);
													if (set != null) {
														set.getQueryPrefetchTasks()
																.add(new QueryPrefetchTask(
																		dataService,
																		taskRequest, version, taskManager));
													} else {
														combiner.getQueryPrefetchTasks()
																.add(new QueryPrefetchTask(
																		dataService,
																		taskRequest, version, taskManager));
														map.put(startTime,
																combiner);
													}
												} else {
													//get combiner task in which this task should move
													SortedMap<Long, QueryPrefetchTaskCombiner> headMap = map
															.headMap(taskRequest
																	.getQueryRequest()
																	.getStartTime() + TimeGranularity.ONE_MINUTE.getGranularity());
													QueryPrefetchTaskCombiner otherCombiner = headMap.get(headMap
															.lastKey());
													if(otherCombiner.getGranToIntervalMap().get(eachInterval.getGranularity()) < eachInterval.getEndTime()) {
														otherCombiner.getGranToIntervalMap().put(eachInterval.getGranularity(), eachInterval.getEndTime());
													}
													otherCombiner
													.getQueryPrefetchTasks()
													.add(new QueryPrefetchTask(
															dataService,
															taskRequest, version, taskManager));
												}
											} else {
												//if future run add task in current combiner only 
												combiner.getQueryPrefetchTasks()
														.add(new QueryPrefetchTask(
																dataService,
																taskRequest, version, taskManager));

											}
										} else {
											requestLists.add(taskRequest);
										}
									}
								}
							}
							combiner.setStartTime(startTime);
							combiner.setEndTime(tempEndTime);
							combiner.setBinClass(binClass);
							combiner.setBinSource(binSourceToInterval.getKey());
							combiner.setGranToIntervalMap(lastCacheUpdateAndPrefetchIntervals
									.getCacheEndTimeMap());
							//if future task set task entime and last bin time. because we are moving insta availability slowly
							combiner.setLastBinTime(isFirstRun?endTime:tempEndTime);
							if (lastCacheUpdateAndPrefetchIntervals
									.getCacheLastUpdateTime() != 0) {
								cubeConfigurationToCacheTime.put(
										prefetchCubeConfiguration,
										lastCacheUpdateAndPrefetchIntervals
												.getCacheLastUpdateTime());
							}
							// exit from here if scheduler is restarted
							if (version != this.version.get()) {
								return;
							}
						}
						if (isFirstRun && !saveRequests) {
							combinerSet.add(combiner);
						} else if (combiner.getQueryPrefetchTasks().size() != 0
								&& !saveRequests) {
							synchronized (combiner) {
								taskManager.submitTask(combiner);
								combiner.wait();
							}
						}
					}
				}
			}

			for (Iterator iterator = combinerSet.iterator(); iterator.hasNext();) {
				QueryPrefetchTaskCombiner queryPrefetchTaskCombiner = (QueryPrefetchTaskCombiner) iterator
						.next();
				if(queryPrefetchTaskCombiner.getQueryPrefetchTasks().size()!=0)
					taskManager.submitTask(queryPrefetchTaskCombiner);
			}

		} catch (Throwable t) {
			logger.error("Error while producing scheduling task.", t);
			// Throwables.propagate(t); - RIX - 1043
		}
	}

	private long getNextEndTime(long startTime, long endTime) {
		Calendar instance = Utility.newCalendar();
		long tempEndTime = Utility.getNextTimeFromGranularity(startTime,
				RubixProperties.SchedulerMaxSegmentDuration.getIntValue(),
				instance);
		if (tempEndTime > endTime) {
			return endTime;
		}
		return tempEndTime;
	}

	private boolean isTimeRangeValid(String binClass, String binSource,
			long level, ICube cube,Map<Long, Interval> aggrGranToLastBinInterval, long endTime, long startTime) {
		if(Utility.isTreeCache(cube)){
			Boolean forcePopulateFromAggrInterval = cube.getPrefetchConfiguration()!=null?cube.getPrefetchConfiguration().isForcePopulationOn():false;
			if(forcePopulateFromAggrInterval){
				|*
				 * Force population: if request doesn't lie in lastBinInterval 
				 * return empty list from here as no request need to be created
		         *|
				long binTimeGran = Bin.getBin(binClass).getTimeGranularity().getGranularity();
				long aggrInterval = level;
				if(binTimeGran==level){
					aggrInterval=Controller.DEFAULT_AGGR_INTERVAL;
				}
				Interval lastBinInterval = aggrGranToLastBinInterval.get(aggrInterval);
				if(lastBinInterval==null){
					return false;
				}else if(endTime > lastBinInterval.getEndTime() || startTime < lastBinInterval.getStartTime()){
					return false;
				}
			}
			
			|*
			 * check if request lie in variable retention map of the cube
			 *|
			long lastBinEndtime = aggrGranToLastBinInterval.get(Controller.DEFAULT_AGGR_INTERVAL).getEndTime();
			EvictionDetails evictionDetails = getEvictionDetails(cube);
			SortedMap<Long,Integer> allRetentionPoints =  evictionDetails.getAllRetentionPoints();;
			if(!RubixCacheFactory.useFlash()){
				allRetentionPoints=evictionDetails.getMemoryAndDiskRetentionPoints();
			}
			if(allRetentionPoints.containsKey(level)){
				int numPoints = allRetentionPoints.get(level);
				long  rangeStartTime = TreeRubixCacheVariableRetentionPolicy.getRangeStartTime(lastBinEndtime, level, numPoints);
				if(endTime <=rangeStartTime){
					return false;
				}
			}else{
				return false;
			}
		}
		
		return true;
	}
     private List<PrefetchTaskRequest> createPrefetchTaskRequests(
        PrefetchCubeConfiguration prefetchCubeConfiguration, long startTime,
        long endTime, String binSource, String binClass, Long level, Map<Long,Interval> aggrGranToLastBinInterval) {
    	 
    	|*
    	 * we are here bcoz either this is co-ordinator or the prefetch config has a local cube
    	 * make tasks for only the local cubes if this is not the co-ordinator
    	 *|
        List<PrefetchTaskRequest> taskRequests = new ArrayList<PrefetchTaskRequest>();
        ICube topCube = prefetchCubeConfiguration.getTopCube();
        QueryRequest topRequest = generateTopRequest(topCube, startTime,
            endTime,binSource, level);
        
		if (!RubixProperties.RunSchedulerOnlyOnCoordinator.getBooleanValue()
				|| RubixDistribution.getInstance().isCoordinator()
				|| Utility.isNullOrEmpty(topCube.getDistributionKeys())){
		
			if(isTimeRangeValid(binClass, binSource, level, topCube,aggrGranToLastBinInterval, endTime, startTime)){
				taskRequests.add(makePrefetchTaskRequest(topCube, topRequest));
			}
		}
        
		for (ICube cube : prefetchCubeConfiguration.getProfileCubes()) {
			if (!RubixProperties.RunSchedulerOnlyOnCoordinator
					.getBooleanValue()
					|| RubixDistribution.getInstance().isCoordinator()
					|| Utility.isNullOrEmpty(cube.getDistributionKeys())) {
				PrefetchConfiguration prefetchConfiguration = cube
						.getPrefetchConfiguration();
				if (prefetchConfiguration != null
						&& prefetchConfiguration.isShouldPreload() && isTimeRangeValid(binClass, binSource, level, cube, aggrGranToLastBinInterval, endTime, startTime)) {
					taskRequests.add(makePrefetchTaskRequest(
							cube,
							generateProfileRequest(topRequest, cube, startTime,
									endTime, topCube, level)));
				}
			}
		}
        return taskRequests;
    }

     private EvictionDetails getEvictionDetails(ICube cube){
    	 String key = cube.getCacheIdentifier().getKeyOfFirstEntry();
     	if(cube.getCacheType().isSingleEntity()){
     		key += SingleEntityRubixCache.SINGLE_ENTITY_CACHE_IDENTIFIER;
     	}
     	return CacheUtils.getEvictionDetails(key, true, cube.getTimeGranularity().getGranularity());
     }
	private PrefetchTaskRequest makePrefetchTaskRequest(ICube cube,
        QueryRequest request) {
        PrefetchTaskRequest topPrefetchTaskRequest = new PrefetchTaskRequest();
        topPrefetchTaskRequest.cashIdentifier=cube.getCacheIdentifier().toString();
        topPrefetchTaskRequest.setQueryRequest(request);
		if (cube.getCacheType() == CacheType.ALL_TIMESERIES|| cube.getCacheType() == CacheType.SINGLE_ENTITY_TIMESERIES) {
			topPrefetchTaskRequest.setRequestDataType(RequestDataType.TimeSeries);
		} else {
			topPrefetchTaskRequest.setRequestDataType(RequestDataType.Aggregate);
		}
        
        return topPrefetchTaskRequest;
    }

    public void clearTaskCacheUpdateTimeMap() {
        lastCacheUpdateTimeMap = new HashMap<String, Map<String,Map<PrefetchCubeConfiguration,Long>>>();
    }
}

*/
}