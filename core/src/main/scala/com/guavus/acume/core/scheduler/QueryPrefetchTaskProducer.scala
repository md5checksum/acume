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
import com.guavus.qb.cube.schema.ICube
import com.guavus.acume.core.DataService
import com.guavus.qb.cube.schema.QueryBuilderSchema
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
import com.guavus.qb.cube.schema.FieldType
import com.guavus.rubix.query.remote.flex.NameValue
import java.util.{HashMap => JHashMap}
import scala.collection.mutable.{HashMap => SHashMap}
import com.guavus.acume.core.AcumeService
import scala.collection.mutable.HashMap

object QueryPrefetchTaskProducer {

  val BIN_SOURCE = "binSource"

  val LAST_BIN_TIME = "lastBinTime"

  val VERSION = "version"

  val logger = LoggerFactory.getLogger(classOf[QueryPrefetchTaskProducer])

}

class QueryPrefetchTaskProducer(acumeConf : AcumeConf, schemas : List[QueryBuilderSchema], private var taskManager: QueryRequestPrefetchTaskManager, private var dataService: DataService, acumeService : AcumeService, saveRequests : Boolean, policy : ISchedulerPolicy, controller : Controller) extends Runnable {

  private val lastCacheUpdateTimeMap: HashMap[String, HashMap[PrefetchCubeConfiguration, Long]] = new HashMap[String, HashMap[PrefetchCubeConfiguration, Long]]()

  private val cubeLocator: PrefetchCubeLocator = new PrefetchCubeLocator(schemas)

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
      var binSourcesToIntervalsMap = controller.getInstaTimeInterval
      val binSourceToCubeConfigurations = cubeLocator.getPrefetchCubeConfigurations
      if (logger.isDebugEnabled) {
        logger.debug("prefetch cube configuration is==>" + binSourceToCubeConfigurations)
      }
      for ((key, value) <- binSourceToCubeConfigurations) {
        var cubeConfigurationToCacheTime = tempLastCacheUpdateTimeMap.get(key).getOrElse(null)
        if (cubeConfigurationToCacheTime == null) {
          cubeConfigurationToCacheTime = new scala.collection.mutable.HashMap[PrefetchCubeConfiguration, Long]()
          tempLastCacheUpdateTimeMap.put(key, cubeConfigurationToCacheTime)
        }
        if (!isFirstRun) {
          binSourcesToIntervalsMap = controller.getInstaTimeInterval
        }
        val intervalMap = binSourcesToIntervalsMap.get(key).getOrElse({throw new IllegalStateException("StartTime for binsource " + key + " can not be null")})
//          var cubeConfigurationToCacheTime = binSourceToCacheTime.get(key).getOrElse({null})
//          if (cubeConfigurationToCacheTime == null) {
//            cubeConfigurationToCacheTime = new scala.collection.mutable.HashMap[PrefetchCubeConfiguration, Long]()
//            binSourceToCacheTime.put(key, cubeConfigurationToCacheTime)
//          }
          var startTime = intervalMap.get(-1).getOrElse({throw new IllegalStateException("StartTime for binsource " + key + " can not be null")}).getStartTime
          startTime = policy.getCeilOfTime(startTime)
          val endTime = intervalMap.get(-1).getOrElse(throw new IllegalStateException("EndTime for binsource " + key + " can not be null")).getEndTime
          val map = new java.util.TreeMap[Long, QueryPrefetchTaskCombiner]()
          var tempEndTime = getNextEndTime(startTime, endTime)
          while (startTime < endTime) {
            val combiner = new QueryPrefetchTaskCombiner(isFirstRun, taskManager, version, acumeConf, acumeService, controller)
            for (prefetchCubeConfiguration <- value) {
              val lastCacheUpdatedTime = cubeConfigurationToCacheTime.get(prefetchCubeConfiguration).getOrElse({null}).asInstanceOf[Long]
              if (lastCacheUpdatedTime != null && lastCacheUpdatedTime != 0 && tempEndTime < lastCacheUpdatedTime) {
                tempEndTime = lastCacheUpdatedTime
              } else {
              if (cubeConfigurationToCacheTime == null) {
                cubeConfigurationToCacheTime = scala.collection.mutable.HashMap()
              }
              val optionalParam = new HashMap[String, Any]()
              optionalParam.put(BIN_SOURCE, key)
              optionalParam.put(LAST_BIN_TIME, intervalMap.get(-1).getOrElse({throw new IllegalStateException("EndTime for binsource " + key + " can not be null")}))
              optionalParam.put(VERSION, version)
              val lastCacheUpdateAndPrefetchIntervals = policy.getIntervalsAndLastUpdateTime(startTime, tempEndTime, prefetchCubeConfiguration, isFirstRun, optionalParam, taskManager)
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
                        if (taskRequest.getQueryRequest.getStartTime >= startTime) {
                          val set = map.get(startTime)
                          if (set != null) {
                            set.getQueryPrefetchTasks.add(new QueryPrefetchTask(dataService, taskRequest, version, taskManager, acumeConf))
                          } else {
                            combiner.getQueryPrefetchTasks.add(new QueryPrefetchTask(dataService, taskRequest, version, taskManager, acumeConf))
                            map.put(startTime, combiner)
                          }
                        } else {
                          val headMap = map.headMap(taskRequest.getQueryRequest.getStartTime + TimeGranularity.ONE_MINUTE.getGranularity)
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
  def clearTaskCacheUpdateTimeMap() {
    lastCacheUpdateTimeMap.clear
  }
}