package com.guavus.acume.core

import java.util.ArrayList
import java.util.Arrays
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.future

import org.apache.spark.sql.catalyst.expressions.Row
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil
import com.guavus.acume.cache.workflow.AcumeCacheResponse
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.acume.cache.workflow.RequestType.Aggregate
import com.guavus.acume.cache.workflow.RequestType.SQL
import com.guavus.acume.cache.workflow.RequestType.Timeseries
import com.guavus.acume.core.configuration.DataServiceFactory
import com.guavus.qb.cube.schema.QueryBuilderSchema
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.rubix.query.remote.flex.AggregateResponse
import com.guavus.rubix.query.remote.flex.AggregateResultSet
import com.guavus.rubix.query.remote.flex.QueryRequest
import com.guavus.rubix.query.remote.flex.TimeseriesResponse
import com.guavus.rubix.query.remote.flex.TimeseriesResultSet
import com.guavus.rubix.search.SearchRequest
import com.guavus.rubix.search.SearchResponse
import com.guavus.rubix.user.management.utils.HttpUtils

import DataService.classificationStats
import DataService.counter
import DataService.logger
import DataService.poolStats
import DataService.queryPoolPolicy
import DataService.{ queryPoolPolicy_= => queryPoolPolicy_= }
import DataService.queryPoolSchedulerPolicy
import DataService.queryPoolUIPolicy
import javax.xml.bind.annotation.XmlRootElement

/**
 * This class interacts with query builder and Olap cache.
 */
class DataService(queryBuilderService: Seq[IQueryBuilderService], val acumeContext: AcumeContextTrait, datasourceName: String) {

  /**
   * Takes QueryRequest i.e. Rubix query and return aggregate Response.
   */
  def servAggregate(queryRequest: QueryRequest, property: HashMap[String, Any] = null): AggregateResponse = {
    servRequest(queryRequest.toSql(""), RequestType.Aggregate, property).asInstanceOf[AggregateResponse]
  }

  /**
   * Takes QueryRequest i.e. Rubix query and return timeseries Response.
   */
  def servTimeseries(queryRequest: QueryRequest, property: HashMap[String, Any] = null): TimeseriesResponse = {
    servRequest(queryRequest.toSql("ts,"), RequestType.Timeseries, property).asInstanceOf[TimeseriesResponse]
  }

  def servSearchRequest(queryRequest: SearchRequest): SearchResponse = {
    servSearchRequest(queryRequest.toSql, RequestType.SQL)
  }

  def servSearchRequest(unUpdatedSql: String, requestDataType: RequestType.RequestType): SearchResponse = {
    val sql = DataServiceFactory.dsInterpreterPolicy.updateQuery(unUpdatedSql)
    val (response, rows) = execute(sql)
    val fields = queryBuilderService.get(0).getQuerySchema(sql, response.schemaRDD.schema.fieldNames.toList)
    val acumeSchema: QueryBuilderSchema = queryBuilderService.get(0).getQueryBuilderSchema
    val dimsNames = new ArrayList[String]()
    for (field <- fields) {
      dimsNames += field
    }
    val finalOutput = new ArrayList[java.util.List[Any]]();
    for(row <- rows) {
      val innerList = new ArrayList[Any]()
      for(value <- row.toSeq) {
        innerList.add(value)
      }
      finalOutput.add(innerList)
    }
    new SearchResponse(dimsNames, finalOutput)
  }

  private def setSparkJobLocalProperties() {
    for ((key, value) <- AcumeCacheContextTraitUtil.poolThreadLocal.get()) {
      var propValue: String = if (value != null) value.toString else null
      acumeContext.acc.cacheSqlContext.sparkContext.setLocalProperty(key, propValue)
    }
    AcumeCacheContextTraitUtil.setSparkSqlShufflePartitions(acumeContext.acc.cacheSqlContext.getConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS))
  }
  
  private def getSparkJobLocalProperties() = {
    if(AcumeCacheContextTraitUtil.poolThreadLocal.get() == null) {
      AcumeCacheContextTraitUtil.poolThreadLocal.set(HashMap[String, Any]())
    }
    AcumeCacheContextTraitUtil.poolThreadLocal.get()
  }

  private def unsetSparkJobLocalProperties() {
    for ((key, value) <- AcumeCacheContextTraitUtil.poolThreadLocal.get()) {
      acumeContext.acc.cacheSqlContext.sparkContext.setLocalProperty(key, null)
    }
    acumeContext.acc.cacheSqlContext.setConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, AcumeCacheContextTraitUtil.getSparkSqlShufflePartitions)
    try{
    	AcumeCacheContextTraitUtil.unsetAll(acumeContext.acc)      
    } catch {
      case ex : Exception => logger.error("Error while unsetting sparkProperties", ex)
      case th : Throwable => logger.error("Error while unsetting sparkProperties", th)
    }
    
  }

  private def getJobDescription(isSchedulerQuery: Boolean, jobGroup: String) = {
    if (isSchedulerQuery) {
      "[scheduler]"
    } else {
      Array[String]("[" + HttpUtils.getLoginInfo() + "]").mkString("-")
    }
  }

  def checkJobLevelProperties(requests: java.util.ArrayList[_ <: Any], requestDataType: RequestType.RequestType): (List[(String, HashMap[String, Any])], List[String]) = {
    this.synchronized {

      var sqlList: java.util.ArrayList[String] = new java.util.ArrayList()
      requests foreach (request => {
        requestDataType match {
          case Aggregate => sqlList.add(request.asInstanceOf[QueryRequest].toSql(""))
          case Timeseries => sqlList.add(request.asInstanceOf[QueryRequest].toSql("ts,"))
          case SQL => sqlList.add(request.asInstanceOf[String])
          case _ => throw new IllegalArgumentException("QueryExecutor does not support request type: " + requestDataType)
        }
      })
      
      val isSchedulerQuery = queryBuilderService.get(0).isSchedulerQuery(sqlList.get(0))
      queryPoolPolicy = if (isSchedulerQuery) queryPoolSchedulerPolicy else queryPoolUIPolicy
      
      var classificationDetails = queryPoolPolicy.getQueriesClassification(sqlList.toList, classificationStats)
      var poolList: java.util.ArrayList[String] = new java.util.ArrayList()
      var classificationList: java.util.ArrayList[(String, HashMap[String, Any])] = new java.util.ArrayList()
      
      queryPoolPolicy.checkForThrottle(classificationDetails.get(0)._1, classificationStats, queryPoolPolicy.getNumberOfQueries(classificationDetails.map(x => x._1)))
      
      classificationDetails foreach (classification => {
        var poolname = queryPoolPolicy.getPoolNameForClassification(classification._1, poolStats)
        poolList.add(poolname)
        classificationList.add(new Tuple2(classification._1, classification._2))
      })
      
      new Tuple2(classificationList.toList, poolList.toList)
    }
  }

  def servRequest(unUpdatedSql: String, requestDataType: RequestType.RequestType, property: HashMap[String, Any] = null): Any = {
    
    val sql = DataServiceFactory.dsInterpreterPolicy.updateQuery(unUpdatedSql)
    
    val jobGroupId = Thread.currentThread().getName() + "-" + Thread.currentThread().getId() + "-" + counter.getAndIncrement
    try {
      if (AcumeCacheContextTraitUtil.poolThreadLocal.get() == null) {
        AcumeCacheContextTraitUtil.poolThreadLocal.set(new HashMap[String, Any]())
      }
      val isSchedulerQuery = queryBuilderService.get(0).isSchedulerQuery(sql)
      val jobDescription = getJobDescription(isSchedulerQuery, Thread.currentThread().getName() + Thread.currentThread().getId())
      logger.info(jobDescription)
      
      def runWithTimeout[T](f: => (AcumeCacheResponse, Array[Row])): (AcumeCacheResponse, Array[Row]) = {
        lazy val fut = future { f }
        Await.result(fut, DurationInt(AcumeConf.acumeConf.getInt(ConfConstants.queryTimeOut).getOrElse(30)) second)
      }
      
      def run(sql: String, jobGroupId : String, jobDescription : String, conf: AcumeConf, localProperties : HashMap[String, Any]) = {

        getSparkJobLocalProperties ++= localProperties
        setSparkJobLocalProperties
        try {
          conf.setDatasourceName(datasourceName)
          acumeContext.sc.setJobGroup(jobGroupId, jobDescription, false)
          execute(sql)
        } finally {
          unsetSparkJobLocalProperties
        }
      }
      
      val localProperties = if (property == null) getSparkJobLocalProperties else property
      val (cacheResponse, rows) = run(sql, jobGroupId, jobDescription, acumeContext.acumeConf, localProperties)
      
      val fields = queryBuilderService.get(0).getQuerySchema(sql, cacheResponse.schemaRDD.schema.fieldNames.toList)
      
      val acumeSchema: QueryBuilderSchema = queryBuilderService.get(0).getQueryBuilderSchema
      val dimsNames = new ArrayBuffer[String]()
      val measuresNames = new ArrayBuffer[String]()
      var j = 0
      var isTimeseries = RequestType.Timeseries.equals(requestDataType)
      
      val dimensions = new ArrayBuffer[Boolean](fields.size)
      
      var tsIndex = 0
      for (i <- 0 to (fields.size-1)) {
        val field = fields(i)
        dimensions(i) = false
        if (field.equalsIgnoreCase("ts") || field.equalsIgnoreCase("timestamp")) {
          isTimeseries = {
            if(RequestType.Aggregate.equals(requestDataType)) {
              // In hbase, ts is a dimension. Adding ts to dimfields even in case of Aggregate
              dimsNames += field
            	false
            }
            else
              true
          }
          tsIndex = j
        } else if (queryBuilderService.get(0).isFieldDimension(field)) {
          dimsNames += field
          dimensions(i) = true
        } else {
          measuresNames += field
        }
        j += 1
      }

      if (isTimeseries) {
        val sortedRows = rows.sortBy(row => row(tsIndex).toString)
       
        val timestamps : List[Long] = {
          val metaDataTimeStamps = cacheResponse.metadata.timestamps
          if(metaDataTimeStamps == null || metaDataTimeStamps.isEmpty)
            sortedRows.map(row => row(tsIndex).toString.toLong).toList.distinct
          else
            metaDataTimeStamps
        }
        
        val timestampsToIndexMap = new scala.collection.mutable.HashMap[Long, Int]()
        var index = -1
        timestamps.foreach(x => { index += 1; timestampsToIndexMap += (x -> index) })
        val rowToMeasureMap = new scala.collection.mutable.HashMap[ArrayBuffer[Any], ArrayBuffer[ArrayBuffer[Any]]]
        
        for (row <- sortedRows) {
          val dims = new ArrayBuffer[Any]()
          val measures = new ArrayBuffer[Any]()
          var i = 0
          var dimIndex, measureIndex = 0
          var timestamp = 0L
          for (k <- 0 to (fields.size-1)) {
            val field = fields(k)
            if (field.equalsIgnoreCase("ts") || field.equalsIgnoreCase("timestamp")) {
              timestamp = java.lang.Long.valueOf(row(i).toString)
            } else if (dimensions(k)) {
              if (row(i) != null)
                dims += row(i)
              else
                dims += queryBuilderService.get(0).getDefaultValueForField(dimsNames(dimIndex))
              dimIndex += 1
            } else {
              if (row(i) != null)
                measures += row(i)
              else
                measures += queryBuilderService.get(0).getDefaultValueForField(measuresNames(measureIndex))
              measureIndex += 1
            }
            i += 1
          }

          def initializeArray(): ArrayBuffer[ArrayBuffer[Any]] = {
            val measureArray = new Array[ArrayBuffer[Any]](measuresNames.size)
            i = 0
            while (i < measuresNames.size) {
              measureArray(i) = { val array = new Array[Object](timestamps.size); Arrays.fill(array, queryBuilderService.get(0).getDefaultValueForField(measuresNames(i)).asInstanceOf[Any]); new ArrayBuffer[Any]() ++= (array) }
              i += 1
            }
            new ArrayBuffer ++= measureArray
          }
          val measureArray = rowToMeasureMap.getOrElse(dims, initializeArray)
          var measureValueIndex = 0
          for (measure <- measures) {
            measureArray(measureValueIndex)(timestampsToIndexMap.get(timestamp).get) = measures(measureValueIndex)
            measureValueIndex += 1
          }
          rowToMeasureMap += (dims -> measureArray)
        }
        val tsResults = new ArrayBuffer[TimeseriesResultSet]()
        for (rowToMap <- rowToMeasureMap) {
          tsResults += new TimeseriesResultSet(rowToMap._1, rowToMap._2.map(_.asJava))
        }
        new TimeseriesResponse(tsResults, dimsNames, measuresNames, timestamps)
      } else {
        val list = new ArrayBuffer[AggregateResultSet](rows.size)
        for (row <- rows) {
          val dims = new ArrayBuffer[Any]()
          val measures = new ArrayBuffer[Any]()

          var i = 0
          var dimIndex, measureIndex = 0
          for (k <- 0 to (fields.size-1)) {
            val field = fields(k)
            if (dimensions(k)) {
              if (row(i) != null)
                dims += row(i)
              else
                dims += queryBuilderService.get(0).getDefaultValueForField(dimsNames(dimIndex))
              dimIndex += 1
            } else {
              if (row(i) != null)
                measures += row(i)
              else
                measures += queryBuilderService.get(0).getDefaultValueForField(measuresNames(measureIndex))
              measureIndex += 1
            }
            i += 1
          }
          list += new AggregateResultSet(dims, measures)
        }
        
        val totalRecords : Int = 
          if(cacheResponse.metadata.totalRecords < 0) 
            rows.size 
          else 
            cacheResponse.metadata.totalRecords.toInt
 
        new AggregateResponse(list, dimsNames, measuresNames, totalRecords)
      }
    } catch {
      case e: Throwable =>
        logger.error("Cancelling Query " + sql + " with GroupId " + jobGroupId, e)
        acumeContext.sc.cancelJobGroup(jobGroupId)
        throw e;
    } finally {
      unsetSparkJobLocalProperties
    }
  }
  
  def execute(sql: String) : (AcumeCacheResponse, Array[Row]) = {
    
    // Get the modified query from queryBuilder
    var isFirst: Boolean = true
    val modifiedSql: String = queryBuilderService.foldLeft("") { (result, current) =>
      if (isFirst) {
        isFirst = false
        current.buildQuery(sql)
      } else {
        current.buildQuery(result)
      }
    }

    // Execute Queries
    if (!modifiedSql.equals("")) {
      if (!queryBuilderService.iterator.next.isSchedulerQuery(sql)) {
        logger.info(modifiedSql)
        acumeContext.acc.acql(modifiedSql)
      } else {
        acumeContext.acc.acql(queryBuilderService.iterator.next.getTotalCountSqlQuery(modifiedSql))
      }
    } else {
      throw new RuntimeException(s"Invalid Modified Query")
    }
    
  }
  
}

object DataService {
  
  val logger = LoggerFactory.getLogger(this.getClass())
  var poolStats: PoolStats = new PoolStats()
  var classificationStats: ClassificationStats = new ClassificationStats()
  val counter = new AtomicLong(0l)

  val policyclass = AcumeContextTraitUtil.acumeConf.getSchedulerPolicyClass
  val throttleMap = AcumeContextTraitUtil.acumeConf.getMaxAllowedQueriesPerClassification.split(",")map(x => {
    val i = x.indexOf(":")
        (x.substring(0, i).trim, x.substring(i+1, x.length).trim.toInt)
  })
  
  val queryPoolUIPolicy: QueryPoolPolicy = Class.forName(policyclass).getConstructors()(0).newInstance(throttleMap.toMap).asInstanceOf[QueryPoolPolicy]
  val queryPoolSchedulerPolicy: QueryPoolPolicy = Class.forName(ConfConstants.queryPoolSchedPolicyClass).getConstructors()(0).newInstance().asInstanceOf[QueryPoolPolicy]
  var queryPoolPolicy: QueryPoolPolicy = null
}
