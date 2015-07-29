package com.guavus.acume.core

import com.guavus.rubix.query.remote.flex.TimeseriesResponse
import com.guavus.rubix.query.remote.flex.AggregateResponse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import com.guavus.rubix.query.remote.flex.QueryRequest
import com.guavus.qb.cube.schema.QueryBuilderSchema
import com.guavus.qb.conf.QBConf
import org.apache.spark.sql.SchemaRDD
import scala.collection.mutable.ArrayBuffer
import com.guavus.rubix.query.remote.flex.AggregateResultSet
import com.guavus.rubix.query.remote.flex.TimeseriesResultSet
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import java.util.Arrays
import com.guavus.rubix.query.remote.flex.SearchResponse
import com.guavus.rubix.query.remote.flex.SearchResponse
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.guavus.rubix.query.remote.flex.SearchRequest
import com.guavus.acume.cache.workflow.AcumeCacheResponse
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.acume.cache.common.QLType
import com.guavus.acume.cache.workflow.AcumeCacheResponse
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.rubix.user.management.utils.HttpUtils
import org.apache.shiro.SecurityUtils
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeoutException
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import acume.exception.AcumeException
import com.guavus.acume.core.exceptions.AcumeExceptionConstants
import com.guavus.acume.workflow.RequestDataType
import com.guavus.acume.cache.common.AcumeConstants
import java.util.concurrent.ConcurrentHashMap

/**
 * This class interacts with query builder and Olap cache.
 */
class DataService(queryBuilderService: Seq[IQueryBuilderService], val acumeContext: AcumeContextTrait) {

  val logger = LoggerFactory.getLogger(this.getClass())
  val counter = new AtomicLong(0l)
  var poolStats: PoolStats = new PoolStats()
  var classificationStats: ClassificationStats = new ClassificationStats()

  val policyclass = acumeContext.acumeConf.getSchedulerPolicyClass
  val throttleMap = acumeContext.acumeConf.get(ConfConstants.maxAllowedQueriesPerClassification, "default:25").split(",")map(x => {
	  val i = x.indexOf(":")
			  (x.substring(0, i).trim, x.substring(i+1, x.length).trim.toInt)
  })
  val queryPoolUIPolicy: QueryPoolPolicy = Class.forName(policyclass).getConstructors()(0).newInstance(throttleMap.toMap, acumeContext).asInstanceOf[QueryPoolPolicy]
  val queryPoolSchedulerPolicy: QueryPoolPolicy = Class.forName(ConfConstants.queryPoolSchedPolicyClass).getConstructors()(0).newInstance(acumeContext).asInstanceOf[QueryPoolPolicy]
  var queryPoolPolicy: QueryPoolPolicy = null

  /**
   * Takes QueryRequest i.e. Rubix query and return aggregate Response.
   */
  def servAggregate(queryRequest: QueryRequest, property: HashMap[String, Any] = null): AggregateResponse = {
    servRequest(queryRequest.toSql(""), property).asInstanceOf[AggregateResponse]
  }

  /**
   * Takes QueryRequest i.e. Rubix query and return timeseries Response.
   */
  def servTimeseries(queryRequest: QueryRequest, property: HashMap[String, Any] = null): TimeseriesResponse = {
    servRequest(queryRequest.toSql("ts,"), property).asInstanceOf[TimeseriesResponse]
  }

  def servSearchRequest(queryRequest: SearchRequest): SearchResponse = {
    servSearchRequest(queryRequest.toSql)
  }

  def servSearchRequest(sql: String): SearchResponse = {
    val response = execute(sql)
    val responseRdd = response.rowRDD
    val schema = response.schemaRDD.schema
    val fields = schema.fieldNames
    val rows = responseRdd.collect
    val acumeSchema: QueryBuilderSchema = queryBuilderService.get(0).getQueryBuilderSchema
    val dimsNames = new ArrayBuffer[String]()
    for (field <- fields) {
      dimsNames += field
    }
    new SearchResponse(dimsNames, rows.map(x => asJavaList(x.map(y => y))).toList)
  }

  private def setSparkJobLocalProperties() {
    for ((key, value) <- acumeContext.ac.threadLocal.get()) {
      var propValue: String = if (value != null) value.toString else null
      acumeContext.ac.cacheSqlContext.sparkContext.setLocalProperty(key, propValue)
    }
    AcumeCacheContextTrait.setSparkSqlShufflePartitions(acumeContext.ac.cacheSqlContext.getConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, "200"))
  }
  
  private def getSparkJobLocalProperties() = {
    if(acumeContext.ac.threadLocal.get() == null) {
      acumeContext.ac.threadLocal.set(HashMap[String, Any]())
    }
    acumeContext.ac.threadLocal.get()
  }

  private def unsetSparkJobLocalProperties() {
    for ((key, value) <- acumeContext.ac.threadLocal.get()) {
      acumeContext.ac.cacheSqlContext.sparkContext.setLocalProperty(key, null)
    }
    acumeContext.ac.cacheSqlContext.setConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, AcumeCacheContextTrait.getSparkSqlShufflePartitions)
    try{
    	AcumeCacheContextTrait.unsetAll(acumeContext.ac)      
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

  def checkJobLevelProperties(requests: java.util.ArrayList[_ <: Any], requestDataType: RequestDataType.RequestDataType): (List[(String, HashMap[String, Any])], List[String]) = {
    this.synchronized {

      var sqlList: java.util.ArrayList[String] = new java.util.ArrayList()
      requests foreach (request => {
        requestDataType match {
          case RequestDataType.Aggregate => sqlList.add(request.asInstanceOf[QueryRequest].toSql(""))
          case RequestDataType.TimeSeries => sqlList.add(request.asInstanceOf[QueryRequest].toSql("ts,"))
          case RequestDataType.SQL => sqlList.add(request.asInstanceOf[String])
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

  def servRequest(sql: String, property: HashMap[String, Any] = null): Any = {

    val jobGroupId = Thread.currentThread().getName() + "-" + Thread.currentThread().getId() + "-" + counter.getAndIncrement
    try {
      if (acumeContext.ac.threadLocal.get() == null) {
        acumeContext.ac.threadLocal.set(new HashMap[String, Any]())
      }
      val isSchedulerQuery = queryBuilderService.get(0).isSchedulerQuery(sql)
      val jobDescription = getJobDescription(isSchedulerQuery, Thread.currentThread().getName() + Thread.currentThread().getId())
      logger.info(jobDescription)
      
      def runWithTimeout[T](f: => (AcumeCacheResponse, Array[Row])): (AcumeCacheResponse, Array[Row]) = {
        lazy val fut = future { f }
        Await.result(fut, DurationInt(acumeContext.acumeConf.getInt(ConfConstants.queryTimeOut, 30)) second)
      }
      def run(sql: String, jobGroupId: String, jobDescription: String, conf: AcumeConf, localProperties: HashMap[String, Any]) = {

        getSparkJobLocalProperties ++= localProperties
        setSparkJobLocalProperties
        try {
          AcumeConf.setConf(conf)
          acumeContext.sc.setJobGroup(jobGroupId, jobDescription, false)
          val cacheResponse = execute(sql)
          val responseRdd = cacheResponse.rowRDD
          (cacheResponse, responseRdd.collect)
        } finally {
          unsetSparkJobLocalProperties
        }
      }
      
      val localProperties = if (property == null) getSparkJobLocalProperties else property
      val (cacheResponse, rows) = run(sql, jobGroupId, jobDescription, acumeContext.acumeConf, localProperties)
      
      val fields = queryBuilderService.get(0).getQuerySchema(sql, cacheResponse.schemaRDD.schema.fieldNames) //schemaRdd.schemaschema.fieldNames
      
      val acumeSchema: QueryBuilderSchema = queryBuilderService.get(0).getQueryBuilderSchema
      val dimsNames = new ArrayBuffer[String]()
      val measuresNames = new ArrayBuffer[String]()
      var j = 0
      var isTimeseries = false
      var tsIndex = 0
      for (field <- fields) {
        if (field.equalsIgnoreCase("ts")) {
          isTimeseries = true
          tsIndex = j
        } else if (queryBuilderService.get(0).isFieldDimension(field)) {
          dimsNames += field
        } else {
          measuresNames += field
        }
        j += 1
      }
      if (isTimeseries) {
        val sortedRows = rows.sortBy(row => row(tsIndex).toString)
        val timestamps = cacheResponse.metadata.timestamps
        val timestampsToIndexMap = new scala.collection.mutable.HashMap[Long, Int]()
        var index = -1
        timestamps.foreach(x => { index += 1; timestampsToIndexMap += (x -> index) })
        val rowToMeasureMap = new scala.collection.mutable.HashMap[ArrayBuffer[Any], ArrayBuffer[ArrayBuffer[Any]]]
        for (row <- rows) {
          val dims = new ArrayBuffer[Any]()
          val measures = new ArrayBuffer[Any]()
          var i = 0
          var dimIndex, measureIndex = 0
          var timestamp = 0L
          for (field <- fields) {
            if (field.equalsIgnoreCase("ts")) {
              timestamp = java.lang.Long.valueOf(row(i).toString)
            } else if (queryBuilderService.get(0).isFieldDimension(field)) {
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
          for (field <- fields) {
            if (queryBuilderService.get(0).isFieldDimension(field)) {
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
        new AggregateResponse(list, dimsNames, measuresNames, cacheResponse.metadata.totalRecords.toInt)
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

  def execute(sql: String): AcumeCacheResponse = {

    //val modifiedSql: String = queryBuilderService.get(0).buildQuery(sql)
    var isFirst: Boolean = true
    val modifiedSql: String = queryBuilderService.foldLeft("") { (result, current) =>

      if (isFirst) {
        isFirst = false
        current.buildQuery(sql)
      } else {
        current.buildQuery(result)
      }
    }

    if (!modifiedSql.equals("")) {
      if (!queryBuilderService.iterator.next.isSchedulerQuery(sql)) {
        logger.info(modifiedSql)
        val resp = acumeContext.ac.acql(modifiedSql)
        if (!queryBuilderService.iterator.next.isTimeSeriesQuery(modifiedSql) && !acumeContext.acumeConf.getDisableTotalForAggregateQueries) {
          resp.metadata.totalRecords = acumeContext.ac.acql(queryBuilderService.iterator.next.getTotalCountSqlQuery(modifiedSql)).schemaRDD.first.getLong(0)
        }
        resp
      } else {
        acumeContext.ac.acql(queryBuilderService.iterator.next.getTotalCountSqlQuery(modifiedSql))
      }
    } else
      throw new RuntimeException(s"Invalid Modified Query")

  }
}