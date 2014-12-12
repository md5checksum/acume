package com.guavus.acume.core

import com.guavus.rubix.query.remote.flex.TimeseriesResponse
import com.guavus.rubix.query.remote.flex.AggregateResponse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import com.guavus.rubix.query.remote.flex.QueryRequest
import com.guavus.querybuilder.cube.schema.QueryBuilderSchema
import com.guavus.qb.conf.QBConf
import org.apache.spark.sql.SchemaRDD
import scala.collection.mutable.ArrayBuffer
import com.guavus.rubix.query.remote.flex.AggregateResultSet
import com.guavus.rubix.query.remote.flex.TimeseriesResultSet
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.guavus.acume.cache.workflow.AcumeCacheContext
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

/**
 * This class interacts with query builder and Olap cache.
 */
class DataService(queryBuilderService: Seq[IQueryBuilderService], acumeContext: AcumeContextTrait) {

  /**
   * Takes QueryRequest i.e. Rubix query and return aggregate Response.
   */
  def servAggregate(queryRequest: QueryRequest): AggregateResponse = {
    servRequest(queryRequest.toSql("")).asInstanceOf[AggregateResponse]
  }

  /**
   * Takes QueryRequest i.e. Rubix query and return timeseries Response.
   */
  def servTimeseries(queryRequest: QueryRequest): TimeseriesResponse = {
    servRequest(queryRequest.toSql("ts,")).asInstanceOf[TimeseriesResponse]
  }
  
  def servSearchRequest(queryRequest: SearchRequest): SearchResponse = {
    servSearchRequest(queryRequest.toSql)
  }

  def servSearchRequest(sql : String) : SearchResponse = {
    val schemaRdd = execute(sql).schemaRDD
    val schema = schemaRdd.schema
    val fields = schema.fieldNames
    val rows = schemaRdd.collect
    val acumeSchema: QueryBuilderSchema = queryBuilderService.get(0).getQueryBuilderSchema
    val dimsNames = new ArrayBuffer[String]()
    for (field <- fields) {
        dimsNames += field
    }
    new SearchResponse(dimsNames,rows.map(x=> asJavaList(x.map(y=>y))).toList)
  }
  
  def servRequest(sql: String): Any = {

    val cacheResponse = execute(sql)
    val schemaRdd = cacheResponse.schemaRDD
    val schema = schemaRdd.schema
    val fields = schema.fieldNames
    val rows = schemaRdd.collect
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
      } else if (acumeSchema.isDimension(field)) {
        dimsNames += field
      } else {
        measuresNames += field
      }
      j += 1
    }
    if (isTimeseries) {
      val sortedRows = rows.sortBy(row => row.getLong(tsIndex))
      val timestamps = cacheResponse.metadata.timestamps
      val timestampsToIndexMap = new scala.collection.mutable.HashMap[Long, Int]()
      var index  = -1
      timestamps.foreach(x=> {index+=1; timestampsToIndexMap += (x -> index)})
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
          } else if (acumeSchema.isDimension(field)) {
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
      for(rowToMap <- rowToMeasureMap) {
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
          if (acumeSchema.isDimension(field)) {
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
      new AggregateResponse(list, dimsNames, measuresNames, rows.size)
    }
  }

  def execute(sql: String): AcumeCacheResponse = {
  
    //val modifiedSql: String = queryBuilderService.get(0).buildQuery(sql)
     var isFirst: Boolean = true
     val modifiedSql : String = queryBuilderService.foldLeft("") { (result, current) => 
      
       if(isFirst){
         try{
         isFirst = false
         current.buildQuery(sql)
         } catch {
           case ex: Exception => ex.printStackTrace()
           null
         }
       }
       else{
         try{
         current.buildQuery(result)
         }
         catch {
           case ex: Exception => ex.printStackTrace()
           null
         }
       }
     }
   
    if(!modifiedSql.equals("")) {
    	print(modifiedSql)
    	acumeContext.ac.acql(modifiedSql)
    }
    else
      throw new RuntimeException(s"Invalid Modified Query")
      
  }
}