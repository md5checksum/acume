package com.guavus.acume.core

import com.guavus.acume.rest.beans.TimeseriesResponse
import com.guavus.acume.rest.beans.AggregateResponse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import com.guavus.acume.rest.beans.QueryRequest
import com.guavus.qb.services.QueryBuilderService
import com.guavus.querybuilder.cube.schema.QueryBuilderSchema
import com.guavus.qb.conf.QBConf
import org.apache.spark.sql.SchemaRDD
import scala.collection.mutable.ArrayBuffer
import com.guavus.acume.rest.beans.AggregateResultSet
import com.guavus.acume.rest.beans.TimeseriesResultSet
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.guavus.acume.cache.workflow.AcumeCacheContext
import scala.collection.mutable.HashMap
import java.util.Arrays
import com.guavus.acume.rest.beans.SearchResponse
import com.guavus.acume.rest.beans.SearchResponse
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.guavus.acume.rest.beans.SearchRequest

/**
 * This class interacts with query builder and Olap cache.
 */
class DataService(queryBuilderService: QueryBuilderService, acumeContext: AcumeContext) {

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
    val schemaRdd = execute(sql)
    val schema = schemaRdd.schema
    val fields = schema.fieldNames
    val rows = schemaRdd.collect
    val acumeSchema: QueryBuilderSchema = queryBuilderService.getQbSchema
    val dimsNames = new ArrayBuffer[String]()
    for (field <- fields) {
        dimsNames += field
    }
    new SearchResponse(dimsNames,rows.map(x=> asJavaList(x.map(y=>y))).toList)
  }
  
  def servRequest(sql: String): Any = {

    val schemaRdd = execute(sql)
    val schema = schemaRdd.schema
    val fields = schema.fieldNames
    val rows = schemaRdd.collect
    val acumeSchema: QueryBuilderSchema = queryBuilderService.getQbSchema
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
      val timestamps = new ArrayBuffer[Long]()
      val timestampsToIndexMap = new HashMap[Long, Int]()
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
              dims += row(i).toString
            else
              dims += queryBuilderService.getDefaultValueForField(dimsNames(dimIndex))
            dimIndex += 1
          } else {
            if (row(i) != null)
              measures += row(i)
            else
              measures += queryBuilderService.getDefaultValueForField(measuresNames(measureIndex))
            measureIndex += 1
          }
          i += 1
        }

        def initializeArray(): ArrayBuffer[ArrayBuffer[Any]] = {
          val measureArray = new Array[ArrayBuffer[Any]](measuresNames.size)
          i = 0
          while (i < measuresNames.size) {
            measureArray(i) = { val array = new Array[Object](timestamps.size); Arrays.fill(array, (new Object).asInstanceOf[Any]); new ArrayBuffer[Any]() ++= (array) }
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
        new TimeseriesResultSet(rowToMap._1, rowToMap._2.map(_.asJava))
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
              dims += row(i).toString
            else
              dims += queryBuilderService.getDefaultValueForField(dimsNames(dimIndex))
            dimIndex += 1
          } else {
            if (row(i) != null)
              measures += row(i)
            else
              measures += queryBuilderService.getDefaultValueForField(measuresNames(measureIndex))
            measureIndex += 1
          }
          i += 1
        }
        list += new AggregateResultSet(dims, measures)
      }
      new AggregateResponse(list, dimsNames, measuresNames, rows.size)
    }
  }

  def execute(sql: String): SchemaRDD = {
    val modifiedSql = queryBuilderService.buildQuery(sql)
    acumeContext.ac.acql(modifiedSql)
  }
}