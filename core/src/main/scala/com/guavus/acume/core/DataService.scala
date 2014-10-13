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

  def servRequest(sql: String): Any = {

    val schemaRdd = execute(sql)
    val schema = schemaRdd.schema
    val fields = schema.fieldNames
    val rows = schemaRdd.collect
    val acumeSchema: QueryBuilderSchema = null
    val dimsNames = new ArrayBuffer[String]()
    val measuresNames = new ArrayBuffer[String]()
    var j = 0
    var isTimeseries = false
    for (field <- fields) {
      if (field.equalsIgnoreCase("ts")) {
        isTimeseries = true
      } else if (acumeSchema.isDimension(field)) {
        dimsNames += field
      } else {
        measuresNames += field
      }
      j += 1
    }
    if (isTimeseries) {
      //      val list = new ArrayBuffer[TimeseriesResultSet](rows.size)
      //      for (row <- rowArray) {
      //        val dims = new ArrayBuffer[Any]()
      //        val measures = new ArrayBuffer[Any]()
      //
      //        var i = 0
      //        for (field <- fields) {
      //          if (acumeSchema.isDimension(field)) {
      //            dims += row(i).toString
      //          } else {
      //            measures += row(i)
      //          }
      //          i += 1
      //        }
      //        list += new TimeseriesResultSet(dims, measures)
      //      }
      //      new AggregateResponse(list, dimsNames, measuresNames, rows.size)
      //      //aggregate query

      //ts query
      null
    } else {
      val list = new ArrayBuffer[AggregateResultSet](rows.size)
      for (row <- rows) {
        val dims = new ArrayBuffer[Any]()
        val measures = new ArrayBuffer[Any]()

        var i = 0
        var dimIndex, measureIndex = 0
        for (field <- fields) {
          if (acumeSchema.isDimension(field)) {
            if(row(i) != null)
            	dims += row(i).toString
            else
              dims += queryBuilderService.getQbSchema.getDefaultValueForField(dimsNames(dimIndex))
              dimIndex+=1
          } else {
            if(row(i) != null)
            	measures += row(i)
            else
              measures += queryBuilderService.getQbSchema.getDefaultValueForField(measuresNames(measureIndex))
              measureIndex+=1
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