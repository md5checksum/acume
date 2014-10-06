package com.guavus.acume.core

import com.guavus.acume.rest.beans.TimeseriesResponse
import com.guavus.acume.rest.beans.AggregateResponse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import com.guavus.acume.rest.beans.QueryRequest
import com.guavus.qb.services.QueryBuilderService

/**
 * This class interacts with query builder and Olap cache.
 */
class DataService {

  def servAggregate(queryRequest : QueryRequest) : AggregateResponse = {
    null
  } 
  
  
  def servTimeseries(queryRequest : QueryRequest) : TimeseriesResponse = {
    null
  }
  
  def execute(sql : String) : RDD[Row] = {
    new QueryBuilderService()
    val schemaRdd = AcumeContext.acumeContext.get.ac.acql(sql)
    val schema = schemaRdd.schema
    if(schema.fieldNames.count(x =>x.equalsIgnoreCase("ts")) > 0 {
      //ts query
    } else {
      //aggregate query
    }
  }
}