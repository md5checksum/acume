package com.guavus.acume.core.scheduler

import com.guavus.acume.workflow.RequestDataType._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.acume.workflow.RequestDataType
import com.guavus.rubix.query.remote.flex.QueryRequest
import java.util.ArrayList
import com.guavus.rubix.query.remote.flex.SortDirection
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class PrefetchTaskRequest extends QueryRequest {

  @BeanProperty
  var queryRequest: QueryRequest = _
  
//  @BeanProperty
//  var endTime: Int = queryRequest.getStartTime
//  
//  @BeanProperty
//  var startTime: Int = queryRequest.getEndTime

  @BeanProperty
  var requestDataType: RequestDataType = _

  var cashIdentifier: String = _

  override def toString(): String = {
    val builder = new StringBuilder()
    builder.append("PrefetchTaskRequest [queryRequest=")
    builder.append(queryRequest)
    builder.append(", requestDataType=")
    builder.append(requestDataType)
    builder.append("]")
    builder.toString
  }
  
  override def toSql(ts1: String): String = {
    val columns = new ArrayList[String]()
    columns.addAll(responseDimensions)
    columns.addAll(responseMeasures)
    if (cubeContextDimensions != null) {
      for (dimension <- cubeContextDimensions) {
        columns.add("c." + dimension)
      }
    }
    
    var andFlag = false
    
    def and = if(!andFlag) "" else " and " 	
    def checkflag(cond: Boolean, True: String, False: String) = {
      val str =
        if (!cond) {
          if(!False.isEmpty) and + False
          else ""
        } else {
          
          if(!True.isEmpty) and + True
          else ""
        }
      if(!str.isEmpty)
        andFlag = true
      str
    }
    
    
    val df = calculateDimensionFilterData
    val _$df = calculateDimensionFilters
    val rf = calculateResponseFilters
    
    def getfilterstring = {
      val filterdata = checkflag((df.equalsIgnoreCase("")), "", df)
      val filters = 
        if(filterdata.isEmpty)
          checkflag((_$df.equalsIgnoreCase("")), "", _$df)
        else ""
      filterdata + filters
    }
    
    val whereClause = 
      checkflag(subQuery == null, "", " (placeholder) in (" + (if(subQuery != null)subQuery.toSql("")) + ") ") + 
      checkflag(startTime < 0, "", " startTime = " + startTime) + 
      checkflag(endTime < 0 , "" , " endTime = " + endTime) + getfilterstring + 
      checkflag((paramMap == null || paramMap.size == 0), "", calculateParams(queryRequest.getParamMap)) + 
      checkflag((responseFilters == null || responseFilters.size == 0), "", rf) + 
      checkflag(binSource == null, "", " binSource " + " = '" + binSource + "' ") + 
      checkflag(timeGranularity < 0, "", " timeGranularity = " + timeGranularity) +
      checkflag(false, "", " acumeQueryType = " + "scheduler") +
      checkflag(searchRequest == null, "", " (placeholder) in (" + (if(searchRequest!= null) searchRequest.toSql()) + ") ")

    val wherestring =
      (if (!whereClause.equals("")) " where " + whereClause else "") +
        (if (sortProperty == null || sortProperty.isEmpty) "" else " order by " + sortProperty + " ") +
        (if (sortDirection == null) "" else if (sortDirection == SortDirection.ASC.toString) " asc" else " desc") +
        (if (length == -1) "" else "  limit " + length) +
        (if (offset == 0) "" else " offset " + offset + " ")
      
    var abs = "select " + ts1 + columns.toString.substring(1, columns.toString.length - 1) + " from global " +wherestring
    abs
  }

}