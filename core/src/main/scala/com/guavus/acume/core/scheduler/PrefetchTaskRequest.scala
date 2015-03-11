package com.guavus.acume.core.scheduler

import com.guavus.acume.workflow.RequestDataType._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.acume.workflow.RequestDataType
import com.guavus.rubix.query.remote.flex.QueryRequest
import java.util.ArrayList
import com.guavus.rubix.query.remote.flex.SortDirection
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.guavus.rubix.query.remote.flex.NameValue

class PrefetchTaskRequest {

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
  
  def toSql(ts1: String): String = {
    val columns = new ArrayList[String]()
    if(ts1 != null && !ts1.isEmpty()) {
    	columns.add(ts1)
    }
    columns.addAll(queryRequest.responseDimensions)
    columns.addAll(queryRequest.responseMeasures)
    if (queryRequest.cubeContextDimensions != null) {
      for (dimension <- queryRequest.cubeContextDimensions) {
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
      checkflag(queryRequest.subQuery == null, "", " (placeholder) in (" + (if(queryRequest.subQuery != null)queryRequest.subQuery.toSql("")) + ") ") + 
      checkflag(queryRequest.startTime < 0, "", " startTime = " + queryRequest.startTime) + 
      checkflag(queryRequest.endTime < 0 , "" , " endTime = " + queryRequest.endTime) + getfilterstring + 
      checkflag((queryRequest.paramMap == null || queryRequest.paramMap.size == 0), "", calculateParams(queryRequest.getParamMap)) + 
      checkflag((queryRequest.responseFilters == null || queryRequest.responseFilters.size == 0), "", rf) + 
      checkflag(queryRequest.binSource == null, "", " binSource " + " = '" + queryRequest.binSource + "' ") + 
      checkflag(queryRequest.timeGranularity < 0, "", " timeGranularity = " + queryRequest.timeGranularity) +
      checkflag(false, "", " acumeQueryType = " + "'scheduler'") +
      checkflag(queryRequest.searchRequest == null, "", " (placeholder) in (" + (if(queryRequest.searchRequest!= null) queryRequest.searchRequest.toSql()) + ") ")

    val wherestring =
      (if (!whereClause.equals("")) " where " + whereClause else "") +
        (if (queryRequest.sortProperty == null || queryRequest.sortProperty.isEmpty) "" else " order by " + queryRequest.sortProperty + " ") +
        (if (queryRequest.sortDirection == null) "" else if (queryRequest.sortDirection == SortDirection.ASC.toString) " asc" else " desc") +
        (if (queryRequest.length == -1) "" else "  limit " + queryRequest.length) +
        (if (queryRequest.offset == 0) "" else " offset " + queryRequest.offset + " ")
      
    var abs = "select " + columns.toString.substring(1, columns.toString.length - 1) + " from " + queryRequest.fromItem + wherestring
    abs
  }
  
   protected def calculateParams(params: Traversable[NameValue]): String = {
    if(queryRequest.paramMap == null)
      return "";
    var sql = " "
    for (nameValue <- params) {
      sql += nameValue.toSql() + " AND "
    }
    if (!sql.equalsIgnoreCase(" ")) sql = sql.substring(0, sql.length - 4)
    sql += " "
    sql
  }

  protected def calculateResponseFilters(): String = {
    if(queryRequest.responseFilters == null) {
      return ""
    }
    var sql = " "
      if(queryRequest.responseFilters == null) {
        return sql
      }
    for (nameValue <- queryRequest.responseFilters) {
      sql += nameValue.toSql() + " AND "
    }
    if(!sql.equals(" ")) 
      sql = sql.substring(0, sql.length - 4)
    sql += " "
    sql
  }

  protected def calculateDimensionFilterData(): String = {
    
    if(queryRequest.filterData == null || queryRequest.filterData.isEmpty)
      return "";
    var sql = " ("
    for (filter <- queryRequest.filterData) {
      sql += "("
      sql += filter.toSql
      sql += ") or "
    }
    sql = sql.substring(0, sql.length - 3)
    sql += ")"
    sql
  }
  
  protected def calculateDimensionFilters(): String = {

    if (queryRequest.filters == null || queryRequest.filters.size == 0)
      return "";
    if (queryRequest.filters.size == 1) {
      return calculateParams(queryRequest.filters.get(0))
    }
    var sql = " ("
    for (filter <- queryRequest.filters) {
      sql += "("
      for (nameValue <- filter) {
        sql += nameValue.toSql() + " AND "
      }
      sql = sql.substring(0, sql.length - 4)
      sql += ") or "
    }
    sql = sql.substring(0, sql.length - 3)
    sql += ")"
    sql
  }

}