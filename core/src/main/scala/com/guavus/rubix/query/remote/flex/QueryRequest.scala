package com.guavus.rubix.query.remote.flex

import java.io.Serializable
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement
import javax.xml.bind.annotation.XmlTransient
import com.google.gson.Gson
import QueryRequest._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import scala.collection.JavaConversions._
import java.util.ArrayList
import com.google.common.collect.Lists
import java.util.Arrays
import netreflex.messages.QueryRequest

object QueryRequest {

  def main(args: Array[String]) {
    val filterDatas = new ArrayList[FilterData]()
    val filterData = new FilterData()
    val singleFilters = new ArrayList[SingleFilter]()
    var singleFilter = new SingleFilter()
    singleFilter.setDimension("a")
    singleFilter.setValue("2")
    singleFilter.setCondition("EQUAL")
    singleFilters.add(singleFilter)
    singleFilter = new SingleFilter()
    singleFilter.setDimension("b")
    singleFilter.setValue("3")
    singleFilter.setCondition("EQUAL")
    singleFilters.add(singleFilter)
    val gson = new Gson()	
    filterData.setFilters(singleFilters)
    filterDatas.add(filterData)
    val data = new MeasureFilterData()
    val multiFilters = new ArrayList[MultiFilter]()
    for (i <- 0 until 2) {
      val innerFilters = new MultiFilter()
      val filter = new ArrayList[MeasureSingleFilter]()
      for (j <- 0 until 3) {
        filter.add(new MeasureSingleFilter("GREATER_THAN", Array(j.toDouble)))
      }
      innerFilters.setMeasure(String.valueOf(i))
      innerFilters.setSingleFilters(filter)
      println(gson.toJson(innerFilters))
      multiFilters.add(innerFilters)
    }
    data.setFilters(multiFilters)
    println(gson.toJson(data))
    val queryRequest = new QueryRequest()
    queryRequest.setResponseDimensions(Lists.newArrayList("*"))
    queryRequest.setResponseMeasures(new ArrayList[String](0))
    queryRequest.setFilterData(null)
    queryRequest.setMeasureFilters(Lists.newArrayList(data))
    queryRequest.setResponseFilters(Lists.newArrayList())
    queryRequest.setParamMap(Lists.newArrayList())
    queryRequest.setLength(10)
    queryRequest.setStartTime(10)
    queryRequest.setEndTime(11)
    queryRequest.setTimeGranularity(5)
//    println(gson.toJson(queryRequest))
//    println(gson.fromJson(gson.toJson(queryRequest), classOf[QueryRequest]))
//    println(gson.fromJson("{'responseMeasures':['CompUpBytes','CompDownBytes'],'responseDimensions':['Attribute'], 'sortProperty':'CompUpBytes','filters':[],'cubeContextDimensions':[],'sortDirection':'DSC','maxResults':-1,'maxResultOffset':0,'length':50,'offset':0,'startTime':1349917200,'endTime':1349935200,'timeGranularity':0,'filters':[[{'name':'Agony','value':'6'}]],'measureFilters':[{'filters':[{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'CompUpBytes'},{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'CompDownBytes'}]}]}", classOf[QueryRequest]))
//    println(gson.fromJson("{'filters':[{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'0'},{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'1'}]}", classOf[MeasureFilterData]))
    println(queryRequest.toSql(""))
  }
}

@XmlRootElement
class QueryRequest extends Serializable {

  @BeanProperty
  var subQuery: QueryRequest = _

  @BeanProperty
  var responseMeasures: ArrayList[String] = _

  @BeanProperty
  var responseDimensions: ArrayList[String] = _

  @BeanProperty
  var cubeContextDimensions: ArrayList[String] = _

  @Deprecated
  @BeanProperty
  var filterMap: ArrayList[NameValue] = _

  @Deprecated
  @BeanProperty
  var filters: ArrayList[ArrayList[NameValue]] = _

  @BeanProperty
  var filterData: ArrayList[FilterData] = _

  @BeanProperty
  var sortProperty: String = _

  @BeanProperty
  var sortDirection: String = _

  @BeanProperty
  var paramMap: ArrayList[NameValue] = _

  @BeanProperty
  var searchRequest: SearchRequest = _

  @BeanProperty
  var maxResults: Int = _

  @BeanProperty
  var maxResultOffset: Int = _

  @BeanProperty
  var length: Int = _

  @BeanProperty
  var offset: Int = _

  @BeanProperty
  var startTime: Long = _

  @BeanProperty
  var endTime: Long = _

  @BeanProperty
  var timeGranularity: Long = _

  @BeanProperty
  var responseFilters: ArrayList[ResponseFilter] = _

  @BeanProperty
  var measureFilters: ArrayList[MeasureFilterData] = _

  @BeanProperty
  var binSource: String = _

  var isOptimizable: Boolean = true

  def setOptimizable(isOptimizable: Boolean) {
    this.isOptimizable = isOptimizable
  }

  override def toString(): String = QueryJsonUtil.queryRequestToJson(this)

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((binSource == null)) 0 else binSource.hashCode)
    result = prime * result + (if ((cubeContextDimensions == null)) 0 else cubeContextDimensions.hashCode)
    result = prime * result + (endTime ^ (endTime >>> 32)).toInt
    result = prime * result + (if ((filterData == null)) 0 else filterData.hashCode)
    result = prime * result + (if ((filterMap == null)) 0 else filterMap.hashCode)
    result = prime * result + (if ((filters == null)) 0 else filters.hashCode)
    result = prime * result + length
    result = prime * result + maxResultOffset
    result = prime * result + maxResults
    result = prime * result + (if ((measureFilters == null)) 0 else measureFilters.hashCode)
    result = prime * result + offset
    result = prime * result + (if (isOptimizable) 1 else 0)
    result = prime * result + (if ((paramMap == null)) 0 else paramMap.hashCode)
    result = prime * result + (if ((responseDimensions == null)) 0 else responseDimensions.hashCode)
    result = prime * result + (if ((responseFilters == null)) 0 else responseFilters.hashCode)
    result = prime * result + (if ((responseMeasures == null)) 0 else responseMeasures.hashCode)
    result = prime * result + (if ((searchRequest == null)) 0 else searchRequest.hashCode)
    result = prime * result + (if ((sortDirection == null)) 0 else sortDirection.hashCode)
    result = prime * result + (if ((sortProperty == null)) 0 else sortProperty.hashCode)
    result = prime * result + (startTime ^ (startTime >>> 32)).toInt
    result = prime * result + (if ((subQuery == null)) 0 else subQuery.hashCode)
    result = prime * result + (timeGranularity ^ (timeGranularity >>> 32)).toInt
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[QueryRequest]
    if (binSource == null) {
      if (other.binSource != null) return false
    } else if (binSource != other.binSource) return false
    if (cubeContextDimensions == null) {
      if (other.cubeContextDimensions != null) return false
    } else if (cubeContextDimensions != other.cubeContextDimensions) return false
    if (endTime != other.endTime) return false
    if (filterData == null) {
      if (other.filterData != null) return false
    } else if (filterData != other.filterData) return false
    if (filterMap == null) {
      if (other.filterMap != null) return false
    } else if (filterMap != other.filterMap) return false
    if (filters == null) {
      if (other.filters != null) return false
    } else if (filters != other.filters) return false
    if (length != other.length) return false
    if (isOptimizable != other.isOptimizable) return false
    if (maxResultOffset != other.maxResultOffset) return false
    if (maxResults != other.maxResults) return false
    if (measureFilters == null) {
      if (other.measureFilters != null) return false
    } else if (measureFilters != other.measureFilters) return false
    if (offset != other.offset) return false
    if (paramMap == null) {
      if (other.paramMap != null) return false
    } else if (paramMap != other.paramMap) return false
    if (responseDimensions == null) {
      if (other.responseDimensions != null) return false
    } else if (responseDimensions != other.responseDimensions) return false
    if (responseFilters == null) {
      if (other.responseFilters != null) return false
    } else if (responseFilters != other.responseFilters) return false
    if (responseMeasures == null) {
      if (other.responseMeasures != null) return false
    } else if (responseMeasures != other.responseMeasures) return false
    if (searchRequest == null) {
      if (other.searchRequest != null) return false
    } else if (searchRequest != other.searchRequest) return false
    if (sortDirection == null) {
      if (other.sortDirection != null) return false
    } else if (sortDirection != other.sortDirection) return false
    if (sortProperty == null) {
      if (other.sortProperty != null) return false
    } else if (sortProperty != other.sortProperty) return false
    if (startTime != other.startTime) return false
    if (subQuery == null) {
      if (other.subQuery != null) return false
    } else if (subQuery != other.subQuery) return false
    if (timeGranularity != other.timeGranularity) return false
    true
  }
  
  def toSql(ts1: String): String = {
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
      checkflag((paramMap == null || paramMap.size == 0), "", calculateParams(paramMap)) + 
      checkflag((responseFilters == null || responseFilters.size == 0), "", rf) + 
      checkflag(binSource == null, "", " binSource " + " = '" + binSource + "' ") + 
      checkflag(timeGranularity < 0, "", " timeGranularity = " + timeGranularity) + 
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

  private def calculateParams(params: Traversable[NameValue]): String = {
    if(paramMap == null)
      return "";
    var sql = " "
    for (nameValue <- params) {
      sql += nameValue.toSql() + " AND "
    }
    if (!sql.equalsIgnoreCase(" ")) sql = sql.substring(0, sql.length - 4)
    sql += " "
    sql
  }

  private def calculateResponseFilters(): String = {
    var sql = " "
    for (nameValue <- responseFilters) {
      sql += nameValue.toSql() + " AND "
    }
    if(!sql.equals(" ")) 
      sql = sql.substring(0, sql.length - 4)
    sql += " "
    sql
  }

  private def calculateDimensionFilterData(): String = {
    
    if(filterData == null || filterData.isEmpty)
      return "";
    var sql = " ("
    for (filter <- filterData) {
      sql += "("
      sql += filter.toSql
      sql += ") or "
    }
    sql = sql.substring(0, sql.length - 3)
    sql += ")"
    sql
  }
  
  private def calculateDimensionFilters(): String = {

    if (filters == null || filters.size == 0)
      return "";
    if (filters.size == 1) {
      return calculateParams(filters.get(0))
    }
    var sql = " ("
    for (filter <- filters) {
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