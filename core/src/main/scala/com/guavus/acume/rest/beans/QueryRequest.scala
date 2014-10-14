package com.guavus.acume.rest.beans

import java.io.Serializable
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
    queryRequest.setResponseDimensions(new ArrayList[String](0))
    queryRequest.setResponseMeasures(new ArrayList[String](0))
    queryRequest.setFilterData(filterDatas)
    queryRequest.setMeasureFilters(Lists.newArrayList(Arrays.asList(new MeasureFilterData{data})))
    println(gson.toJson(queryRequest))
    println(gson.fromJson(gson.toJson(queryRequest), classOf[QueryRequest]))
    println(gson.fromJson("{'responseMeasures':['CompUpBytes','CompDownBytes'],'responseDimensions':['Attribute'], 'sortProperty':'CompUpBytes','filters':[],'cubeContextDimensions':[],'sortDirection':'DSC','maxResults':-1,'maxResultOffset':0,'length':50,'offset':0,'startTime':1349917200,'endTime':1349935200,'timeGranularity':0,'filters':[[{'name':'Agony','value':'6'}]],'measureFilters':[{'filters':[{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'CompUpBytes'},{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'CompDownBytes'}]}]}", classOf[QueryRequest]))
    println(gson.fromJson("{'filters':[{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'0'},{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'1'}]}", classOf[MeasureFilterData]))
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
  var filterData: Traversable[FilterData] = _

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
    val abs = "select " + ts1 + columns.toString.substring(1, columns.toString.length - 1) + " from global where " + (if (subQuery == null) "" else " (placeholder) in (" + subQuery.toSql("") + ") and ") + " startTime = " + startTime + " and endTime = " + endTime + (if ((filters == null || filters.size == 0 || calculateDimensionFilters().equalsIgnoreCase("  "))) "" else " and " + calculateDimensionFilters()) + (if ((paramMap == null || paramMap.size == 0)) "" else " and " + calculateParams(paramMap)) + (if ((responseFilters == null || responseFilters.size == 0)) "" else " and " + calculateResponseFilters()) + (if ((binSource == null)) "" else " and " + " binSource " + " = '" + binSource + "' ") + " and timeGranularity = " + timeGranularity + (if (searchRequest == null) "" else " and (placeholder) in (" + searchRequest.toSql() + ") ") + (if ((sortProperty == null || sortProperty.isEmpty)) " " else " order by " + sortProperty + " " + ((if (sortDirection == SortDirection.ASC.toString) " asc" else " desc"))) + (if ((length == -1)) "" else "  limit " + length) + (if ((offset == 0)) "" else " offset " + offset + " ")
    abs
  }

  private def calculateParams(params: Traversable[NameValue]): String = {
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
    sql = sql.substring(0, sql.length - 4)
    sql += " "
    sql
  }

  private def calculateDimensionFilters(): String = {
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

/*
Original Java:
package com.guavus.acume.rest.beans;

import java.io.Serializable;
import java.util.ArrayArrayList;
import java.util.Collection;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.testng.collections.ArrayLists;

import com.google.gson.Gson;
import com.guavus.rubix.query.SortDirection;
import com.guavus.rubix.query.remote.flex.MeasureFilterData.MultiFilter;
import com.guavus.rubix.search.SearchRequest;

|**
 * This object represents a query criteria for querying streaming data. Details
 * of the member variables
 * <ul>
 * <li>{@link #responseDimensions} - This is the ArrayList of Dimensions which are
 * required in the output. For a usecase prespective .
 * <ul>
 * <li></li>A Grid data request can have dimensions as src_PE_name ,
 * dest_PE_name .
 * </li>
 * <li>A Donut request can have the group by dimension as single response
 * dimension</li>
 * <li>A total request can have no dimensions at all</li>
 *
 * </ul>
 * </li> </li> <li> {@link #responseMeasures} The measures which are required in
 * the response
 * <ul>
 * <li>A request for Grid can have the traffic or volume as measures , similarly
 * for Donut or Grid</li></li> </ul> </li><li>
 * {@link #cubeContextDimensions} The dimensions w.r.t. context of query
 * requested. This can be termed analogous to the dimensions of the cube from
 * which the data is being fetched. From UI prespective this will be depedent on
 * the tab selected. <br/>
 * </li> 
 * 
 * <li> {@link #filterMap} This is a ArrayList of {@link NameValue} containing the
 * Dimension as {@link NameValue#getName()} as name and
 * {@link NameValue#getValue()} as its value. These Dimensions are the exact
 * match selection criteria . The Dimensions sent in this pram should cached
 * types only. A Cached type dimension is the dimension which is stored in
 * cache. This filter works on exact match. For e.g customer_id = 199</li>
 * 
 * <li> {@link #filters} This is a ArrayList, having the ArrayList of {@link NameValue} which  further contain
 * the Dimension as {@link NameValue#getName()} as name and
 * {@link NameValue#getValue()} as its value. These Dimensions are the exact
 * match selection criteria . The Dimensions sent in this pram should cached
 * types only. A Cached type dimension is the dimension which is stored in
 * cache. This filter works on exact match. For e.g customer_id = 199
 * The criterion in the inner ArrayList are ANDed together and all such criterion
 * present in each element of outer ArrayList are ORed to get the final selection criteria
 * 
 * </li>
 * <li> {@link #sortProperty} A dimension on which the results will be
 * sorted
 * <p></li>
 * </ul>
 * <p>
 * <p>
 * 
 * 
 *|
@XmlRootElement
public class QueryRequest implements Serializable {

	|**
	 * Sub query , used to select dimensions
	 *|
	private QueryRequest subQuery ;
	
	|**
	 * Measures names which are required in the response
	 *|
	private ArrayList<String> responseMeasures;

	|**
	 * Dimension Names which are required in the response
	 *|
	private ArrayList<String> responseDimensions;

	|**
	 * The dimensions in which contects the qurey is being made. Typically
	 * derived from a tab
	 *|
	private ArrayList<String> cubeContextDimensions;

	|**
	 * Section criteria , if a selection is made then the specific values of the
	 * Dimension on which the filter is required The key is Dimension name and
	 * value is Dimension value . For example if a PE-PE grid is selected then
	 * the exact value s of the selected PE-PE pair is to be set in this object
	 *|
	@Deprecated
	private ArrayList<NameValue> filterMap;
	
	|**
	 * Selection criteria, the selection criteria is created by ANDing all the NameValue filters
	 * in the inner ArrayList and ORing all the selection criterion formed by each element of the outer ArrayList
	 * For backward compatibility, filterMap is also ORed to the final criteria 
	 * 
	 *|
	@Deprecated
	private ArrayList<ArrayList<NameValue>> filters;
	
	|**
	 * The filtering criteria should be passed in this
	 *|
	private Collection<FilterData> filterData;

	|**
	 * Sorting of resutls on server side, key is name of dimension
	 *|
	private String sortProperty;

	|**
	 * Sort direction , value can be ASC | DSC
	 *|
	private String sortDirection;

	|**
	 * Special params map . As of now this will be used for special params of
	 * series of N-point moving average
	 *|
	private ArrayList<NameValue> paramMap;

    |**
     * Search request.
      *|
    private SearchRequest searchRequest;
	|**
	 * max number of results to be returned in response
	 *|
	private int maxResults;
	
	|**
	 * Offset from where the <code>maxResults</code> are started. Default is 0 
	 *|
	private int maxResultOffset;

	|**
	 * Pagination length
	 *|
	private int length;

	|**
	 * Offset
	 *|
	private int offset;

	|**
	 * Duration start time
	 *|
	private long startTime;

	|**
	 * Duration end time
	 *|
	private long endTime;
	
	|**
	 * time granularity in secs
	 *|
	private long timeGranularity;

	|*
	 * measure filters for supported operations see operator class
	 *|
	private ArrayList<ResponseFilter> responseFilters;
	
	|*
	 * 
	 *|
	private ArrayList<MeasureFilterData> measureFilters;
	
	|**
	 *  Bin class source name SE , DME etc
	 *|
	private String binSource;
	
	|**
	 * Is Query optimizable/supposed to be optimized? 
	 * This parameter is applicable only for sub-queries.
	 *|
	private boolean isOptimizable = true;
	
	|**
	 * @return the subQuery
	 *|
	public QueryRequest getSubQuery() {
		return subQuery;
	}

	|**
	 * @param subQuery the subQuery to set
	 *|
	public void setSubQuery(QueryRequest subQuery) {
		this.subQuery = subQuery;
	}

	|**
	 * @return the responseMeasures
	 *|
	@XmlElement(type=ArrayArrayList.class)
	public ArrayList<String> getResponseMeasures() {
		return responseMeasures;
	}

	|**
	 * @param responseMeasures
	 *            the responseMeasures to set
	 *|
	public void setResponseMeasures(ArrayList<String> responseMeasures) {
		this.responseMeasures = responseMeasures;
	}

	|**
	 * @return the responseDimensions
	 *|
	@XmlElement(type=ArrayArrayList.class)
	public ArrayList<String> getResponseDimensions() {
		return responseDimensions;
	}

	|**
	 * @param responseDimensions
	 *            the responseDimensions to set
	 *|
	public void setResponseDimensions(ArrayList<String> responseDimensions) {
		this.responseDimensions = responseDimensions;
	}

	|**
	 * @return the cubeContextDimensions
	 *|
	@XmlElement(type=ArrayArrayList.class)
	public ArrayList<String> getCubeContextDimensions() {
		return cubeContextDimensions;
	}

	|**
	 * @param cubeContextDimensions
	 *            the cubeContextDimensions to set
	 *|
	public void setCubeContextDimensions(ArrayList<String> cubeContextDimensions) {
		this.cubeContextDimensions = cubeContextDimensions;
	}

	|**
	 * @return the filterMap
	 *|
	@Deprecated
	@XmlElement(type=ArrayArrayList.class)
	public ArrayList<NameValue> getFilterMap() {
		return filterMap;
	}

	|**
	 * @param filterMap
	 *            the filterMap to set
	 *|
	@Deprecated
	public void setFilterMap(ArrayList<NameValue> filterMap) {
		this.filterMap = filterMap;
	}
	
	|**
	 * @return the filters
	 *|
	@Deprecated
	@XmlElement(type=ArrayArrayList.class)
	public ArrayList<ArrayList<NameValue>> getFilters() {
		return filters;
	}

	|**
	 * Use setFilterData() instead
	 * @param filters
	 *            the filters to set
	 * 
	 *|
	@Deprecated
	public void setFilters(ArrayList<ArrayList<NameValue>> filters) {
		this.filters = filters;
	}

	@XmlElement(type=ArrayArrayList.class)
	public Collection<FilterData> getFilterData() {
		return filterData;
	}

	@XmlElement(type=ArrayArrayList.class)
	public void setFilterData(Collection<FilterData> filterData) {
		this.filterData = filterData;
	}

	|**
	 * @return the sortProperty
	 *|
	public String getSortProperty() {
		return sortProperty;
	}

	|**
	 * @param sortProperty
	 *            the sortProperty to set
	 *|
	public void setSortProperty(String sortProperty) {
		this.sortProperty = sortProperty;
	}

	|**
	 * @return the sortDirection
	 *|
	public String getSortDirection() {
		return sortDirection;
	}

	|**
	 * @param sortDirection
	 *            the sortDirection to set
	 *|
	public void setSortDirection(String sortDirection) {
		this.sortDirection = sortDirection;
	}

	|**
	 * @return the paramMap
	 *|
	@XmlElement(type=ArrayArrayList.class)
	public ArrayList<NameValue> getParamMap() {
		return paramMap;
	}

	|**
	 * @param paramMap
	 *            the paramMap to set
	 *|
	public void setParamMap(ArrayList<NameValue> paramMap) {
		this.paramMap = paramMap;
	}

	|**
	 * @return the maxResults
	 *|
	public int getMaxResults() {
		return maxResults;
	}

	|**
	 * @param maxResults
	 *            the maxResults to set
	 *|
	public void setMaxResults(int maxResults) {
		this.maxResults = maxResults;
	}

	|**
	 * @return the length
	 *|
	public int getLength() {
		return length;
	}

	|**
	 * @param length
	 *            the length to set
	 *|
	public void setLength(int length) {
		this.length = length;
	}

	|**
	 * @return the offset
	 *|
	public int getOffset() {
		return offset;
	}

	|**
	 * @param offset
	 *            the offset to set
	 *|
	public void setOffset(int offset) {
		this.offset = offset;
	}

	|**
	 * @return the startTime
	 *|
	public long getStartTime() {
		return startTime;
	}

	|**
	 * @param startTime
	 *            the startTime to set
	 *|
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	|**
	 * @return the endTime
	 *|
	public long getEndTime() {
		return endTime;
	}

	|**
	 * @param endTime
	 *            the endTime to set
	 *|
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	|**
	 * @return the timeGranularity
	 *|
	public long getTimeGranularity() {
		return timeGranularity;
	}

	|**
	 * @param timeGranularity the timeGranularity to set
	 *|
	public void setTimeGranularity(long timeGranularity) {
		this.timeGranularity = timeGranularity;
	}

	|**
	 * @return the maxResultOffset
	 *|
	public int getMaxResultOffset() {
		return maxResultOffset;
	}

	|**
	 * @param maxResultOffset the maxResultOffset to set
	 *|
	public void setMaxResultOffset(int maxResultOffset) {
		this.maxResultOffset = maxResultOffset;
	}
	
	@XmlTransient
    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public void setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

	|**
	 * @param measureFilters the measureFilters to set
	 *|
	public void setResponseFilters(ArrayList<ResponseFilter> measureFilters) {
		this.responseFilters = measureFilters;
	}

	|**
	 * @return the measureFilters
	 *|
	@XmlElement(type=ArrayArrayList.class)
	public ArrayList<ResponseFilter> getResponseFilters() {
		return responseFilters;
	}
	
	|**
	 * @param new measureFilters the measureFilters to set
	 *|
	public void setMeasureFilters(ArrayList<MeasureFilterData> measureFilters) {
		this.measureFilters = measureFilters;
	}

	|**
	 * @return the measureFilters
	 *|
	@XmlElement(type=ArrayArrayList.class)
	public ArrayList<MeasureFilterData> getMeasureFilters() {
		return measureFilters;
	}
	
	
	public String getBinSource() {
        return binSource;
    }

    public void setBinSource(String binSource) {
        this.binSource = binSource;
    }

    |**
	 * Is Query optimizable/supposed to be optimized? 
	 * This parameter is applicable only for sub-queries.
	 *|
    public boolean isOptimizable() {
		return isOptimizable;
	}

	public void setOptimizable(boolean isOptimizable) {
		this.isOptimizable = isOptimizable;
	}

	@Override
	public String toString() {
		return QueryJsonUtil.queryRequestToJson(this);
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((binSource == null) ? 0 : binSource.hashCode());
		result = prime
				* result
				+ ((cubeContextDimensions == null) ? 0 : cubeContextDimensions
						.hashCode());
		result = prime * result + (int) (endTime ^ (endTime >>> 32));
		result = prime * result
				+ ((filterData == null) ? 0 : filterData.hashCode());
		result = prime * result
				+ ((filterMap == null) ? 0 : filterMap.hashCode());
		result = prime * result + ((filters == null) ? 0 : filters.hashCode());
		result = prime * result + length;
		result = prime * result + maxResultOffset;
		result = prime * result + maxResults;
		result = prime * result
				+ ((measureFilters == null) ? 0 : measureFilters.hashCode());
		result = prime * result + offset;
		result = prime * result + (isOptimizable ? 1 :0);
		result = prime * result
				+ ((paramMap == null) ? 0 : paramMap.hashCode());
		result = prime
				* result
				+ ((responseDimensions == null) ? 0 : responseDimensions
						.hashCode());
		result = prime * result
				+ ((responseFilters == null) ? 0 : responseFilters.hashCode());
		result = prime
				* result
				+ ((responseMeasures == null) ? 0 : responseMeasures.hashCode());
		result = prime * result
				+ ((searchRequest == null) ? 0 : searchRequest.hashCode());
		result = prime * result
				+ ((sortDirection == null) ? 0 : sortDirection.hashCode());
		result = prime * result
				+ ((sortProperty == null) ? 0 : sortProperty.hashCode());
		result = prime * result + (int) (startTime ^ (startTime >>> 32));
		result = prime * result
				+ ((subQuery == null) ? 0 : subQuery.hashCode());
		result = prime * result
				+ (int) (timeGranularity ^ (timeGranularity >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryRequest other = (QueryRequest) obj;
		if (binSource == null) {
			if (other.binSource != null)
				return false;
		} else if (!binSource.equals(other.binSource))
			return false;
		if (cubeContextDimensions == null) {
			if (other.cubeContextDimensions != null)
				return false;
		} else if (!cubeContextDimensions.equals(other.cubeContextDimensions))
			return false;
		if (endTime != other.endTime)
			return false;
		if (filterData == null) {
			if (other.filterData != null)
				return false;
		} else if (!filterData.equals(other.filterData))
			return false;
		if (filterMap == null) {
			if (other.filterMap != null)
				return false;
		} else if (!filterMap.equals(other.filterMap))
			return false;
		if (filters == null) {
			if (other.filters != null)
				return false;
		} else if (!filters.equals(other.filters))
			return false;
		if (length != other.length)
			return false;
		if (isOptimizable != other.isOptimizable)
			return false;
		if (maxResultOffset != other.maxResultOffset)
			return false;
		if (maxResults != other.maxResults)
			return false;
		if (measureFilters == null) {
			if (other.measureFilters != null)
				return false;
		} else if (!measureFilters.equals(other.measureFilters))
			return false;
		if (offset != other.offset)
			return false;
		if (paramMap == null) {
			if (other.paramMap != null)
				return false;
		} else if (!paramMap.equals(other.paramMap))
			return false;
		if (responseDimensions == null) {
			if (other.responseDimensions != null)
				return false;
		} else if (!responseDimensions.equals(other.responseDimensions))
			return false;
		if (responseFilters == null) {
			if (other.responseFilters != null)
				return false;
		} else if (!responseFilters.equals(other.responseFilters))
			return false;
		if (responseMeasures == null) {
			if (other.responseMeasures != null)
				return false;
		} else if (!responseMeasures.equals(other.responseMeasures))
			return false;
		if (searchRequest == null) {
			if (other.searchRequest != null)
				return false;
		} else if (!searchRequest.equals(other.searchRequest))
			return false;
		if (sortDirection == null) {
			if (other.sortDirection != null)
				return false;
		} else if (!sortDirection.equals(other.sortDirection))
			return false;
		if (sortProperty == null) {
			if (other.sortProperty != null)
				return false;
		} else if (!sortProperty.equals(other.sortProperty))
			return false;
		if (startTime != other.startTime)
			return false;
		if (subQuery == null) {
			if (other.subQuery != null)
				return false;
		} else if (!subQuery.equals(other.subQuery))
			return false;
		if (timeGranularity != other.timeGranularity)
			return false;
		return true;
	}

	|* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 *|
	|*@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("QueryRequest [cubeContextDimensions=");
		builder.append(cubeContextDimensions);
		builder.append(", endTime=");
		builder.append(endTime);
		builder.append(", filterMap=");
		builder.append(filterMap);
		builder.append(", filters=");
		builder.append(filters);
		builder.append(", length=");
		builder.append(length);
		builder.append(", maxResultOffset=");
		builder.append(maxResultOffset);
		builder.append(", maxResults=");
		builder.append(maxResults);
		builder.append(", offset=");
		builder.append(offset);
		builder.append(", paramMap=");
		builder.append(paramMap);
		builder.append(", responseDimensions=");
		builder.append(responseDimensions);
		builder.append(", responseMeasures=");
		builder.append(responseMeasures);
		builder.append(", sortDirection=");
		builder.append(sortDirection);
		builder.append(", sortProperty=");
		builder.append(sortProperty);
		builder.append(", startTime=");
		builder.append(startTime);
        builder.append(", searchRequest=");
        builder.append(searchRequest);
		builder.append(", subQuery=");
		builder.append(subQuery);
		builder.append(", timeGranularity=");
        builder.append(timeGranularity);
		builder.append("]");
		return builder.toString();
	}*|	
    public static void main(String[] args) {
		ArrayList<FilterData> filterDatas = new ArrayArrayList<FilterData>();
		FilterData filterData = new FilterData();
		ArrayList<SingleFilter> singleFilters = new ArrayArrayList<SingleFilter>();
		SingleFilter singleFilter = new SingleFilter();
		singleFilter.setDimension("a");
		singleFilter.setValue("2");
		singleFilter.setCondition("EQUAL");
		singleFilters.add(singleFilter);
		singleFilter = new SingleFilter();
		singleFilter.setDimension("b");
		singleFilter.setValue("3");
		singleFilter.setCondition("EQUAL");
		singleFilters.add(singleFilter);
		Gson gson = new Gson();
		filterData.setFilters(singleFilters);
		filterDatas.add(filterData);
		MeasureFilterData data = new MeasureFilterData();
		ArrayList<MultiFilter> multiFilters = new ArrayArrayList<MultiFilter>();
		for (int i = 0; i < 2; i++) {
			MultiFilter innerFilters=  data.new MultiFilter();
			ArrayList<MeasureSingleFilter> filter = new ArrayArrayList<MeasureSingleFilter>();
			for (int j= 0; j< 3 ; j ++) {
				filter.add(new MeasureSingleFilter("GREATER_THAN", new Double[]{(double) j}));
			}
			innerFilters.setMeasure(String.valueOf(i));
			innerFilters.setSingleFilters(filter);
			System.out.println(gson.toJson(innerFilters));
			multiFilters.add(innerFilters);
		}
		
		data.setFilters(multiFilters);
		System.out.println(gson.toJson(data));
		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setResponseDimensions(ArrayLists.<String>newArrayArrayList());
		queryRequest.setResponseMeasures(ArrayLists.<String>newArrayArrayList());
		queryRequest.setFilterData(filterDatas);
		queryRequest.setMeasureFilters(com.google.common.collect.ArrayLists.newArrayArrayList(data));
		System.out.println(gson.toJson(queryRequest));
		System.out.println(gson.fromJson(gson.toJson(queryRequest),QueryRequest.class));
		System.out.println(gson.fromJson("{'responseMeasures':['CompUpBytes','CompDownBytes'],'responseDimensions':['Attribute'], 'sortProperty':'CompUpBytes','filters':[],'cubeContextDimensions':[],'sortDirection':'DSC','maxResults':-1,'maxResultOffset':0,'length':50,'offset':0,'startTime':1349917200,'endTime':1349935200,'timeGranularity':0,'filters':[[{'name':'Agony','value':'6'}]],'measureFilters':[{'filters':[{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'CompUpBytes'},{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'CompDownBytes'}]}]}", QueryRequest.class));
		System.out.println(gson.fromJson("{'filters':[{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'0'},{'singleFilters':[{'operand':[0.0],'operator':'GREATER_THAN'},{'operand':[1.0],'operator':'GREATER_THAN'},{'operand':[2.0],'operator':'GREATER_THAN'}],'measure':'1'}]}", MeasureFilterData.class));
	
    	//System.out.println(QueryJsonUtil.fromJsonToQueryRequest("{\"subQuery\":{\"responseMeasures\":[\"TotalViews\",\"DownStreamPeakBitRate\",\"UpStreamPeakBitRate\",\"TotalBitRate\",\"UpStreamTonnage\",\"DownStreamTonnage\",\"TotalTonnage\",\"TotalOperationsTransactions\",\"SuccessRate\",\"SuccessRateByBytesDownloaded\"],\"responseDimensions\":[\"SERVING_REGION_ID\",\"SERVING_REGION_NAME_BASIC\"],\"filters\":[[]],\"sortProperty\":\"PeakCpuUsageBasic\",\"sortDirection\":\"DSC\",\"maxResults\":0,\"maxResultOffset\":0,\"length\":50,\"offset\":0,\"startTime\":1361854800,\"endTime\":1362459600,\"timeGranularity\":0},\"responseMeasures\":[\"PeakCpuUsageBasic\"],\"responseDimensions\":[],\"cubeContextDimensions\":[\"SHIELD_CACHE_OR_DA_ID\"],\"filters\":[[{\"name\":\"SERVING_REGION_ID\",\"value\":\"382\"}]],\"paramMap\":[],\"maxResults\":-1,\"maxResultOffset\":0,\"length\":-1,\"offset\":0,\"startTime\":1361854800,\"endTime\":1362459600,\"timeGranularity\":0}").toSql(""));
	}
    
    
    public String toSql(String ts1)
 {

		ArrayList<String> columns = new ArrayArrayList<String>();
		columns.addAll(responseDimensions);
		columns.addAll(responseMeasures);
		if (cubeContextDimensions != null) {
			for (String dimension : cubeContextDimensions) {
				columns.add("c." + dimension);
			}
		}

		String abs = "select "
				+ ts1
				+ columns.toString().substring(1,
						columns.toString().length() - 1)
				+ " from global where "
				+ (subQuery == null ? "" : " (placeholder) in ("
						+ subQuery.toSql("") + ") and ")
				+ " startTime = "
				+ startTime
				+ " and endTime = "
				+ endTime
				+ ((filters == null || filters.size() == 0 || calculateDimensionFilters().equalsIgnoreCase("  ")) ? "" : " and "
						+ calculateDimensionFilters())
				+ ((paramMap == null || paramMap.size() == 0) ? "" : " and "
						+ calculateParams(paramMap))
				+ ((responseFilters == null || responseFilters.size() == 0) ? "" : " and "
						+ calculateResponseFilters())
				+ ((binSource == null) ? "" : " and "
						+ " binSource " + " = '" + binSource + "' ")
				+ " and timeGranularity = "
				+ timeGranularity
				+ (searchRequest == null ? "" : " and (placeholder) in ("
						+ searchRequest.toSql() + ") ")
				+ ((sortProperty == null || sortProperty.isEmpty()) ? " "
						: " order by "
								+ sortProperty
								+ " "
								+ ((sortDirection.equals(SortDirection.ASC
										.name()) ? " asc" : " desc")))
				+ ((length == -1) ? "" : "  limit " + length)
				+ ((offset == 0 ) ? "" : " offset " + offset + " ");
				
		return abs;

	}
    
    
    private String calculateParams(Collection<NameValue> params) {
    	String sql = " ";
    		for(NameValue nameValue : params) {
    			sql += nameValue.toSql() + " AND ";
    		}
    		if(!sql.equalsIgnoreCase(" ")) sql = sql.substring(0, sql.length() - 4);
    		sql+= " ";
    	return sql;
    }
    
    private String calculateResponseFilters() {
    	String sql = " ";
    		for(ResponseFilter nameValue : responseFilters) {
    			sql += nameValue.toSql() + " AND ";
    		}
    		sql = sql.substring(0, sql.length() - 4);
    		sql+= " ";
    	return sql;
    }
    
    
    private String calculateDimensionFilters() {
    	if(filters.size() == 1) {
    		return calculateParams(filters.get(0));
    	}
    	String sql = " (";
    	for(ArrayList<NameValue> filter : filters) {
    		sql+= "(";
    		for(NameValue nameValue : filter) {
    			sql += nameValue.toSql() + " AND ";
    		}
    		sql = sql.substring(0, sql.length() - 4);
    		sql += ") or ";
    		
    		
    	}
    	sql = sql.substring(0,sql.length()-3);
    	
    	sql += ")";
    	return sql;
    }
}

*/
}