package com.guavus.rubix.query.remote.flex

import java.io.Serializable
import java.util.List
import scala.reflect.BeanProperty

@SerialVersionUID(-2432627248383151081L)
class SearchRequest extends Serializable {

  @BeanProperty
  var searchCriteria: SearchCriteria = _

  @BeanProperty
  var responseDimensions: List[String] = _

  @BeanProperty
  var cubeContextDimensions: List[String] = _

  @BeanProperty
  var startTime: Long = _

  @BeanProperty
  var endTime: Long = _

  @BeanProperty
  var timeGranularity: Long = _

  @BeanProperty
  var limit: Int = _

  def this(searchCriteria: SearchCriteria, responseDimensions: List[String], startTime: Long, endTime: Long, timeGranularity: Long) {
    this()
    this.searchCriteria = searchCriteria
    this.startTime = startTime
    this.responseDimensions = responseDimensions
    this.endTime = endTime
    this.setTimeGranularity(timeGranularity)
    this.limit = -1
  }

  def this(searchCriteria: SearchCriteria, responseDimensions: List[String], startTime: Long, endTime: Long, timeGranularity: Long, limit: Int) {
    this()
    this.searchCriteria = searchCriteria
    this.startTime = startTime
    this.responseDimensions = responseDimensions
    this.endTime = endTime
    this.setTimeGranularity(timeGranularity)
    this.limit = limit
  }

  override def toString(): String = {
    "SearchRequest [searchCriteria=" + searchCriteria + ", responseDimensions=" + responseDimensions + ", startTime=" + startTime + ", endTime=" + endTime + ", timeGranularity=" + timeGranularity + "]"
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (endTime ^ (endTime >>> 32)).toInt
    result = prime * result + (if ((responseDimensions == null)) 0 else responseDimensions.hashCode)
    result = prime * result + (if ((searchCriteria == null)) 0 else searchCriteria.hashCode)
    result = prime * result + (startTime ^ (startTime >>> 32)).toInt
    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[SearchRequest]
    if (endTime != other.endTime) return false
    if (responseDimensions == null) {
      if (other.responseDimensions != null) return false
    } else if (responseDimensions != other.responseDimensions) return false
    if (searchCriteria == null) {
      if (other.searchCriteria != null) return false
    } else if (searchCriteria != other.searchCriteria) return false
    if (startTime != other.startTime) return false
    true
  }

  def toSql(): String = {
    val abs = "select " + responseDimensions.toString.substring(1, responseDimensions.toString.length - 1) + " from search where startTime = " + startTime + " and endTime = " + endTime + " and timeGranularity = " + timeGranularity + (if (searchCriteria.isEmpty) "" else " and " + searchCriteria.toSql()) + " "
    abs
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.search;

import java.io.Serializable;
import java.util.List;

|**
 * @author Akhil Swain
 * 
 *|
public class SearchRequest implements Serializable {
	private static final long serialVersionUID = -2432627248383151081L;
	private SearchCriteria searchCriteria;
	private List<String> responseDimensions;
	private List<String> cubeContextDimensions;
	private long startTime;
	private long endTime;
	private long timeGranularity;
	private int limit ;

	public SearchRequest() {

	}

	public SearchRequest(SearchCriteria searchCriteria,
			List<String> responseDimensions, long startTime, long endTime,
			long timeGranularity) {
		super();
		this.searchCriteria = searchCriteria;
		this.startTime = startTime;
		this.responseDimensions = responseDimensions;
		this.endTime = endTime;
		this.setTimeGranularity(timeGranularity);
		this.limit = -1;
	}
	
	public SearchRequest(SearchCriteria searchCriteria,
			List<String> responseDimensions, long startTime, long endTime,
			long timeGranularity,int limit) {
		super();
		this.searchCriteria = searchCriteria;
		this.startTime = startTime;
		this.responseDimensions = responseDimensions;
		this.endTime = endTime;
		this.setTimeGranularity(timeGranularity);
		this.limit = limit;
	}

	public SearchCriteria getSearchCriteria() {
		return searchCriteria;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setResponseDimensions(List<String> responseDimensions) {
		this.responseDimensions = responseDimensions;
	}

	public List<String> getResponseDimensions() {
		return responseDimensions;
	}
	
	public List<String> getCubeContextDimensions() {
        return cubeContextDimensions;
    }
	
	public void setCubeContextDimensions(List<String> cubeContextDimensions) {
        this.cubeContextDimensions = cubeContextDimensions;
    }

	public void setSearchCriteria(SearchCriteria searchCriteria) {
		this.searchCriteria = searchCriteria;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	|**
	 * @param timeGranularity
	 *            the timeGranularity to set
	 *|
	public void setTimeGranularity(long timeGranularity) {
		this.timeGranularity = timeGranularity;
	}

	|**
	 * @return the timeGranularity
	 *|
	public long getTimeGranularity() {
		return timeGranularity;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	@Override
	public String toString() {
		return "SearchRequest [searchCriteria=" + searchCriteria
				+ ", responseDimensions=" + responseDimensions + ", startTime="
				+ startTime + ", endTime=" + endTime + ", timeGranularity="
				+ timeGranularity + "]";
	}

	|* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 *|
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (endTime ^ (endTime >>> 32));
		result = prime
				* result
				+ ((responseDimensions == null) ? 0 : responseDimensions
						.hashCode());
		result = prime * result
				+ ((searchCriteria == null) ? 0 : searchCriteria.hashCode());
		result = prime * result + (int) (startTime ^ (startTime >>> 32));
		return result;
	}

	|* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 *|
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SearchRequest other = (SearchRequest) obj;
		if (endTime != other.endTime)
			return false;
		if (responseDimensions == null) {
			if (other.responseDimensions != null)
				return false;
		} else if (!responseDimensions.equals(other.responseDimensions))
			return false;
		if (searchCriteria == null) {
			if (other.searchCriteria != null)
				return false;
		} else if (!searchCriteria.equals(other.searchCriteria))
			return false;
		if (startTime != other.startTime)
			return false;
		return true;
	}
	
	
	public String toSql() {
		String abs = "select "
				+ responseDimensions.toString().substring(1,
						responseDimensions.toString().length() - 1)
				+ " from search where startTime = " + startTime
				+ " and endTime = " + endTime + " and timeGranularity = "
				+ timeGranularity + (searchCriteria.isEmpty() ? "": " and " + searchCriteria.toSql()) + " ";
		return abs;
	}
}

*/
}