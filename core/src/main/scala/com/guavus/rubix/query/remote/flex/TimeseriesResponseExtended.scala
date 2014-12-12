package com.guavus.rubix.query.remote.flex

import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import java.util.ArrayList
import java.util.List

@SerialVersionUID(-1764259705886717573L)
@XmlRootElement
class TimeseriesResponseExtended(results: List[TimeseriesResultSet], responseDimensions: List[String], responseMeasures: List[String], timestamps: List[Long], @BeanProperty var executionTime: Long = 0) extends TimeseriesResponse(results, responseDimensions, responseMeasures, timestamps) {

  def this(response: TimeseriesResponse, executionTime: Long) {
    this(response.getResults, response.getResponseDimensions, response.getResponseMeasures, response.getTimestamps)
    this.executionTime = executionTime
  }

  override def toString(): String = {
    "TimeseriesResponseExtended [executionTime=" + executionTime + ", getResults()=" + getResults + ", getResponseDimensions()=" + getResponseDimensions + ", getResponseMeasures()=" + getResponseMeasures + ", getTimestamps()=" + getTimestamps + "]"
  }

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TimeseriesResponseExtended  extends TimeseriesResponse{

	public TimeseriesResponseExtended(List<TimeseriesResultSet> results,
			List<String> responseDimensions, List<String> responseMeasures,
			List<Long> timestamps, long executionTime) {
		super(results, responseDimensions, responseMeasures, timestamps);
		this.executionTime = executionTime;
	}

	public TimeseriesResponseExtended(TimeseriesResponse response, long executionTime) {
		super(response.getResults(), response.getResponseDimensions(), response.getResponseMeasures(), response.getTimestamps());
		this.executionTime = executionTime;
	}
	
	private static final long serialVersionUID = -1764259705886717573L;
	
	private long executionTime;
	
	public long getExecutionTime() {
		return executionTime;
	}
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}
	
	@Override
	public String toString() {
		return "TimeseriesResponseExtended [executionTime=" + executionTime
				+ ", getResults()=" + getResults()
				+ ", getResponseDimensions()=" + getResponseDimensions()
				+ ", getResponseMeasures()=" + getResponseMeasures()
				+ ", getTimestamps()=" + getTimestamps() + "]";
	}
	
}

*/
}