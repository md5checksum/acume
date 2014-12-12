package com.guavus.rubix.query.remote.flex

import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import java.util.ArrayList
import java.util.List

@SerialVersionUID(622337018807038280L)
@XmlRootElement
class AggregateResponseExtended(results: List[AggregateResultSet], responseDimensions: List[String], responseMeasures: List[String], totalRecords: Int, @BeanProperty var executionTime: Long = 0) extends AggregateResponse(results, responseDimensions, responseMeasures, totalRecords) {
 
  def this(aggregateResponse: AggregateResponse, executionTime: Long) {
    this(aggregateResponse.getResults, aggregateResponse.getResponseDimensions, aggregateResponse.getResponseMeasures, aggregateResponse.getTotalRecords)
    this.executionTime = executionTime
  }

  override def toString(): String = {
    "AggregateResponseExtended [executionTime=" + executionTime + ", getResults()=" + getResults + ", getResponseDimensions()=" + getResponseDimensions + ", getResponseMeasures()=" + getResponseMeasures + ", getTotalRecords()=" + getTotalRecords + "]"
  }

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class AggregateResponseExtended extends AggregateResponse {
	
	public AggregateResponseExtended(List<AggregateResultSet> results,
			List<String> responseDimensions, List<String> responseMeasures,
			int totalRecords, long executionTime) {
		super(results, responseDimensions, responseMeasures, totalRecords);
		this.executionTime = executionTime;
	}

	public AggregateResponseExtended(AggregateResponse aggregateResponse, long executionTime) {
		super(aggregateResponse.getResults(), aggregateResponse.getResponseDimensions(), 
				aggregateResponse.getResponseMeasures(), aggregateResponse.getTotalRecords());
		this.executionTime = executionTime;
	}
	
	private static final long serialVersionUID = 622337018807038280L;

	private long executionTime;
	
	public long getExecutionTime() {
		return executionTime;
	}
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}

	@Override
	public String toString() {
		return "AggregateResponseExtended [executionTime=" + executionTime
				+ ", getResults()=" + getResults()
				+ ", getResponseDimensions()=" + getResponseDimensions()
				+ ", getResponseMeasures()=" + getResponseMeasures()
				+ ", getTotalRecords()=" + getTotalRecords() + "]";
	}
	
	
}

*/
}