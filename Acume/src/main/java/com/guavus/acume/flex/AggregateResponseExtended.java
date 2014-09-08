package com.guavus.acume.flex;

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
