package com.guavus.acume.flex;

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
