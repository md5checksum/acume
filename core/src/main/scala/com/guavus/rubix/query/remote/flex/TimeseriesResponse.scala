package com.guavus.rubix.query.remote.flex

import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import TimeseriesResponse._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import java.util.ArrayList
import com.guavus.acume.core.configuration.ConfigFactory
import java.util.List
import com.guavus.acume.core.AcumeContextTrait

object TimeseriesResponse {

	val gsonBuilder = new GsonBuilder()
	
	var gson: Gson = gsonBuilder.create()

	gsonBuilder.registerTypeAdapter(classOf[TimeseriesResultSet], new TimeseriesResultSet.JsonAdaptor())
}

@XmlRootElement
class TimeseriesResponse(@BeanProperty var results: List[TimeseriesResultSet], @BeanProperty var responseDimensions: List[String], @BeanProperty var responseMeasures: List[String], @BeanProperty var timestamps: List[Long]) extends Serializable {

  override def toString(): String = {
    val maxLen = ConfigFactory.getInstance.getBean(classOf[AcumeContextTrait]).acumeConf().getMaxQueryLogRecords	
    val tsResponse = new TimeseriesResponse(results.subList(0,Math.min(results.size, maxLen)), responseDimensions, responseMeasures, timestamps)
    gson.toJson(tsResponse)
  }

/*
Original Java:
package com.guavus.acume.rest.beans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.guavus.rubix.configuration.RubixProperties;

@XmlRootElement
public class TimeseriesResponse  implements Serializable{

	|**
	 * List of results 
	 *|
	private  List<TimeseriesResultSet> results;

	
	|**
	 * Ordered list of Dimensions in the <code>results</code>
	 *|
	private List<String> responseDimensions;
	
	|**
	 * Ordered list of Measures in the <code>responseMeasures</code>
	 *|
	private List<String> responseMeasures ;
	
	|**
	 * Ordered list of sampled timestamps 
	 *|
	private List<Long> timestamps;
	
	

	public TimeseriesResponse(List<TimeseriesResultSet> results,
			List<String> responseDimensions, List<String> responseMeasures,
			List<Long> timestamps) {
		super();
		this.results = results;
		this.responseDimensions = responseDimensions;
		this.responseMeasures = responseMeasures;
		this.timestamps = timestamps;
	}

	@XmlElement(type=ArrayList.class)
	public List<TimeseriesResultSet> getResults() {
		return results;
	}

	public void setResults(List<TimeseriesResultSet> results) {
		this.results = results;
	}

	@XmlElement(type=ArrayList.class)
	public List<String> getResponseDimensions() {
		return responseDimensions;
	}

	public void setResponseDimensions(List<String> responseDimensions) {
		this.responseDimensions = responseDimensions;
	}

	@XmlElement(type=ArrayList.class)
	public List<String> getResponseMeasures() {
		return responseMeasures;
	}

	public void setResponseMeasures(List<String> responseMeasures) {
		this.responseMeasures = responseMeasures;
	}

	@XmlElement(type=ArrayList.class)
	public List<Long> getTimestamps() {
		return timestamps;
	}

	public void setTimestamps(List<Long> timestamps) {
		this.timestamps = timestamps;
	}

    |**
     * Used for converting this to json String
     *|
    private static Gson gson;
    
    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(TimeseriesResultSet.class, new TimeseriesResultSet.JsonAdaptor());
        gson  = gsonBuilder.create();
    }
    
    @Override
    public String toString() {
    	final int maxLen = RubixProperties.MaxLength.getIntValue();
        
        TimeseriesResponse tsResponse = new TimeseriesResponse(results.subList(0,
				Math.min(results.size(), maxLen)),
				responseDimensions, responseMeasures,
				timestamps);
				
		return gson.toJson(tsResponse);
		
    }
	
}

*/
}