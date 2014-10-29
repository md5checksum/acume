package com.guavus.acume.rest.beans

import java.util.List

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.BeanProperty

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeContext
import com.guavus.acume.core.configuration.ConfigFactory

import AggregateResponse._
import javax.xml.bind.annotation.XmlRootElement

object AggregateResponse {

  val gsonBuilder = new GsonBuilder()
	
  var gson: Gson = gsonBuilder.create()


  gsonBuilder.registerTypeAdapter(classOf[AggregateResultSet], new AggregateResultSet.JsonAdaptor())

  def fromString(response: String): AggregateResponse = {
    gson.fromJson(response, classOf[AggregateResponse])
  }
}

@SerialVersionUID(1L)
@XmlRootElement
class AggregateResponse extends Serializable {

  @BeanProperty
  var results: List[AggregateResultSet] = _

  @BeanProperty
  var responseDimensions: List[String] = _

  @BeanProperty
  var responseMeasures: List[String] = _

  def this(results: List[AggregateResultSet], responseDimensions: List[String], responseMeasures: List[String], totalRecords: Int) {
    this()
    this.results = results
    this.responseDimensions = responseDimensions
    this.responseMeasures = responseMeasures
    this.totalRecords = totalRecords
  }

  @BeanProperty
  var totalRecords: Int = _

  override def toString(): String = {
    val maxLen = ConfigFactory.getInstance.getBean(classOf[AcumeContext]).acumeConf.getMaxQueryLogRecords
    val aggResponse = new AggregateResponse(results.subList(0, Math.min(results.size, maxLen)), responseDimensions, responseMeasures, totalRecords)
    gson.toJson(aggResponse)
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      return false
    }
    if (!(obj.isInstanceOf[AggregateResponse])) {
      return false
    }
    val response = obj.asInstanceOf[AggregateResponse]
    if (this == obj) {
      return true
    }
    if (response.getResponseDimensions == responseDimensions && response.getResponseMeasures == responseMeasures && response.getTotalRecords == totalRecords) {
      if (response.getResults == this.results) {
        return true
      }
      if (response.getResults == null || this.results == null) {
        return false
      }
      if (response.getResults.size != this.results.size) {
        return false
      }
      for (i <- 0 until results.size if !response.getResults.contains(results.get(i))) {
        return false
      }
      true
    } else {
      false
    }
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


|**
 * 
 * Response object for Aggregate requests 
 *
 *|
@XmlRootElement
public class AggregateResponse implements Serializable {

	|**
	 * 
	 *|
	private static final long serialVersionUID = 1L;

	|**
	 * List of results 
	 *|
	private List<AggregateResultSet> results ;
	
	|**
	 * Ordered list of Dimensions in the <code>results</code>
	 *|
	private List<String> responseDimensions ;
	
	
	|**
	 * Ordered list of Measures in the <code>responseMeasures</code>
	 *|
	private List<String> responseMeasures;
	
	public AggregateResponse() {
	}

	
	public AggregateResponse(List<AggregateResultSet> results,
			List<String> responseDimensions, List<String> responseMeasures,
			int totalRecords) {
		super();
		this.results = results;
		this.responseDimensions = responseDimensions;
		this.responseMeasures = responseMeasures;
		this.totalRecords = totalRecords;
	}

	|**
	 * Total number of records
	 *|
	private int totalRecords ;

	
	@XmlElement(type=ArrayList.class)
	public List<AggregateResultSet> getResults() {
		return results;
	}

	public void setResults(List<AggregateResultSet> results) {
		this.results = results;
	}

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

	public int getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(int totalRecords) {
		this.totalRecords = totalRecords;
	}


    |**
     * Used for converting this to json String
     *|
    private static Gson gson;
    
    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(AggregateResultSet.class, new AggregateResultSet.JsonAdaptor());
        gson  = gsonBuilder.create();
    }
    
	@Override
	public String toString() {
		final int maxLen = RubixProperties.MaxLength.getIntValue();

		AggregateResponse aggResponse = new AggregateResponse(results.subList(0,
				Math.min(results.size(), maxLen)),
				responseDimensions, responseMeasures,
				totalRecords);
				
		return gson.toJson(aggResponse);
	}
    
    public static AggregateResponse fromString(String response) {
        return gson.fromJson(response, AggregateResponse.class);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof AggregateResponse)) {
            return false;
        }
        AggregateResponse response = (AggregateResponse) obj;
        if (this == obj) {
            return true;
        }
        if (response.getResponseDimensions().equals(responseDimensions)
                && response.getResponseMeasures().equals(responseMeasures)
                && response.getTotalRecords() == totalRecords) {
            // check if aggregateresultset is same.
            if (response.getResults() == this.results) {
                return true;
            }
            if (response.getResults() == null || this.results == null) {
                return false;
            }
            if (response.getResults().size() != this.results.size()) {
                return false;
            }

            for (int i = 0; i < results.size(); i++) {
                if (!response.getResults().contains(results.get(i))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }
	

	
	
}

*/
}