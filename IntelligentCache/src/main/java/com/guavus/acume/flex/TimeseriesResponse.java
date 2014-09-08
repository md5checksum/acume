package com.guavus.acume.flex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.guavus.acume.common.EquinoxConstants;
import com.guavus.acume.configuration.AcumeConfiguration;


@XmlRootElement
public class TimeseriesResponse  implements Serializable{

	/**
	 * List of results 
	 */
	private  List<TimeseriesResultSet> results;

	
	/**
	 * Ordered list of Dimensions in the <code>results</code>
	 */
	private List<String> responseDimensions;
	
	/**
	 * Ordered list of Measures in the <code>responseMeasures</code>
	 */
	private List<String> responseMeasures ;
	
	/**
	 * Ordered list of sampled timestamps 
	 */
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

    /**
     * Used for converting this to json String
     */
    private static Gson gson;
    
    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(TimeseriesResultSet.class, new TimeseriesResultSet.JsonAdaptor());
        gson  = gsonBuilder.create();
    }
    
    @Override
    public String toString() {
    	final int maxLen = AcumeConfiguration.MaxLength.getIntValue();
        
        TimeseriesResponse tsResponse = new TimeseriesResponse(results.subList(0,
				Math.min(results.size(), maxLen)),
				responseDimensions, responseMeasures,
				timestamps);
        
        tsResponse.applyEncryption();
				
		return gson.toJson(tsResponse);	
    }
    
    private void applyEncryption() {
		HashSet<String> encryptedDimensions = new HashSet<String>(Arrays.asList(AcumeConfiguration.EncryptedDimensions.getStringArray(";")));
		HashSet<String> encryptedMeasures = new HashSet<String>(Arrays.asList(AcumeConfiguration.EncryptedMeasures.getStringArray(";")));
		
		List<Integer> encryptedDimensionIndexes = new ArrayList<Integer>();
		List<Integer> encryptedMeasureIndexes = new ArrayList<Integer>();
		
		for (Integer i = 0; i < responseDimensions.size(); ++i) {
			String responseDimension = responseDimensions.get(i);
			if (encryptedDimensions.contains(responseDimension)) {
				encryptedDimensionIndexes.add(i);
			}
		}
		
		for (Integer i = 0; i < responseMeasures.size(); ++i) {
			String responseMeasure = responseMeasures.get(i);
			if (encryptedMeasures.contains(responseMeasure)) {
				encryptedMeasureIndexes.add(i);
			}
		}
		
		for (TimeseriesResultSet resultSet : results) {
			
			for (Integer encryptedDimensionIndex : encryptedDimensionIndexes) {
				resultSet.getRecord().set(encryptedDimensionIndex, EquinoxConstants.ENCRYPTION_VALUE());
			}
			
			List<List<Object>> measures = resultSet.getMeasures();
			
			for (Integer encryptedMeasureIndex : encryptedMeasureIndexes) {
				List<Object> measure = measures.get(encryptedMeasureIndex);
				for (int i = 0; i < measure.size(); ++i) {
					measure.set(i, EquinoxConstants.ENCRYPTION_VALUE());
				}
			}
		} 
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TimeseriesResponse other = (TimeseriesResponse) obj;
		if (responseDimensions == null) {
			if (other.responseDimensions != null)
				return false;
		} else if (!responseDimensions.equals(other.responseDimensions))
			return false;
		if (responseMeasures == null) {
			if (other.responseMeasures != null)
				return false;
		} else if (!responseMeasures.equals(other.responseMeasures))
			return false;
		if (timestamps == null) {
			if (other.timestamps != null)
				return false;
		} else if (!timestamps.equals(other.timestamps))
			return false;
		if (results == null) {
			if (other.results != null)
				return false;
		} else {
			if (other.getResults().size() != this.results.size()) {
                return false;
            }

            for (int i = 0; i < results.size(); i++) {
                if (!other.getResults().contains(results.get(i))) {
                    return false;
                }
            }
		}
		return true;
	}
	
}
