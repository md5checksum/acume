package com.guavus.acume.flex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.guavus.acume.common.*;
import com.guavus.acume.configuration.AcumeConfiguration;
import com.guavus.acume.query.data.Table;

/**
 * 
 * Response object for Aggregate requests 
 *
 */
@XmlRootElement
public class AggregateResponse implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * List of results 
	 */
	private List<AggregateResultSet> results ;
	
	/**
	 * Ordered list of Dimensions in the <code>results</code>
	 */
	private List<String> responseDimensions ;
	
	
	/**
	 * Ordered list of Measures in the <code>responseMeasures</code>
	 */
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

	/**
	 * Total number of records
	 */
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


    /**
     * Used for converting this to json String
     */
    private static Gson gson;
    
    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(AggregateResultSet.class, new AggregateResultSet.JsonAdaptor());
        gson  = gsonBuilder.create();
    }
    /*
    public static AggregateResponse fromRequestAndTable(QueryRequest request,Table<Integer, String, Object> table) {
    	
    	List<String> requestDimensions = request.getResponseDimensions();
    	List<String> requestMeasures = request.getResponseMeasures();

		List<AggregateResultSet> aggregateResultSets = new ArrayList<AggregateResultSet>();
		
		final List<String> requestDimensionsCopy = new ArrayList<String>(requestDimensions);
		Comparator<String> comparator = new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				if(!requestDimensionsCopy.contains(o1) && !requestDimensionsCopy.contains(o2)) {
					return 0;
				}
				if(!requestDimensionsCopy.contains(o1)) {
					return 1;
				}
				if(!requestDimensionsCopy.contains(o2)) {
					return -1;
				}
				if(requestDimensionsCopy.indexOf(o1) < requestDimensionsCopy.indexOf(o2)) {
					return -1;
				}
				return 1;
			}
		};
		
		List<String> columns = new ArrayList<String>(table.columnKeySet());
		Collections.sort(columns, comparator);
		
		//TODO:sorted rows on rowId??
    	for(Integer rowId : table.rowKeySet()) {

			List<Object> record = new ArrayList<Object>();
			List<Object> measuresRecord = new ArrayList<Object>();
			
    		for(String sqlCubeProperty : columns) {
    			Object obj = table.get(rowId, sqlCubeProperty);
    			if(requestDimensions != null && requestDimensions.contains(sqlCubeProperty)) {
    				record.add(obj);
    			} else if(requestMeasures != null && requestMeasures.contains(sqlCubeProperty)) {
    				measuresRecord.add(obj);
    			} else { //This is the case when solution adds some new columns through the hook provided by us
    			         //and we are unaware of those columns. We will treat them as dimensions and add them in 
    				     //response accordingly. We are assuming that new columns will always come after 
    				     //defined dimensions and measures columns
    				if(requestDimensions == null) {
    					requestDimensions = new ArrayList<String>();
    				}
    				if(!requestDimensions.contains(sqlCubeProperty)) {
    					requestDimensions.add(sqlCubeProperty);
    				}
    				record.add(obj);
    			}
    		}
    		AggregateResultSet aggregateResultSet = new AggregateResultSet(record, measuresRecord);
			aggregateResultSets.add(aggregateResultSet);
    	}
    	
    	AggregateResponse response = new AggregateResponse(aggregateResultSets, 
    			requestDimensions, 
    			requestMeasures, 
    			aggregateResultSets.size());
    	
		return response;
    }
    */
    
	@Override
	public String toString() {
		final int maxLen = AcumeConfiguration.MaxLength.getIntValue();

		AggregateResponse aggResponse = new AggregateResponse(results.subList(0,
				Math.min(results.size(), maxLen)),
				responseDimensions, responseMeasures,
				totalRecords);
		
		aggResponse.applyEncryption();
		
		return gson.toJson(aggResponse);
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
		
		for (AggregateResultSet resultSet : results) {
			
			for (Integer encryptedDimensionIndex : encryptedDimensionIndexes) {
				resultSet.getRecord().set(encryptedDimensionIndex, AcumeConstants.ENCRYPTION_VALUE());
			}
			
			for (Integer encryptedMeasureIndex : encryptedMeasureIndexes) {
				resultSet.getMeasures().set(encryptedMeasureIndex, AcumeConstants.ENCRYPTION_VALUE());
			}
		} 
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
