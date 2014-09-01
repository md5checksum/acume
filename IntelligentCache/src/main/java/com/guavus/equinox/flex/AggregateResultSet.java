package com.guavus.equinox.flex;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

/**
 * Single Resulset 
 * 
 *
 */
public class AggregateResultSet implements Serializable{
	

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Ordered List of Dimension values . The order is defined by
	 * {@link AggregateResponse.responseDimensions}
	 */
	private List<Object> record ;
	
	/**
	 * Ordered List of Measure values . The order is defined by
	 * {@link AggregateResponse.responseMeasures}
	 */
	private List<Object> measures ;
	
	public AggregateResultSet() {
	}
	
	
	public AggregateResultSet(List<Object> record, List<Object> measures) {
		super();
		this.record = record;
		this.measures = measures;
	}

	
	@XmlElement(type=ArrayList.class)
	public List<Object> getRecord() {
		return record;
	}

	public void setRecord(List<Object> record) {
		this.record = record;
	}

	
	@XmlElement(type=ArrayList.class)
	public List<Object> getMeasures() {
		return measures;
	}

	public void setMeasures(List<Object> measures) {
		this.measures = measures;
	}

	@Override
	public String toString() {
	    DecimalFormat format = new DecimalFormat("#.000000");
	    //format.applyPattern("#.######");
	    List<String> duplicateMeasureList =  new ArrayList<String>();
	    for (int i = 0; i < measures.size(); i++) {
            duplicateMeasureList.add(format.format(measures.get(i)));
        }
		StringBuilder builder = new StringBuilder();
		builder.append("AggregateResultSet [measures=");
		builder.append(duplicateMeasureList);
		builder.append(", record=");
		builder.append(record);
		builder.append("]");
		return builder.toString();
	}
	
	@Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof AggregateResultSet)) {
            return false;
        }

        AggregateResultSet resultSet = (AggregateResultSet) obj;
        if (resultSet == this) {
            return true;
        }
        //This is slow. Only used for testing purpose
        if (resultSet.toString().equals(this.toString())) {
            return true;
        } else {
            return false;
        }
    }
    
	public static class JsonAdaptor implements JsonDeserializer<AggregateResultSet>{
	    private static Gson gson = new Gson();
	    private static Type listOfDouble = new TypeToken<List<Double>>() {}.getType();
        @Override
        public AggregateResultSet deserialize(JsonElement jsonElement, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
            List<Object> record = new ArrayList<Object>();
            JsonObject json = (JsonObject) jsonElement;
            List<Object> measures = gson.fromJson(json.get("measures"), listOfDouble);
            JsonArray jsonArray = json.get("record").getAsJsonArray();
            for(JsonElement jsonItem: jsonArray)
            {
                record.add(jsonItem.getAsString());
            }
            return new AggregateResultSet(record, measures);
        }   

    }
}
