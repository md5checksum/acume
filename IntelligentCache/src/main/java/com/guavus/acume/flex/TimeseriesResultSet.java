package com.guavus.acume.flex;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

public class TimeseriesResultSet implements Serializable {

	/**
	 * Ordered List of Dimension values . The order is defined by
	 * {@link TimeseriesResponse.responseDimensions}
	 */
	private List<String> record ;
	
	
	/**
	 * Ordered List of Time stamps  Measure values . The order is defined by
	 * first {@link TimeseriesResponse.timestamps}  and then {@link TimeseriesResponse.responseMeasures}
	 */
	private List<List<Object>> measures ;

	
	
	public TimeseriesResultSet(List<String> record, List<List<Object>> measures) {
		super();
		this.record = record;
		this.measures = measures;
	}

	public List<String> getRecord() {
		return record;
	}

	public void setRecord(List<String> record) {
		this.record = record;
	}

	public List<List<Object>> getMeasures() {
		return measures;
	}

	public void setMeasures(List<List<Object>> measures) {
		this.measures = measures;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TimeseriesResultSet [record=");
		builder.append(record);
		builder.append(", measures=");
		builder.append(measures);
		builder.append("]");
		return builder.toString();
	}
	
	public static class JsonAdaptor implements JsonDeserializer<TimeseriesResultSet>{
	    private static Gson gson = new Gson();
	    private static Type listOfDouble = new TypeToken<List<Double>>() {}.getType();
        @Override
        public TimeseriesResultSet deserialize(JsonElement jsonElement, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
            List<String> record = new ArrayList<String>();
            JsonObject json = (JsonObject) jsonElement;
            List<List<Object>> measures = gson.fromJson(json.get("measures"), listOfDouble);
            JsonArray jsonArray = json.get("record").getAsJsonArray();
            for(JsonElement jsonItem: jsonArray)
            {
                record.add(jsonItem.getAsString());
            }
            return new TimeseriesResultSet(record, measures);
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
		TimeseriesResultSet other = (TimeseriesResultSet) obj;
		if (measures == null) {
			if (other.measures != null)
				return false;
		} else if (!measures.equals(other.measures))
			return false;
		if (record == null) {
			if (other.record != null)
				return false;
		} else if (!record.equals(other.record))
			return false;
		return true;
	}
}
