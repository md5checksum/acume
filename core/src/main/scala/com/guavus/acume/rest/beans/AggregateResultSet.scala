package com.guavus.acume.rest.beans

import java.io.Serializable
import java.lang.reflect.Type
import java.text.DecimalFormat
import java.util.ArrayList
import java.util.List
import javax.xml.bind.annotation.XmlElement
import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParseException
import com.google.gson.reflect.TypeToken
import AggregateResultSet._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
//remove if not needed
import scala.collection.JavaConversions._

object AggregateResultSet {

  object JsonAdaptor {

    private var gson: Gson = new Gson()

  }

  class JsonAdaptor extends JsonDeserializer[AggregateResultSet] {

	  private var listOfDouble: Type = new TypeToken[List[Double]]() {
  }.getType
  
    override def deserialize(jsonElement: JsonElement, typeOfT: Type, context: JsonDeserializationContext): AggregateResultSet = {
      val record = new ArrayList[Any]()
      val json = jsonElement.asInstanceOf[JsonObject]
      val measures = JsonAdaptor.gson.fromJson(json.get("measures"), listOfDouble)
      val jsonArray = json.get("record").getAsJsonArray
      for (jsonItem <- jsonArray) {
        record.add(jsonItem.getAsString)
      }
      new AggregateResultSet(record, measures)
    }
  }
}

@SerialVersionUID(1L)
class AggregateResultSet extends Serializable {

  @BeanProperty
  var record: List[Any] = _

  @BeanProperty
  var measures: List[Any] = _

  def this(record: List[Any], measures: List[Any]) {
    this()
    this.record = record
    this.measures = measures
  }

  override def toString(): String = {
    val format = new DecimalFormat("#.000000")
    val duplicateMeasureList = new ArrayList[String]()
    for (i <- 0 until measures.size) {
      duplicateMeasureList.add(format.format(measures.get(i)))
    }
    val builder = new StringBuilder()
    builder.append("AggregateResultSet [measures=")
    builder.append(duplicateMeasureList)
    builder.append(", record=")
    builder.append(record)
    builder.append("]")
    builder.toString
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      return false
    }
    if (!(obj.isInstanceOf[AggregateResultSet])) {
      return false
    }
    val resultSet = obj.asInstanceOf[AggregateResultSet]
    if (resultSet == this) {
      return true
    }
    if (resultSet.toString == this.toString) {
      true
    } else {
      false
    }
  }

/*
Original Java:
package com.guavus.acume.rest.beans;

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

|**
 * Single Resulset 
 * 
 *
 *|
public class AggregateResultSet implements Serializable{
	

    |**
	 * 
	 *|
	private static final long serialVersionUID = 1L;

	|**
	 * Ordered List of Dimension values . The order is defined by
	 * {@link AggregateResponse.responseDimensions}
	 *|
	private List<Object> record ;
	
	|**
	 * Ordered List of Measure values . The order is defined by
	 * {@link AggregateResponse.responseMeasures}
	 *|
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

*/
}