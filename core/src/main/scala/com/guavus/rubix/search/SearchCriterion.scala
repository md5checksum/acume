package com.guavus.rubix.search
import java.io.Serializable
import java.util.ArrayList
import com.google.gson.Gson
import com.google.gson.TypeAdapter
import com.google.gson.internal.StringMap
import com.google.gson.internal.bind.ObjectTypeAdapter
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken.BEGIN_ARRAY
import com.google.gson.stream.JsonToken.BEGIN_OBJECT
import com.google.gson.stream.JsonToken.BOOLEAN
import com.google.gson.stream.JsonToken.STRING
import com.google.gson.stream.JsonToken.NUMBER
import com.google.gson.stream.JsonToken.NULL
import com.google.gson.stream.JsonToken
import com.google.gson.stream.JsonWriter
import SearchCriterion._
import scala.reflect.BeanProperty

object SearchCriterion {

  private val DIMENSION_NAME = "dimensionName"

  private val OPERATOR = "operator"

  private val VALUE = "value"


  class SearchCriterionJsonAdapter extends TypeAdapter[SearchCriterion] {

    private var gson: Gson = new Gson()

    override def read(in: JsonReader): SearchCriterion = {
      val criterion = new SearchCriterion()
      in.beginObject()
      while (in.hasNext) {
        if (in.peek() == JsonToken.NULL) {
          in.nextNull()
          //continue
        }
        val name = in.nextName()
        if (name == DIMENSION_NAME) {
          criterion.setDimensionName(in.nextString())
        } else if (name == OPERATOR) {
          criterion.setOperator(in.nextString())
        } else if (name == VALUE) {
          criterion.setValue(readObject(in))
        }
      }
      in.endObject()
      criterion
    }

    private def readObject(in: JsonReader): AnyRef = {
      val token = in.peek()
      token match {
        case BEGIN_ARRAY => 
          var list = new ArrayList[Any]()
          in.beginArray()
          while (in.hasNext) {
            list.add(readObject(in))
          }
          in.endArray()
          return list

        case BEGIN_OBJECT => 
          var map = new StringMap[Any]()
          in.beginObject()
          while (in.hasNext) {
            map.put(in.nextName(), readObject(in))
          }
          in.endObject()
          return map

        case STRING => return in.nextString()
        case NUMBER => try {
          return new Integer(in.nextInt())
        } catch {
          case ex: NumberFormatException => return new java.lang.Double(in.nextDouble())
        }
        case BOOLEAN => return new java.lang.Boolean(in.nextBoolean())
        case NULL => 
          in.nextNull()
          return null

      }
      throw new IllegalStateException()
    }

    override def write(out: JsonWriter, value: SearchCriterion) {
      out.beginObject()
      if (value.getDimensionName != null) {
        out.name(DIMENSION_NAME).value(value.getDimensionName)
      } else {
        out.name(DIMENSION_NAME).nullValue()
      }
      if (value.getOperator != null) {
        out.name(OPERATOR).value(value.getOperator)
      } else {
        out.name(OPERATOR).nullValue()
      }
      out.name(VALUE)
      val `object` = value.getValue
      if (`object` == null) {
        out.nullValue()
      } else {
        val typeAdapter = gson.getAdapter(`object`.getClass).asInstanceOf[TypeAdapter[Any]]
        if (typeAdapter.isInstanceOf[ObjectTypeAdapter]) {
          out.beginObject()
          out.endObject()
          return
        }
        typeAdapter.write(out, `object`)
      }
      out.endObject()
    }
  }
}

@SerialVersionUID(-7127439724969199902L)
class SearchCriterion(dimension: String, condition: String, @BeanProperty var value: AnyRef) extends Serializable {

  @BeanProperty
  var dimensionName: String = dimension

  @BeanProperty
  var operator: String = condition

  def this() = this(null, null , null)

  def getDimension(): String = dimensionName

  override def toString(): String = {
    val builder = new StringBuilder()
    builder.append("SearchCriterion [" + DIMENSION_NAME + "=")
    builder.append(dimensionName)
    builder.append(", " + OPERATOR + "=")
    builder.append(operator)
    builder.append(", " + VALUE + "=")
    builder.append(value)
    builder.append("]")
    builder.toString
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((dimensionName == null)) 0 else dimensionName.hashCode)
    result = prime * result + (if ((operator == null)) 0 else operator.hashCode)
    result = prime * result + (if ((value == null)) 0 else value.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[SearchCriterion]
    if (dimensionName == null) {
      if (other.dimensionName != null) return false
    } else if (dimensionName != other.dimensionName) return false
    if (operator == null) {
      if (other.operator != null) return false
    } else if (operator != other.operator) return false
    if (value == null) {
      if (other.value != null) return false
    } else if (value != other.value) return false
    true
  }

/*
Original Java:
package com.guavus.rubix.search;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.StringMap;
import com.google.gson.internal.bind.ObjectTypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

public class SearchCriterion implements Serializable {
	|**
	 * 
	 *|
	private static final long serialVersionUID = -7127439724969199902L;
	private String dimensionName;
	private String operator;
	// value can be String or Number
	private Object value;
	// While matching dimensions for search cube selection, whether the
	// dimension in this criterion can be ignored or not
	private boolean ignorableForSearchCubeSelection;

	private static final String DIMENSION_NAME = "dimensionName";
	private static final String OPERATOR = "operator";
	private static final String VALUE = "value";
	private static final String IGNORABLE_FOR_SEARCH_CUBE_SELECTION = "ignorableForSearchCubeSelection";
	
	public SearchCriterion(String dimension, String condition, Object value) {
		this(dimension, condition, value, false);
	}

	public SearchCriterion(String dimension, String condition, Object value,
			boolean ignorableForSearchCubeSelection) {
		this.dimensionName = dimension;
		this.operator = condition;
		this.value = value;
		this.setIgnorableForSearchCubeSelection(ignorableForSearchCubeSelection);

	}

	public SearchCriterion() {

	}

	public String getDimension() {
		return dimensionName;
	}

	public String getOperator() {
		return operator;
	}

	public String getDimensionName() {
		return dimensionName;
	}

	public void setDimensionName(String dimensionName) {
		this.dimensionName = dimensionName;
	}
	
	public void setOperator(String operator) {
		this.operator = operator;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public boolean isIgnorableForSearchCubeSelection() {
		return ignorableForSearchCubeSelection;
	}

	public void setIgnorableForSearchCubeSelection(
			boolean ignorableForSearchCubeSelection) {
		this.ignorableForSearchCubeSelection = ignorableForSearchCubeSelection;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SearchCriterion [" + DIMENSION_NAME + "=");
		builder.append(dimensionName);
		builder.append(", " + OPERATOR + "=");
		builder.append(operator);
		builder.append(", " + VALUE + "=");
		builder.append(value);
		builder.append("]");
		return builder.toString();
	}

	public Object getValue() {
		return value;
	}

	|*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 *|
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dimensionName == null) ? 0 : dimensionName.hashCode());
		result = prime * result
				+ ((operator == null) ? 0 : operator.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	|*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 *|
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SearchCriterion other = (SearchCriterion) obj;
		if (dimensionName == null) {
			if (other.dimensionName != null)
				return false;
		} else if (!dimensionName.equals(other.dimensionName))
			return false;
		if (operator == null) {
			if (other.operator != null)
				return false;
		} else if (!operator.equals(other.operator))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	|**
	 * Json Adapter for SearchCriterion class. This class converts JSON format {'dimensionName':'INGRESS_POP_ID','operator':'IN','value':[5.0,6.0,7.0,8.0]}
	 * to SearchCriterion object via read method and vice-versa via write method.
	 * 
	 * @author bhupesh.goel
	 * 
	 *|
	public static class SearchCriterionJsonAdapter extends
			TypeAdapter<SearchCriterion> {

		private Gson gson;

		|**
		 * public Constructor.
		 *|
		public SearchCriterionJsonAdapter() {
			gson = new Gson();
		}

		|*
		 * (non-Javadoc)
		 * 
		 * @see
		 * com.google.gson.TypeAdapter#read(com.google.gson.stream.JsonReader)
		 *|
		@Override
		public SearchCriterion read(JsonReader in) throws IOException {
			SearchCriterion criterion = new SearchCriterion();
			in.beginObject();
			while (in.hasNext()) {
				if (in.peek() == JsonToken.NULL) {
					in.nextNull();
					continue;
				}
				String name = in.nextName();
				if (name.equals(DIMENSION_NAME)) {
					criterion.setDimensionName(in.nextString());
				} else if (name.equals(OPERATOR)) {
					criterion.setOperator(in.nextString());
				} else if (name.equals(VALUE)) {
					criterion.setValue(readObject(in));
				} else if (name.equals(IGNORABLE_FOR_SEARCH_CUBE_SELECTION)) {
					criterion.setIgnorableForSearchCubeSelection(in.nextBoolean());
				}
			}
			in.endObject();
			return criterion;
		}

		|**
		 * Method to read JSON generic object String.
		 * @param in JsonReader.
		 * @return constructed object from JSONReader.
		 * @throws IOException
		 *|
		private Object readObject(JsonReader in) throws IOException {
			JsonToken token = in.peek();
			switch (token) {
			case BEGIN_ARRAY:
				List<Object> list = new ArrayList<Object>();
				in.beginArray();
				while (in.hasNext()) {
					list.add(readObject(in));
				}
				in.endArray();
				return list;

			case BEGIN_OBJECT:
				Map<String, Object> map = new StringMap<Object>();
				in.beginObject();
				while (in.hasNext()) {
					map.put(in.nextName(), readObject(in));
				}
				in.endObject();
				return map;

			case STRING:
				return in.nextString();

			case NUMBER:
				try {
					return in.nextInt();
				} catch (NumberFormatException ex) {
					return in.nextDouble();
				}
			case BOOLEAN:
				return in.nextBoolean();

			case NULL:
				in.nextNull();
				return null;
			}
			throw new IllegalStateException();

		}

		|*
		 * (non-Javadoc)
		 * 
		 * @see
		 * com.google.gson.TypeAdapter#write(com.google.gson.stream.JsonWriter,
		 * java.lang.Object)
		 *|
		@Override
		public void write(JsonWriter out, SearchCriterion value)
				throws IOException {
			out.beginObject();
			if (value.getDimensionName() != null) {
				out.name(DIMENSION_NAME).value(value.getDimensionName());
			} else {
				out.name(DIMENSION_NAME).nullValue();
			}
			if (value.getOperator() != null) {
				out.name(OPERATOR).value(value.getOperator());
			} else {
				out.name(OPERATOR).nullValue();
			}
			out.name(IGNORABLE_FOR_SEARCH_CUBE_SELECTION).value(value.isIgnorableForSearchCubeSelection());
			
			out.name(VALUE);
			Object object = value.getValue();
			if (object == null) {
				out.nullValue();
			} else {
				TypeAdapter<Object> typeAdapter = (TypeAdapter<Object>) gson
						.getAdapter(object.getClass());
				if (typeAdapter instanceof ObjectTypeAdapter) {
					out.beginObject();
					out.endObject();
					return;
				}
				typeAdapter.write(out, object);
			}
			out.endObject();
		}
	}
}

*/
}