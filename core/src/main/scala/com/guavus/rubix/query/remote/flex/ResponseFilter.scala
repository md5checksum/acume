package com.guavus.rubix.query.remote.flex

import java.io.Serializable
import java.util.Arrays
import scala.reflect.{BeanProperty, BooleanBeanProperty}
//remove if not needed
import scala.collection.JavaConversions._

class ResponseFilter extends Serializable {

  @BeanProperty
  var cubeProperty: String = _

  @BeanProperty
  var operator: String = _

  @BeanProperty
  var values: Array[Double] = _

  def toSql(): String = {
    var sql = " " + cubeProperty + " " + Operator.convertValue(Operator.withName(operator)).sqlSymbol + " "
    for (i <- 0 until values.length) {
      sql += values(i) + " and "
    }
    sql = sql.substring(0, sql.length - 4)
    sql
  }

  def this(cubeProperty: String, operator: String, values: Array[Double]) {
    this()
    this.cubeProperty = cubeProperty
    this.operator = operator
    this.values = values
  }

  override def toString(): String = {
    val buffer = new StringBuffer()
    buffer.append(cubeProperty + ", " + operator + ", ")
    for (value <- values) {
      buffer.append(value)
      buffer.append(",")
    }
    buffer.toString
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((cubeProperty == null)) 0 else cubeProperty.hashCode)
    result = prime * result + (if ((operator == null)) 0 else operator.hashCode)
    result = prime * result + Arrays.hashCode(values)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[ResponseFilter]
    if (cubeProperty == null) {
      if (other.cubeProperty != null) return false
    } else if (cubeProperty != other.cubeProperty) return false
    if (operator == null) {
      if (other.operator != null) return false
    } else if (operator != other.operator) return false
    if (!Arrays.equals(values, other.values)) return false
    true
  }

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

import java.io.Serializable;
import java.util.Arrays;

import com.guavus.rubix.filter.Operator;

public class ResponseFilter implements Serializable {
	private String cubeProperty;
	private String operator;
	private Double[] values;
	
	public ResponseFilter() {
	}
	
	
	public String toSql() {
		String sql = " " + cubeProperty + " " + Operator.valueOf(operator).getSqlSymbol() + " " ;
		for (int i = 0; i < values.length; i++) {
			sql+= values[i] + " and ";
		}
		sql = sql.substring(0, sql.length() - 4);
		return sql;
	}
	
	public ResponseFilter(String cubeProperty, String operator, Double... values) {
		super();
		this.cubeProperty = cubeProperty;
		this.operator = operator;
		this.values = values;
	}

	|**
	 * @return the measureName
	 *|
	public String getCubeProperty() {
		return cubeProperty;
	}

	|**
	 * @param measureName the measureName to set
	 *|
	public void setCubeProperty(String measureName) {
		this.cubeProperty = measureName;
	}

	|**
	 * @return the operator
	 *|
	public String getOperator() {
		return operator;
	}

	|**
	 * @param operator the operator to set
	 *|
	public void setOperator(String operator) {
		this.operator = operator;
	}

	|**
	 * @return the values
	 *|
	public Double[] getValues() {
		return values;
	}

	|**
	 * @param values the values to set
	 *|
	public void setValues(Double... values) {
		this.values = values;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(cubeProperty + ", " +  operator + ", ") ;
		for(Double value: values) {
			buffer.append(value);
			buffer.append(",");
		}
		return buffer.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((cubeProperty == null) ? 0 : cubeProperty.hashCode());
		result = prime * result
				+ ((operator == null) ? 0 : operator.hashCode());
		result = prime * result + Arrays.hashCode(values);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ResponseFilter other = (ResponseFilter) obj;
		if (cubeProperty == null) {
			if (other.cubeProperty != null)
				return false;
		} else if (!cubeProperty.equals(other.cubeProperty))
			return false;
		if (operator == null) {
			if (other.operator != null)
				return false;
		} else if (!operator.equals(other.operator))
			return false;
		if (!Arrays.equals(values, other.values))
			return false;
		return true;
	}
	
	
}

*/
}