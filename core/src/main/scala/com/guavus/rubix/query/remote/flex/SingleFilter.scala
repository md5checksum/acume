package com.guavus.rubix.query.remote.flex

import java.io.Serializable
import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}

@SerialVersionUID(-9174188557495936312L)
@XmlRootElement
class SingleFilter extends Serializable {

  @BeanProperty
  var dimension: String = _

  @BeanProperty
  var value: String = _

  @BeanProperty
  var condition: String = _

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((condition == null)) 0 else condition.hashCode)
    result = prime * result + (if ((dimension == null)) 0 else dimension.hashCode)
    result = prime * result + (if ((value == null)) 0 else value.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[SingleFilter]
    if (condition == null) {
      if (other.condition != null) return false
    } else if (condition != other.condition) return false
    if (dimension == null) {
      if (other.dimension != null) return false
    } else if (dimension != other.dimension) return false
    if (value == null) {
      if (other.value != null) return false
    } else if (value != other.value) return false
    true
  }
  
  def toSql(): String = {
    if (this.condition == "EQUAL")
      " " + dimension +" = "+value + " "
    else
      " " + dimension +" != "+ value + " "
  }

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SingleFilter implements Serializable {

	private static final long serialVersionUID = -9174188557495936312L;

	private String dimension;
	private String value;
	|**
	 * Should be one of "EQUAL, NOT_EQUAL"
	 *|
	private String condition;
	
	public String getDimension() {
		return dimension;
	}
	public void setDimension(String dimension) {
		this.dimension = dimension;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((condition == null) ? 0 : condition.hashCode());
		result = prime * result
				+ ((dimension == null) ? 0 : dimension.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		SingleFilter other = (SingleFilter) obj;
		if (condition == null) {
			if (other.condition != null)
				return false;
		} else if (!condition.equals(other.condition))
			return false;
		if (dimension == null) {
			if (other.dimension != null)
				return false;
		} else if (!dimension.equals(other.dimension))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}	
	
	
}

*/
}