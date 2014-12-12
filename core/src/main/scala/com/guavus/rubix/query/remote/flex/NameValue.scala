package com.guavus.rubix.query.remote.flex

import java.io.Serializable
import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}

@XmlRootElement
class NameValue extends Serializable {

  @BeanProperty
  var name: String = _

  @BeanProperty
  var value: String = _

  def this(name: String, value: String) {
    this()
    this.name = name
    this.value = value
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((name == null)) 0 else name.hashCode)
    result = prime * result + (if ((value == null)) 0 else value.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[NameValue]
    if (name == null) {
      if (other.name != null) return false
    } else if (name != other.name) return false
    if (value == null) {
      if (other.value != null) return false
    } else if (value != other.value) return false
    true
  }

  def toSql(): String = " " + name + " = '" + value + "' "

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class NameValue implements Serializable {

	private String name;
	
	private String value;
	
	public NameValue() {
		// TODO Auto-generated constructor stub
	}
	
	public NameValue(String name, String value) {
		this.name = name;
		this.value = value;
	}

	|**
	 * @return the name
	 *|
	public String getName() {
		return name;
	}

	|**
	 * @param name the name to set
	 *|
	public void setName(String name) {
		this.name = name;
	}

	|**
	 * @return the value
	 *|
	public String getValue() {
		return value;
	}

	|**
	 * @param value the value to set
	 *|
	public void setValue(String value) {
		this.value = value;
	}

	|* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 *|
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	|* (non-Javadoc)
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
		NameValue other = (NameValue) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	
	
	public String toSql() {
		return " " + name + " = '" + value  + "' ";  
	}
	
}

*/
}