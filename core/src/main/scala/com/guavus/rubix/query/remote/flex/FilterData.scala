package com.guavus.rubix.query.remote.flex

import java.io.Serializable
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import java.util.ArrayList

@SerialVersionUID(-4945096459581562055L)
@XmlRootElement
class FilterData extends Serializable {

  @BeanProperty
  var filters: ArrayList[SingleFilter] = _

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((filters == null)) 0 else filters.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[FilterData]
    if (filters == null) {
      if (other.filters != null) return false
    } else if (filters != other.filters) return false
    true
  }

/*
Original Java:
package com.guavus.acume.rest.beans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class FilterData implements Serializable {

	private static final long serialVersionUID = -4945096459581562055L;

	private List<SingleFilter> filters;

	@XmlElement(type=ArrayList.class)
	public List<SingleFilter> getFilters() {
		return filters;
	}

	public void setFilters(List<SingleFilter> filters) {
		this.filters = filters;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((filters == null) ? 0 : filters.hashCode());
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
		FilterData other = (FilterData) obj;
		if (filters == null) {
			if (other.filters != null)
				return false;
		} else if (!filters.equals(other.filters))
			return false;
		return true;
	}
	
	

}

*/
}