package com.guavus.rubix.query.remote.flex

import java.io.Serializable

import java.util.ArrayList
import scala.reflect.BeanProperty

import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

@SerialVersionUID(-2622636580887371088L)
@XmlRootElement
class MultiFilter extends Serializable {

  var singleFilters: ArrayList[MeasureSingleFilter] = _

  @BeanProperty
  var measure: String = _

  def this(measure: String, singleFilters: ArrayList[MeasureSingleFilter]) {
    this()
    this.measure = measure
    this.singleFilters = singleFilters
  }

  @XmlElement(`type` = classOf[ArrayList[MeasureSingleFilter]])
  def getSingleFilters(): ArrayList[MeasureSingleFilter] = singleFilters

  def setSingleFilters(singleFilters: ArrayList[MeasureSingleFilter]) {
    this.singleFilters = singleFilters
  }
}

@SerialVersionUID(-4945096459581562055L)
@XmlRootElement
class MeasureFilterData extends Serializable {

  @BeanProperty
  var filters: ArrayList[MultiFilter] = _

  /*
Original Java:
package com.guavus.rubix.query.remote.flex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class MeasureFilterData implements Serializable {

	private static final long serialVersionUID = -4945096459581562055L;

	private List<MultiFilter> filters;
	
	@XmlRootElement
	public class MultiFilter implements Serializable {
		
		private static final long serialVersionUID = -2622636580887371088L;
		List<MeasureSingleFilter> singleFilters;
		private String measure;
		
		public MultiFilter() {
		}
		
		public MultiFilter(String measure, List<MeasureSingleFilter> singleFilters) {
			this.measure = measure;
			this.singleFilters = singleFilters;
		}
		
		@XmlElement(type=ArrayList.class)
		public List<MeasureSingleFilter> getSingleFilters() {
			return singleFilters;
		}

		public void setSingleFilters(List<MeasureSingleFilter> singleFilters) {
			this.singleFilters = singleFilters;
		}

		public String getMeasure() {
			return measure;
		}

		public void setMeasure(String measure) {
			this.measure = measure;
		}
	}

	@XmlElement(type=ArrayList.class)
	public List<MultiFilter> getFilters() {
		return filters;
	}

	public void setFilters(List<MultiFilter> filters) {
		this.filters = filters;
	}

}

*/
}