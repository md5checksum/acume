package com.guavus.equinox.flex;

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
