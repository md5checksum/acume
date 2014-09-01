package com.guavus.equinox.filter;

public enum FilterExceptions {
	FILTER_ALREADY_EXISTS("filter already exists"),
	FILTER_NOT_SAVED("could not save filter"),
	NO_FILTER("no filter exists"),
	MULTIPLE_FILTERS("more than one filter exists"),
	FILTER_NOT_UPDATED("could not update filter"),
	FILTER_NOT_DELETED("could not delete filter");
	
	private String name;
	
	private FilterExceptions(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
