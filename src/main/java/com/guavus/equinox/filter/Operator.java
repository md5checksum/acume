package com.guavus.equinox.filter;

import com.guavus.equinox.core.MeasureRange;

public enum Operator {
	GREATER_THAN(">") {
		boolean eval(Double record, Double... filters) {
			return (record.compareTo(filters[0]) > 0);
		}

		@Override
		boolean isValid(MeasureRange mRange, Double... filters) {
			return filters[0].compareTo(mRange.getMinValue()) >= 0;
		}
		
	},
	GREATER_THAN_EQUAL(">=") {
		boolean eval(Double record, Double... filters) {
			return (record.compareTo(filters[0]) >= 0);
		}

		@Override
		boolean isValid(MeasureRange mRange, Double... filters) {
			return filters[0].compareTo(mRange.getMinValue()) >= 0;
		}
	},
	LESS_THAN("<") {
		boolean eval(Double record, Double... filters) {
			return (record.compareTo(filters[0]) < 0);
		}
	},
	
	LESS_THAN_EQUAL("<=") {
		boolean eval(Double record, Double... filters) {
			return (record.compareTo(filters[0]) <= 0);
		}
	},
	BETWEEN("between") {
		boolean eval(Double record, Double... filters) {
			return (record.compareTo(filters[0]) > 0)?
					record.compareTo(filters[1]) < 0 : false;
		}
		
		boolean isValid(MeasureRange mRange, Double... filters) {
	        return GREATER_THAN.isValid(mRange, filters);
	    }
	},
	
	BETWEEN_INCLUSIVE("between") {
		boolean eval(Double record, Double... filters) {
			return (record.compareTo(filters[0]) >= 0)?
					record.compareTo(filters[1]) <= 0 : false;
		}
		
		boolean isValid(MeasureRange mRange, Double... filters) {
	        return GREATER_THAN_EQUAL.isValid(mRange, filters);
	    }
	},
	EQUAL("=") {
		boolean eval(Double record, Double... filters) {
			return record.equals(filters[0]);
		}
	};
	
	private String sqlSymbol;
	abstract boolean eval(Double record, Double... filters);
	boolean isValid(MeasureRange mRange, Double... filters) {
		return true;
	}
	
	private Operator(String sqlSymbol) {
		this.sqlSymbol = sqlSymbol;
	}
	
	public String getSqlSymbol() {
		return sqlSymbol;
	}
	
}
