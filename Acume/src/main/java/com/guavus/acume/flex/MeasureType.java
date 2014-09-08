package com.guavus.acume.flex;

public enum MeasureType {
	
	BASIC, DERIVED, DERIVED_ANNOTATED, DERIVED_DYNAMIC;
	
	/**
	 * Returns trues if the measure is derived runtime,
	 * i.e. measure is evaluated using measure processor.
	 * @return
	 */
	public boolean isDerivedRuntime() {
		if(this == DERIVED || this == DERIVED_DYNAMIC) {
			return true;
		}
		return false;
	}
	
	/**
	 * Returns true if the measure is derived,
	 * i.e. measure is evaluated using annotator
	 * or measure processor.
	 * @return
	 */
	public boolean isDerived() {
		if(this == DERIVED || this == DERIVED_ANNOTATED || this == DERIVED_DYNAMIC) {
			return true;
		}
		return false;
	}

}
