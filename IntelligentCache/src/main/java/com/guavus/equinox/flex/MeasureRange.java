package com.guavus.equinox.flex;

public enum MeasureRange {
	
	DEFAULT(0, Double.POSITIVE_INFINITY),
	GROWTH(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
	
	private double minValue, maxValue;

	private MeasureRange(double minValue, double maxValue) {
		this.minValue = minValue;
		this.maxValue = maxValue;
	}

	public double getMinValue() {
		return minValue;
	}

	public double getMaxValue() {
		return maxValue;
	}
}
