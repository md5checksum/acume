package com.guavus.acume.rest.beans

import scala.reflect.BeanProperty



object MeasureRange extends Enumeration {

  val DEFAULT = new MeasureRange(0, Double.PositiveInfinity)

  val GROWTH = new MeasureRange(Double.NegativeInfinity, Double.PositiveInfinity)

  case class MeasureRange(minValue: Double, maxValue: Double) extends Val
  
  implicit def convertValue(v: Value): MeasureRange = v.asInstanceOf[MeasureRange]

/*
Original Java:
package com.guavus.rubix.core;

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

*/
}