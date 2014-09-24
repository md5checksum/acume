package com.guavus.acume.core

trait NumericFunctionTrait {

  def init(): Double
//  def compute(oldValue: Double, newValue: Double, context: AggregationContext): Double
	
  @Deprecated
  def compute(oldValue: Double, newValue: Double): Double;
  
}