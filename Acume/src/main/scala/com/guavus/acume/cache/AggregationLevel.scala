package com.guavus.acume.cache

trait AggregationLevel
case class HOURLY extends AggregationLevel
case class WEEKLY extends AggregationLevel
case class MONTHLY extends AggregationLevel
case class YEARLY extends AggregationLevel


trait BIN_INTERVAL
case class HOURLY_BIN_INTERVAL extends BIN_INTERVAL