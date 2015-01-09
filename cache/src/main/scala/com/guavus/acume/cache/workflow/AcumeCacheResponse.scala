package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SchemaRDD

import scala.collection.mutable.MutableList


/**
 * @author archit.thakur
 *
 */

case class AcumeCacheResponse(schemaRDD: SchemaRDD, metadata:MetaData)



case class MetaData(var totalRecords: Long, timestamps: List[Long])
//case class AggregateMetaData(rows: Long) extends MetaData
//case class TimeSeriesMetaData(timestamps: List[Long]) extends MetaData

