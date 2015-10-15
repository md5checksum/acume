package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SchemaRDD
import scala.collection.mutable.MutableList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame


/**
 * @author archit.thakur
 *
 */

case class AcumeCacheResponse(schemaRDD: DataFrame, rowRDD: RDD[Row], metadata:MetaData)



case class MetaData(var totalRecords: Long = 0l, timestamps: List[Long])
//case class AggregateMetaData(rows: Long) extends MetaData
//case class TimeSeriesMetaData(timestamps: List[Long]) extends MetaData

