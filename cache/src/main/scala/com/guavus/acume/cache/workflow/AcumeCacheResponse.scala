package com.guavus.acume.cache.workflow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

/**
 * @author archit.thakur
 *
 */

case class AcumeCacheResponse(var schemaRDD: DataFrame, var rowRDD: RDD[Row], var metadata:MetaData)

case class MetaData(var totalRecords: Long = 0l, var timestamps: List[Long])

