package com.guavus.acume.cache.workflow

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row


/**
 * @author archit.thakur
 *
 */

case class AcumeCacheResponse(schemaRDD: DataFrame, rowRDD: RDD[Row], metadata:MetaData)

case class MetaData(var totalRecords: Long = 0l, timestamps: List[Long])

