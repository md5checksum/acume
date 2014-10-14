package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SchemaRDD

import scala.collection.mutable.MutableList


case class AcumeCacheResponse(schemaRDD: SchemaRDD, metadata:MetaData)
case class MetaData(timestamps: List[Long])


