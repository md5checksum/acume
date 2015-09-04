package com.guavus.acume.core

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

object NdfTimeSeriesCompleter {

  def getProperties = {
    val ndfConfigFile = "/opt/tms/acumeDF-ca4.0/WEB-INF/classes/ndf.properties"
    val prop = new java.util.Properties()
    try {
      prop.load(new java.io.FileInputStream(ndfConfigFile))
    } catch {
      case ex: Exception => throw new RuntimeException("NdfTimeSeriesCompleter: failed to read solution config file: " + ndfConfigFile, ex)
    }
    println(s"NdfTimeSeriesCompleter: successfully read config file: $ndfConfigFile")
    prop
  }

  val prop = getProperties
  val hourlyPointCount = prop.getProperty("hourlyPointCount").toInt

}

case class NdfTimeSeriesCompleter {

  def completeTimeSeries(cacheResponse: com.guavus.acume.cache.workflow.AcumeCacheResponse,
    queryBuilderService: com.guavus.qb.services.IQueryBuilderService, inputQuery: String): (Array[org.apache.spark.sql.Row], List[Long]) = {

    val schemaRdd = cacheResponse.schemaRDD
    val schema = schemaRdd.schema

    var rows = schemaRdd.collect
    var tsIndex = schema.fieldNames.indexOf("ts")
    if (tsIndex == -1) return (rows, Nil)

    val sortedRows = rows.sortBy(row => row(tsIndex).toString)
    var timestamps = if (cacheResponse.metadata.timestamps != Nil) cacheResponse.metadata.timestamps else sortedRows.toList.map(row => row(tsIndex).asInstanceOf[Long]).distinct
    val (startTime, endTime, isDailyCall) = getTimeRangeAndCallType(inputQuery)

    val tsToDataMap = HashMap[Long, Boolean]()
    val timeseries = startTime until endTime by (if (isDailyCall) 86400 else 3600)
    timeseries.foreach(ts => tsToDataMap.put(ts, false))
    timestamps.foreach(ts => tsToDataMap.put(ts, true))
    val hasAllRows = tsToDataMap.forall(entry => entry._2 == true)
    if (hasAllRows) return (rows, timestamps)

    val defaultValues = schema.fieldNames.map(field => queryBuilderService.getDefaultValueForField(field).asInstanceOf[Any]).toArray
    val missingRows = for ((ts, dataPresent) <- tsToDataMap) yield {
      if (!dataPresent) {
        defaultValues(tsIndex) = ts.asInstanceOf[Any]
        new org.apache.spark.sql.catalyst.expressions.GenericRow(defaultValues.clone)
      } else {
        Nil.asInstanceOf[org.apache.spark.sql.catalyst.expressions.GenericRow]
      }
    }
    
    (rows ++ missingRows.filter(row => row != Nil), timeseries.toList)
  }

  def getTimeRangeAndCallType(inputQuery: String): (Long, Long, Boolean) = {
    val startTime = inputQuery.split("startTime = ")(1).split("""[ )]""")(0).toInt
    val endTime = inputQuery.split("endTime = ")(1).split("""[ )]""")(0).toInt
    val timeRange = endTime - startTime

    val dataEndTime = inputQuery.split("dataEndTime = ")(1).split("""[ )]""")(0).replace("\"", "").toInt
    val dataStartTime = dataEndTime - NdfTimeSeriesCompleter.hourlyPointCount * 3600

    if (startTime < dataStartTime) {
      // case: query timerange is not within cached hourly points timerange
      // this means that the start and end time have to be aligned at day level
      (startTime, endTime, true)
    } else {
      (startTime, endTime, false)
    }
  }

}
