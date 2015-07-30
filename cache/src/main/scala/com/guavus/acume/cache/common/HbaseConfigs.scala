package com.guavus.acume.cache.common

case class HbaseConfigs(tableName: String, datasourceName: String, cubeName: String, primaryKeys : Array[String], columnMappings : Map[String, String])
