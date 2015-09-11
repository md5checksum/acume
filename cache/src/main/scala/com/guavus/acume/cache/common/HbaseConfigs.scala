package com.guavus.acume.cache.common

case class HbaseConfigs(nameSpace: String, tableName: String, datasourceName: String, cubeName: String, primaryKeys : Array[String], columnMappings : Map[String, String])
