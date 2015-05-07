package com.guavus.acume.cache.core

import org.apache.spark.sql.SchemaRDD

case class AcumeTreeCacheValue(val dimensionTableName: String, val measuretableName: String, var measureschemardd: SchemaRDD)



