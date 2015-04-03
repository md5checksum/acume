package com.guavus.acume.cache.core

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.scheduler.cluster.PartitionDistributorRDD

class AcumeTreeCacheValue(val dimensionTableName: String, val measuretableName: String, val measureschemardd: SchemaRDD) {
  
  def this(sqlContext: SQLContext, dimensionTableName: String, measuretableName: String, measureschemardd: SchemaRDD) {
    
    this(dimensionTableName, measuretableName, measureschemardd)
    val numPartitions = measureschemardd.partitions.size
    val partitionDistributorRdd = PartitionDistributorRDD.getInstance(sqlContext.sparkContext, numPartitions)
    val zippedSchemaRdd = sqlContext.applySchema(partitionDistributorRdd.zipRdd(measureschemardd), measureschemardd.schema)
 
    zippedSchemaRdd.registerTempTable(measuretableName)
    sqlContext.cacheTable(measuretableName)
  }
 
}




