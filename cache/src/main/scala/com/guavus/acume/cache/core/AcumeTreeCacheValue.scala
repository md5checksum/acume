package com.guavus.acume.cache.core

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.scheduler.cluster.PartitionDistributorRDD

class AcumeTreeCacheValue(val dimensionTableName: String, val measuretableName: String, var measureschemardd: SchemaRDD) {
  
  def this(sqlContext: SQLContext, dimensionTableName: String, measuretableName: String, measureschemardd: SchemaRDD) {
    
    this(dimensionTableName, measuretableName, null)
    val numPartitions = measureschemardd.partitions.size
    val partitionDistributorRdd = PartitionDistributorRDD.getInstance(sqlContext.sparkContext, numPartitions)
    val zippedSchemaRdd = sqlContext.applySchema(partitionDistributorRdd.zipRdd(measureschemardd), measureschemardd.schema)
 
    zippedSchemaRdd.registerTempTable(measuretableName)
    this.measureschemardd = zippedSchemaRdd
    sqlContext.cacheTable(measuretableName)
  }
 
}




