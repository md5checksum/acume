package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import org.apache.spark.SparkContext
import com.guavus.crux.core.Fields
import com.guavus.crux.df.core.FieldDataType._
import org.apache.spark.rdd.RDD
import com.guavus.crux.core.TextDelimitedScheme
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD

class ParquetDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube) extends BasicDataLoader(acumeCacheContext, conf, cube) { 
  
  override def getRowSchemaRDD(sqlContext: SQLContext, baseDir: String, fields: Fields, datatypearray: Array[FieldDataType]): RDD[Row] = {
    
    sqlContext.parquetFile(baseDir)
  }
}




