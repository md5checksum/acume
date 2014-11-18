package com.guavus.acume.cache.disk.utility

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.crux.core.Fields
import com.guavus.crux.core.TextDelimitedScheme
import com.guavus.crux.df.core.FieldDataType.FieldDataType
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.core.AcumeCache

class TextDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, acumeCache: AcumeCache) extends BasicDataLoader(acumeCacheContext, conf, acumeCache) { 
  
  override def getRowSchemaRDD(sqlContext: SQLContext, baseDir: String, fields: Fields, datatypearray: Array[FieldDataType]): RDD[Row] = {
    
    new TextDelimitedScheme(fields, "\t", datatypearray)._getRdd(baseDir, sqlContext.sparkContext).map(x => Row.fromSeq(x.getValueArray.toSeq))
  }
}