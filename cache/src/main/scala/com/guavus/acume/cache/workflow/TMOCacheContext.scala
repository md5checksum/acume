package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.QLType
import org.apache.spark.sql.SchemaRDD
import java.util.regex.Pattern
import org.apache.spark.rdd.PartitionPruningRDD

class TMOCacheContext(override val sqlContext: SQLContext, override val conf: AcumeCacheConf) extends AcumeCacheContext(sqlContext, conf) {

  override def execute(qltype: QLType.QLType, updatedsql: String): SchemaRDD = {
    val response = AcumeCacheContext.ACQL(qltype, sqlContext)(updatedsql)
    
    val regex = "(subscr)\\s+(=)\\s+\\d+";
    val pattern = Pattern.compile(regex)
    
    val matcher = pattern.matcher(updatedsql)
    if(matcher.find) {
      val str = matcher.group(0)
      val regex = "\\d+";
      val pattern = Pattern.compile(regex)
      val matcher2 = pattern.matcher(str)
      if(matcher2.find) {
        val id = matcher2.group(0).toLong
        
        def func(partition: Int) : Boolean = {
          //TODO: MAke configuration driven
          if(partition % conf.getInt("acume.cache.partitions") == TMOPartitioner.getPartition(id)) 
            return true
          false
        }
    
        val rdd = new PartitionPruningRDD(response, func)
        return sqlContext.applySchema(rdd, response.schema)
      }
      
    }
    
    return response
  }
  
}