package com.guavus.common_util.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.fs.Path

object OrcReader {

  def readORCWrittenByORCWriteModule{
    
    val config = new Configuration
    val fs = FileSystem.get(config)
    val reader = OrcFile.createReader(fs, new Path("/Users/archit.thakur/Documents/Code_Crux.Git_Scala/orc.txt"))
    reader
  }
  
  def main(args: Array[String]) = { 
    
    val config = new Configuration
    val fs = FileSystem.get(config)
    val reader = OrcFile.createReader(fs, new Path("/Users/archit.thakur/Documents/Code_Custom_SparkCache_Scala/SearchPRI_InteractionIngressDimension.orc"))
    val rowSet = reader.rows()
    var previous = null.asInstanceOf[Object]
    while(rowSet.hasNext){
      previous = rowSet.next(previous)
      println(previous)
    }
    rowSet.close()
  }
}
