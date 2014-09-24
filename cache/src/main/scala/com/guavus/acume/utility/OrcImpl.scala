package com.guavus.acume.utility

import org.apache.hadoop.hive.ql.io.orc.Reader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.ql.io.orc.CompressionKind
import org.apache.hadoop.hive.ql.io.orc.Writer

object OrcImpl {

  def getReader(filename: String, config: Configuration): Reader = { 
    
    val fs = FileSystem.get(config)
    OrcFile.createReader(fs, new Path(filename))
  }
  
  def getWriter(str: String, filename: String, config: Configuration, compress:CompressionKind=CompressionKind.ZLIB, bufferSize:Int=524288, rowIndexStride:Int=10000, stripeSize:Long=268435456l) : Writer = { 
    
    val fs = FileSystem.get(config)
    val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(str)
    val inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
    OrcFile.createWriter(fs, new Path(filename), config, inspector, stripeSize, compress, bufferSize, rowIndexStride)
  }
  
}

