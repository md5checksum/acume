package com.guavus.common_util.orc

import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.ql.io.orc.CompressionKind
import com.guavus.common_util.GenericWriter

object OrcWriter extends GenericWriter {

  val rowIndexStride = 10000
  val stripeSize = 268435456l
  val bufferSize = 524288
  val compress = CompressionKind.ZLIB
  def main(args: Array[String]) = {

    val config = new Configuration
    val fs = FileSystem.get(config)
    val objInspector = ObjectInspectorFactory.getReflectionObjectInspector(classOf[String], ObjectInspectorFactory.ObjectInspectorOptions.JAVA)
    val writer = OrcFile.createWriter(fs, new Path("/Users/archit.thakur/Documents/Code_Crux.Git_Scala/orc.txt"), config, objInspector, stripeSize, compress, bufferSize, rowIndexStride)
    writer.addRow(new String("Written by OrcWriter."))
    writer.close
  }
  
}