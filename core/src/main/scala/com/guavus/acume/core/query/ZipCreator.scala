package com.guavus.acume.core.query;

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.util.List
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import scala.collection.JavaConversions._

import com.guavus.acume.cache.utility.Utility

object ZipCreator {
    
    def zip(inputFiles: List[File], outputFileName: String) {
    val zipFile = new File(outputFileName)
    var outStream: ZipOutputStream = null
    var inStream: FileInputStream = null
    try {
      outStream = new ZipOutputStream(new FileOutputStream(zipFile))
      for (file <- inputFiles) {
        inStream = new FileInputStream(file)
        outStream.putNextEntry(new ZipEntry(file.getName))
        Utility.copyStream(inStream, outStream)
        outStream.closeEntry()
        inStream.close()
      }
    } catch {
      case e: IOException => throw new RuntimeException("Caught exception creating zip file from input files", 
        e)
    } finally {
      Utility.closeStream(outStream)
      Utility.closeStream(inStream)
    }
  }
}
