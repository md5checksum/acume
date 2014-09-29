package com.guavus.acume.cache.utility

import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions
import org.apache.hadoop.hive.ql.io.orc.Reader
import org.apache.hadoop.hive.ql.io.orc.Writer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.CompressionKind

trait OrcTrait {

  def setSchema(str: String): Unit
  def getReader(filename: String, config: Configuration): Reader
  def getWriter(filename: String, config: Configuration): Writer
  def set(rowIndexStride: Int = 10000, stripeSize: Long = 268435456l, bufferSize: Int = 524288, compress: CompressionKind = CompressionKind.ZLIB)
  def close(): Unit;
}