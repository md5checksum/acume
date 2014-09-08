package com.guavus.acume.utility

import java.io.OutputStream
import java.io.InputStream

trait RWTrait {

  def getOutputStream(output: String, append: Boolean): OutputStream
  def getInputStream(input: String): InputStream
  def exists(input: String): Boolean
  def findFile(path: String, prefix: String): Set[String]
  def listFile(dir: String): Set[String]
  def listFolder(dir: String): Set[String]
  def createEmptyFile(filename: String): Boolean
}



