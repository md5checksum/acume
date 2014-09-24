package com.guavus.acume.utility

import org.apache.spark.SparkContext
import java.io.File

object FileWrapper {

  private def listFiles(rootDirectory: String): Set[String] = { 
    
    val listSet = Set[String]()
    try{
		val listFile: Array[File] = new File(rootDirectory).listFiles()
		if(listFile == null || listFile.length == 0) 
		  listSet
		else  
		    listFile.filter(_.isFile()).map(_.getAbsolutePath()).toSet
    } catch {
      case iex: Exception => throw iex
      listSet
    }
  }
  
  def listFolders(rootDirectory: String, level: Int = -1): List[String] = { 
    
    /*
     * level defines the level to go into.
     * if level = -1 ... go till leaf node.
     */
    val listSet = List[String]()
    try{
		val listFile: Array[File] = new File(rootDirectory).listFiles()
		if(level == 0 || listFile == null || listFile.length == 0) 
		  listSet
		else { 
		  
		  val list = listFile.toList.filter(x => (!x.isFile() && !x.isHidden())).map(_.getAbsolutePath())//.map(listFolders(_, -1))
		  list.++(list.map(listFolders(_, level - 1)).flatten)
		}  
    } catch {
      case iex: Exception => throw iex
      listSet
    }
  }
  
  def commaSeparatedFile(rootDirectory: String): String = { 
    
    listFiles(rootDirectory).reduce(_+","+_)
  }
  
  def commaSeparatedFile(rootDirectory: String*): String = { 

    var combined = commaSeparatedFile(rootDirectory(0))
    for(i <- 1 to rootDirectory.length - 1) { 

      val dir = rootDirectory(i)
      if(dir !=null)
        combined = combined + "," + commaSeparatedFile(rootDirectory(i))
    }
    
    combined
  }
  
  def addLocalJar(sparkContext: SparkContext, rootDirectory: String): Boolean = { 
    
    try{
      val list = listFiles(rootDirectory).map("file://" + _)
      list.foreach(sparkContext.addJar)
      list.foreach(println)
      true
    } catch {
      case ex: Exception => throw ex
        false
    }
  }
}