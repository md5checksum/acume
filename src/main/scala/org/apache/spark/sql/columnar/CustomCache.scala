package org.apache.spark.sql.columnar

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.guavus.compute.Compute

object CustomCache {
  
  case class Track(trackId: String, songId: String, artistName: String, songTitle: String)
  case class Location(artistId: String, latitute: String, longitude: String, trackId: String, location: String)
  case class Artist(artistId: String, artistMbId: String, trackId: String, artistName: String)
  
  def randomGet(_$d: Array[String], index: Int) = 
    if (_$d.length >= index + 1)
      _$d(index)
    else 
      null
      
  def main(args: Array[String]) = { 
    
    val sparkContext = Compute.sparkContext
    val textFile = sparkContext.textFile("/data/archit//dataset/unique_tracks_dataset.txt", 3)
    val rdd_2 = sparkContext.textFile("/data/archit//dataset/unique_artists_dataset.txt", 3)
    val rdd_3 = sparkContext.textFile("/data/archit//dataset/artist_location_dataset.txt", 3)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext._
    textFile.map(_.split("<SEP>")).map(elx => Track(elx(0), elx(1), elx(2), randomGet(elx, 3))).registerAsTable("Track")
    rdd_2.map(_.split("<SEP>")).map(elx => Artist(elx(0), elx(1), elx(2), elx(3))).registerAsTable("Artist")
    rdd_3.map(_.split("<SEP>")).map(elx => Location(elx(0), elx(1), elx(2), elx(3), randomGet(elx, 4))).registerAsTable("Location")
    
    val grdd = sqlContext.sql("SELECT * from Track where songId = 'SOEYRFT12AB018936C'")
    grdd.saveAsTextFile("/data/archit//new_g")
    
//    val hrdd = sqlContext.sql("SELECT * from Track JOIN Location")
//    hrdd.saveAsTextFile("/data/archit//h")

    
    cacheTable("Track")
//    sqlContext.sql("SELECT * from Track where songTitle = 'SOEYRFT12AB018936C'").collect.foreach(println)
  }
}

