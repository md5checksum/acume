package com.guavus.dev

import scala.math.random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Logging
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import com.guavus.mapred.common.collection._
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import java.util.ArrayList
import org.apache.spark.SparkContext._
import org.apache.hadoop.hive.ql.io.orc.OrcSerde
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.ql.io.orc.Writer
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import org.apache.hadoop.io.Writable
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat
import java.io.BufferedReader
import java.io.FileReader
import com.guavus.mapred.reflex.bizreflex.cube.SearchPRI_InteractionEgressDimension
import com.guavus.mapred.reflex.bizreflex.cube.SearchPRI_InteractionEgressMeasure
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.ql.io.orc.CompressionKind
import com.guavus.mapred.reflex.bizreflex.cube._

object DevMain extends Logging {

  def main(args: Array[String]) { 
    
    /*
     * Test Logging.
     
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    //export SPARK_LOG4J_CONF="/opt/spark/conf/spark-container-log4j.properties
    logError("env var = " + System.getenv("SPARK_LOG4J_CONF"))
    logTrace("driver logs - Trace")
    logDebug("driver logs - Debug")
    logInfo("driver logs - Info")
    logWarning("driver logs - Warning")
    logError("driver logs - Error")
    val count = spark.parallelize(1 to n, slices).map(mapper_func).reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop
    * 
    * 
    * 
    */
    
//    Dev.readSeq("/Users/archit.thakur/Desktop/orc_file/X.MAPREDUCE.0.5");
    
    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "spark://192.168.117.119:7077")
    sparkConf.set("spark.app.name", "ConversionFromTextToOrcFile")
    val sparkContext = new SparkContext(sparkConf)
    val list = List(1)
    sparkContext.parallelize(list,1).map({tuple =>
      val o=0
      val i=0
      val str = "/data/archit/bizreflex_out/2013.11.18."
        for(o <- 9 to 10){
          for(i <- 0 to 9){
            val oNew = if(o<10) "0"+o else ""+o
            Dev.convertSequenceFileToText(str+oNew+"/X.MAPREDUCE.0."+i)
          }
        }})
  }
  
  def mapper_func(i: Int): Int = { 
    
    logError("env var = " + System.getenv("SPARK_LOG4J_CONF"))
    logTrace("executor logs - Trace")
    logDebug("executor logs - Debug")
    logInfo("executor logs - Info")
    logWarning("executor logs - Warning")
    logError("executor logs - Error")
    val x = random * 2 - 1
     val y = random * 2 - 1
     if (x*x + y*y < 1) 1 else 0
  }
  
  def readSeq(str: String) = {
    
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val path = new Path(str)
	val reader = new SequenceFile.Reader(fs, path, conf)
    
    val key = new DimensionSet
    val value = new MeasureSet
    
    val writer = new BufferedWriter(new FileWriter("/Users/archit.thakur/Documents/Code_Custom_SparkCache_Scala/del"))
    while(reader.next(key, value)){
      
      writer.write(key.toString + "\t" + value.toString + "\n" )
    }
    reader.close();
    writer.close();
  }
}

object RCScheme {
 
  
  def check(ar: Array[String], str: String): Boolean = {
    for(string <- ar){
    if(str.startsWith(string))
      return true
    }
    return false
  }
  
  def close(wtr: java.util.HashMap[String, Writer]) {
    
    for(entry <- wtr.entrySet().toArray()){
      
      entry.asInstanceOf[java.util.Map.Entry[String, Writer]].getValue().close()
    }
  }
  
  def init_writer(readMap: java.util.HashMap[String, String], wtr: java.util.HashMap[String, Writer]) = {
    
    val config = new Configuration
    val fs = FileSystem.get(config)
    val rowIndexStride = 10000
    val stripeSize = 268435456l
    val bufferSize = 524288
    val compress = CompressionKind.ZLIB
    for(entry <- readMap.entrySet().toArray()){

      val key = entry.asInstanceOf[java.util.Map.Entry[String, String]].getKey()
      val value = entry.asInstanceOf[java.util.Map.Entry[String, String]].getValue()
      
      val token = key.split("_")
      val token3 = 
        if(token(token.length - 1) == "ds")
        "Dimension"
      else
        "Measure"
        
        var token2 = ""
        for(in <- 1 to token.length - 2)
          if(in != token.length - 2) 
            token2 = token2 + token(in)+"_"
          else
            token2 = token2+token(in)
        
      
      val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(value)
      val inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
      val writer = OrcFile.createWriter(fs, new Path("/Users/archit.thakur/Documents/Code_Custom_SparkCache_Scala/orc/" + token2 + token3 + ".orc"), config, inspector, stripeSize, compress, bufferSize, rowIndexStride)
      wtr.put(key, writer)
    }
  }
  
  def init_id(x: java.util.HashMap[Int, java.util.HashMap[DimensionSet, Int]], cubeId: Array[String]) {
    
    for(i <- cubeId) {
      
      x.put(i.toInt, new java.util.HashMap[DimensionSet, Int])
    }
  }
  
  def main(args: Array[String]) {
      
//    val cubeId = "102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 130 131 132 133 138 139 140 141 144 145 32 56 58 64 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 95".split(" ")
    val cubeId = "75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 95".split(" ")
    import java.util.{HashMap => JHashMap}
    val hashMap = new JHashMap[String, String]
    val orcWtr = new JHashMap[String, Writer]
    val dsIdMap = new JHashMap[Int, JHashMap[DimensionSet, Int]]
    init(hashMap)
    init_writer(hashMap, orcWtr)
    init_id(dsIdMap, cubeId)
    
    
    /*
     * Spark Code.
    
    val conf = new SparkConf
    conf.set("spark.app.name", "ConversionFromTextToOrcFile")
    val sc = new SparkContext("local[5]","ConversionFromTextToOrcFile",new SparkConf)
    val rdd = sc.parallelize(List(1), 1).map({
      tuple=>
        val typeString = "struct<c1:string,c2:int,c3:string>";
        val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString)
        val inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
        val row = new ArrayList[Comparable[_]](3);
        row.add("hello")
        row.add(int2Integer(214))
        row.add("goyal")
        val ss = new OrcSerde;
        ss.serialize(row, inspector);
    })
    
    val rdd2 = rdd.map(tuple => (NullWritable.get(), tuple))
    rdd2.saveAsNewAPIHadoopFile("/var/tmp/nitin-rc2", classOf[NullWritable], classOf[Writable], classOf[OrcNewOutputFormat])
    val rdd3 = sc.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/var/tmp/nitin-rc2")
    println(rdd3.collect);
    * */
    
    val rowIndexStride = 10000
    val stripeSize = 268435456l
    val bufferSize = 524288
    val compress = CompressionKind.ZLIB
    
    val o=0
    val i=0
    val _$str = "/Users/archit.thakur/Documents/Code_Custom_SparkCache_Scala/2013.11.18."
    val orc = new OrcSerde
    
    val config = new Configuration
    val fs = FileSystem.get(config)
//    val objInspector = ObjectInspectorFactory.getReflectionObjectInspector(classOf[ArrayList[Comparable[_]]], ObjectInspectorOptions.JAVA)
    var writer_d: Writer = null //OrcFile.createWriter(fs, new Path("/Users/archit.thakur/Documents/Code_Crux.Git_Scala/SearchPRI_InteractionEgressdimension.orc"), config, objInspector, stripeSize, compress, bufferSize, rowIndexStride)
    var writer_m: Writer = null //OrcFile.createWriter(fs, new Path("/Users/archit.thakur/Documents/Code_Crux.Git_Scala/SearchPRI_InteractionEgressmeasure.orc"), config, objInspector, stripeSize, compress, bufferSize, rowIndexStride)
    for(o <- 9 to 10){
      for(i <- 0 to 9){
        val oNew = if(o<10) "0"+o else ""+o
        
        val bufferReader = new BufferedReader(new FileReader(_$str+ oNew +"/X.MAPREDUCE.0."+i+".text"))
        var str: String = null
        while({str = bufferReader.readLine; str} != null.asInstanceOf[String]){
          
          val cube = str.split("\t")
          val dimensionSet = cube(0)
          val measureSet = cube(1)
          
          val dsobject = new DimensionSet
          val msobject = new MeasureSet
          
          try{
          dsobject.readFrom(dimensionSet)
          msobject.readFrom(measureSet)
          } catch{
            case ex: Exception => println(dimensionSet + " " + measureSet)
            throw ex
          }
          
          val localCubeId = dsobject.getCubeId()
          val localTimestamp = dsobject.getTimestamp()
          
          var flag = false
          
          for(x <- cubeId) {
            
            if(localCubeId == x.toInt) {
              
              flag = true
            }
          }
          
          if(flag ) {
          val out = dsIdMap.get(localCubeId)
          val id = out.get(dsobject)
          var m_Id = 0
          var toBeWritten = false
          if(id == null.asInstanceOf[Int]) {
              
            val keySet = out.keySet()
            for(key <- keySet.toArray) {
            
              val value = out.get(key);
              if(value > m_Id)
                m_Id = value
            }
            m_Id = m_Id + 1 	
            out.put(dsobject, m_Id)
            toBeWritten = true
          }
            
          else { 
          
            m_Id = id 	 	
          }
          
          if(str.startsWith("75")){
            if(toBeWritten) { 
            val row_d = new ArrayList[Comparable[_]](9)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchEgressPeerCubeDimension.EgressPeerEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchEgressPeerCubeDimension.EgressAS.getLong(dsobject)))
            row_d.add(long2Long(searchEgressPeerCubeDimension.EgressIP.getLong(dsobject)))
            row_d.add(long2Long(searchEgressPeerCubeDimension.EgressRTR.getLong(dsobject)))
            row_d.add(long2Long(searchEgressPeerCubeDimension.OutgoingIF.getLong(dsobject)))
            row_d.add(long2Long(searchEgressPeerCubeDimension.EgressRuleId.getLong(dsobject)))
            row_d.add(long2Long(searchEgressPeerCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("75_searchEgressPeerCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](11)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.TTS_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.On_net_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.Off_net_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.Local_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.Regional_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.Continental_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.XAtlantic_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.XPacific_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressPeerCubeMeasure.XOthers_B.getLong(msobject)))
            writer_m = orcWtr.get("75_searchEgressPeerCube_ms")
            writer_m.addRow(row_m)
            
          }
          
          /*else if(str.startsWith(( "89" ))){

            if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](9)
              row_d.add(long2Long(m_Id.toLong))
              row_d.add(long2Long(localTimestamp))
              row_d.add(long2Long(SearchPRI_InteractionEgressDimension.EgressProspectEntityId.getLong(dsobject)))
              row_d.add(long2Long(SearchPRI_InteractionEgressDimension.EgressNeighborEntityId.getLong(dsobject)))
              row_d.add(long2Long(SearchPRI_InteractionEgressDimension.EgressAS.getLong(dsobject)))
              row_d.add(long2Long(SearchPRI_InteractionEgressDimension.EgressIP.getLong(dsobject)))
              row_d.add(long2Long(SearchPRI_InteractionEgressDimension.EgressRTR.getLong(dsobject)))
              row_d.add(long2Long(SearchPRI_InteractionEgressDimension.OutgoingIF.getLong(dsobject)))
              row_d.add(long2Long(SearchPRI_InteractionEgressDimension.FlowDirection.getLong(dsobject)))
              writer_d = orcWtr.get("89_SearchPRI_InteractionEgress_ds")
              writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(SearchPRI_InteractionEgressMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("89_SearchPRI_InteractionEgress_ms")
            writer_m.addRow(row_m)
            
          }
        
          else if(str.startsWith("76")){
            
            if(toBeWritten) { 
              
            val row_d = new ArrayList[Comparable[_]](9)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchIngressCustCubeDimension.IngressCustomerEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchIngressCustCubeDimension.IngressAS.getLong(dsobject)))
            row_d.add(long2Long(searchIngressCustCubeDimension.IngressIP.getLong(dsobject)))
            row_d.add(long2Long(searchIngressCustCubeDimension.IngressRTR.getLong(dsobject)))
            row_d.add(long2Long(searchIngressCustCubeDimension.IncomingIF.getLong(dsobject)))
            row_d.add(long2Long(searchIngressCustCubeDimension.IngressRuleId.getLong(dsobject)))
            row_d.add(long2Long(searchIngressCustCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("76_searchIngressCustCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](11)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchIngressCustCubeMeasure.TTS_B.getLong(msobject)))
            row_m.add(long2Long(searchIngressCustCubeMeasure.On_net_B.getLong(msobject)))
            row_m.add(long2Long(searchIngressCustCubeMeasure.Off_net_B.getLong(msobject)))
            row_m.add(long2Long(searchIngressCustCubeMeasure.Local_B.getLong(msobject)))
            row_m.add(long2Long(searchIngressCustCubeMeasure.Regional_B.getLong(msobject)))
            row_m.add(long2Long(searchIngressCustCubeMeasure.Continental_B.getLong(msobject)))
            row_m.add(long2Long(searchIngressCustCubeMeasure.XAtlantic_B.getLong(msobject)))
            row_m.add(long2Long(searchIngressCustCubeMeasure.XPacific_B.getLong(msobject)))
            row_m.add(long2Long(searchIngressCustCubeMeasure.XOthers_B.getLong(msobject)))
            writer_m = orcWtr.get("76_searchIngressCustCube_ms")
            writer_m.addRow(row_m)
            
          }
          
          else if(str.startsWith("77")) { 
            
            if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](9)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchEgressCustCubeDimension.EgressCustomerEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchEgressCustCubeDimension.EgressAS.getLong(dsobject)))
            row_d.add(long2Long(searchEgressCustCubeDimension.EgressIP.getLong(dsobject)))
            row_d.add(long2Long(searchEgressCustCubeDimension.EgressRTR.getLong(dsobject)))
            row_d.add(long2Long(searchEgressCustCubeDimension.OutgoingIF.getLong(dsobject)))
            row_d.add(long2Long(searchEgressCustCubeDimension.EgressRuleId.getLong(dsobject)))
            row_d.add(long2Long(searchEgressCustCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("77_searchEgressCustCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](11)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchEgressCustCubeMeasure.TTS_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressCustCubeMeasure.On_net_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressCustCubeMeasure.Off_net_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressCustCubeMeasure.Local_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressCustCubeMeasure.Regional_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressCustCubeMeasure.Continental_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressCustCubeMeasure.XAtlantic_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressCustCubeMeasure.XPacific_B.getLong(msobject)))
            row_m.add(long2Long(searchEgressCustCubeMeasure.XOthers_B.getLong(msobject)))
            writer_m = orcWtr.get("77_searchEgressCustCube_ms")
            writer_m.addRow(row_m)
          
          } 	
          
          else if(str.startsWith("78")) {
            
            if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](10)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchIngressProsCubeDimension.IngressProspectEntityId.getLong(dsobject))) 
            row_d.add(long2Long(searchIngressProsCubeDimension.IngressAS.getLong(dsobject)))
            row_d.add(long2Long(searchIngressProsCubeDimension.Src2HopAS.getLong(dsobject)))
            row_d.add(long2Long(searchIngressProsCubeDimension.SrcFinalAS.getLong(dsobject)))
            row_d.add(long2Long(searchIngressProsCubeDimension.IngressIP.getLong(dsobject)))
            row_d.add(long2Long(searchIngressProsCubeDimension.IngressRTR.getLong(dsobject)))
            row_d.add(long2Long(searchIngressProsCubeDimension.IncomingIF.getLong(dsobject)))
            row_d.add(long2Long(searchIngressProsCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("78_searchIngressProsCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchIngressProsCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("78_searchIngressProsCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("79")) {
            
            if(toBeWritten) {
            val row_d = new ArrayList[Comparable[_]](10)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchEgressProsCubeDimension.EgressProspectEntityId.getLong(dsobject))) 
            row_d.add(long2Long(searchEgressProsCubeDimension.EgressAS.getLong(dsobject)))
            row_d.add(long2Long(searchEgressProsCubeDimension.Dst2HopAS.getLong(dsobject)))
            row_d.add(long2Long(searchEgressProsCubeDimension.DstFinalAS.getLong(dsobject)))
            row_d.add(long2Long(searchEgressProsCubeDimension.EgressIP.getLong(dsobject)))
            row_d.add(long2Long(searchEgressProsCubeDimension.EgressRTR.getLong(dsobject)))
            row_d.add(long2Long(searchEgressProsCubeDimension.OutgoingIF.getLong(dsobject)))
            row_d.add(long2Long(searchEgressProsCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("79_searchEgressProsCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchEgressProsCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("79_searchEgressProsCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("80")) {
            
            if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](8)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchIngressEntityCubeDimension.IngressNeighborEntityId.getLong(dsobject))) 
            row_d.add(long2Long(searchIngressEntityCubeDimension.IngressAS.getLong(dsobject)))
            row_d.add(long2Long(searchIngressEntityCubeDimension.IngressIP.getLong(dsobject)))
            row_d.add(long2Long(searchIngressEntityCubeDimension.IngressRTR.getLong(dsobject)))
            row_d.add(long2Long(searchIngressEntityCubeDimension.IncomingIF.getLong(dsobject)))
            row_d.add(long2Long(searchIngressEntityCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("80_searchIngressEntityCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchIngressEntityCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("80_searchIngressEntityCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("81")) {
            
            if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](8)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchEgressEntityCubeDimension.EgressNeighborEntityId.getLong(dsobject))) 
            row_d.add(long2Long(searchEgressEntityCubeDimension.EgressAS.getLong(dsobject)))
            row_d.add(long2Long(searchEgressEntityCubeDimension.EgressIP.getLong(dsobject)))
            row_d.add(long2Long(searchEgressEntityCubeDimension.EgressRTR.getLong(dsobject)))
            row_d.add(long2Long(searchEgressEntityCubeDimension.OutgoingIF.getLong(dsobject)))
            row_d.add(long2Long(searchEgressEntityCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("81_searchEgressEntityCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchEgressEntityCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("81_searchEgressEntityCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("82")) {

            if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](5)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchPrefixIngressCustCubeDimension.IngressAS.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixIngressCustCubeDimension.IngressCustomerEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixIngressCustCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("82_searchPrefixIngressCustCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchPrefixIngressCustCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("82_searchPrefixIngressCustCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("83")) {
            if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](5)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchPrefixEgressCustCubeDimension.EgressAS.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixEgressCustCubeDimension.EgressCustomerEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixEgressCustCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("83_searchPrefixEgressCustCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchPrefixEgressCustCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("83_searchPrefixEgressCustCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("84")) {
            if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](5)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchPrefixIngressPeerCubeDimension.IngressAS.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixIngressPeerCubeDimension.IngressPeerEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixIngressPeerCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("84_searchPrefixIngressPeerCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchPrefixIngressPeerCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("84_searchPrefixIngressPeerCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("85")) {
          if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](5)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchPrefixEgressPeerCubeDimension.EgressAS.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixEgressPeerCubeDimension.EgressPeerEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixEgressPeerCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("85_searchPrefixEgressPeerCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchPrefixEgressPeerCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("85_searchPrefixEgressPeerCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("86")) {
          if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](5)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchPrefixIngressProsCubeDimension.SrcFinalAS.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixIngressProsCubeDimension.IngressProspectEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixIngressProsCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("86_searchPrefixIngressProsCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchPrefixIngressProsCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("86_searchPrefixIngressProsCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("87")) {
          if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](5)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(searchPrefixEgressProsCubeDimension.DstFinalAS.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixEgressProsCubeDimension.EgressProspectEntityId.getLong(dsobject)))
            row_d.add(long2Long(searchPrefixEgressProsCubeDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("87_searchPrefixEgressProsCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(searchPrefixEgressProsCubeMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("87_searchPrefixEgressProsCube_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("88")) {
          if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](9)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(SearchPRI_InteractionIngressDimension.IngressProspectEntityId.getLong(dsobject)))
            row_d.add(long2Long(SearchPRI_InteractionIngressDimension.IngressNeighborEntityId.getLong(dsobject)))
            row_d.add(long2Long(SearchPRI_InteractionIngressDimension.IngressAS.getLong(dsobject)))
            row_d.add(long2Long(SearchPRI_InteractionIngressDimension.IngressIP.getLong(dsobject)))
            row_d.add(long2Long(SearchPRI_InteractionIngressDimension.IngressRTR.getLong(dsobject)))
            row_d.add(long2Long(SearchPRI_InteractionIngressDimension.IncomingIF.getLong(dsobject)))
            row_d.add(long2Long(SearchPRI_InteractionIngressDimension.FlowDirection.getLong(dsobject)))
            writer_d = orcWtr.get("88_SearchPRI_InteractionIngress_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(SearchPRI_InteractionIngressMeasure.TTS_B.getLong(msobject)))
            writer_m = orcWtr.get("88_SearchPRI_InteractionIngress_ms")
            writer_m.addRow(row_m)
          }
          
          else if(str.startsWith("95")) {
          if(toBeWritten) {
              val row_d = new ArrayList[Comparable[_]](3)
            row_d.add(long2Long(m_Id.toLong))
            row_d.add(long2Long(localTimestamp))
            row_d.add(long2Long(DummyCubeDimension.DummyDimension.getLong(dsobject)))
            writer_d = orcWtr.get("95_DummyCube_ds")
            writer_d.addRow(row_d)
            }
            
            val row_m = new ArrayList[Comparable[_]](3)
            row_m.add(long2Long(m_Id.toLong))
            row_m.add(long2Long(localTimestamp))
            row_m.add(long2Long(DummyCubeMeasure.DummyMeasure.getLong(msobject)))
            writer_m = orcWtr.get("95_DummyCube_ms")
            writer_m.addRow(row_m)
          }*/
          }
        }
        bufferReader.close()
      }
    }
    
    close(orcWtr)
  }
  
  var nextId = 0
  def getNextId = {
    nextId += 1
    nextId
  }
  
  def init(hashMap: java.util.HashMap[String, String]) {
    hashMap.put("76_searchIngressCustCube_ds","struct<id:bigint,timestamp:bigint,IngressCustomerEntityId:bigint,IngressAS:bigint,IngressIP:bigint,IngressRTR:bigint,IncomingIF:bigint,IngressRuleId:bigint,FlowDirection:bigint>")
    hashMap.put("76_searchIngressCustCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint,On_net_B:bigint,Off_net_B:bigint,Local_B:bigint,Regional_B:bigint,Continental_B:bigint,XAtlantic_B:bigint,XPacific_B:bigint,XOthers_B:bigint>")

    hashMap.put("77_searchEgressCustCube_ds","struct<id:bigint,timestamp:bigint,EgressCustomerEntityId:bigint,EgressAS:bigint,EgressIP:bigint,EgressRTR:bigint,OutgoingIF:bigint,EgressRuleId:bigint,FlowDirection:bigint>")
    hashMap.put("77_searchEgressCustCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint,On_net_B:bigint,Off_net_B:bigint,Local_B:bigint,Regional_B:bigint,Continental_B:bigint,XAtlantic_B:bigint,XPacific_B:bigint,XOthers_B:bigint>")
    
    hashMap.put("75_searchEgressPeerCube_ds","struct<id:bigint,timestamp:bigint,EgressPeerEntityId:bigint,EgressAS:bigint,EgressIP:bigint,EgressRTR:bigint,OutgoingIF:bigint,EgressRuleId:bigint,FlowDirection:bigint>")
    hashMap.put("75_searchEgressPeerCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint,On_net_B:bigint,Off_net_B:bigint,Local_B:bigint,Regional_B:bigint,Continental_B:bigint,XAtlantic_B:bigint,XPacific_B:bigint,XOthers_B:bigint>")

    hashMap.put("78_searchIngressProsCube_ds","struct<id:bigint,timestamp:bigint,IngressProspectEntityId:bigint,IngressAS:bigint,Src2HopAS:bigint,SrcFinalAS:bigint,IngressIP:bigint,IngressRTR:bigint,IncomingIF:bigint,FlowDirection:bigint>")
    hashMap.put("78_searchIngressProsCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")

    hashMap.put("79_searchEgressProsCube_ds","struct<id:bigint,timestamp:bigint,EgressProspectEntityId:bigint,EgressAS:bigint,Dst2HopAS:bigint,DstFinalAS:bigint,EgressIP:bigint,EgressRTR:bigint,OutgoingIF:bigint,FlowDirection:bigint>")
    hashMap.put("79_searchEgressProsCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")
                                                              
    hashMap.put("80_searchIngressEntityCube_ds","struct<id:bigint,timestamp:bigint,IngressNeighborEntityId:bigint,IngressAS:bigint,IngressIP:bigint,IngressRTR:bigint,IncomingIF:bigint,FlowDirection:bigint>")
    hashMap.put("80_searchIngressEntityCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")
                                                                    
    hashMap.put("81_searchEgressEntityCube_ds","struct<id:bigint,timestamp:bigint,EgressNeighborEntityId:bigint,EgressAS:bigint,EgressIP:bigint,EgressRTR:bigint,OutgoingIF:bigint,FlowDirection:bigint>")    
    hashMap.put("81_searchEgressEntityCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")
                 
    hashMap.put("82_searchPrefixIngressCustCube_ds","struct<id:bigint,timestamp:bigint,IngressAS:bigint,IngressCustomerEntityId:bigint,FlowDirection:bigint>")
    hashMap.put("82_searchPrefixIngressCustCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")                             

    hashMap.put("83_searchPrefixEgressCustCube_ds","struct<id:bigint,timestamp:bigint,EgressAS:bigint,EgressCustomerEntityId:bigint,FlowDirection:bigint>")
    hashMap.put("83_searchPrefixEgressCustCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")       
    
    hashMap.put("84_searchPrefixIngressPeerCube_ds","struct<id:bigint,timestamp:bigint,IngressAS:bigint,IngressPeerEntityId:bigint,FlowDirection:bigint>")
    hashMap.put("84_searchPrefixIngressPeerCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")              
    
    hashMap.put("85_searchPrefixEgressPeerCube_ds","struct<id:bigint,timestamp:bigint,EgressAS:bigint,EgressPeerEntityId:bigint,FlowDirection:bigint>")    
    hashMap.put("85_searchPrefixEgressPeerCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")

    hashMap.put("86_searchPrefixIngressProsCube_ds","struct<id:bigint,timestamp:bigint,SrcFinalAS:bigint,IngressProspectEntityId:bigint,FlowDirection:bigint>") 
    hashMap.put("86_searchPrefixIngressProsCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")       
    
    hashMap.put("87_searchPrefixEgressProsCube_ds","struct<id:bigint,timestamp:bigint,DstFinalAS:bigint,EgressProspectEntityId:bigint,FlowDirection:bigint>")  
    hashMap.put("87_searchPrefixEgressProsCube_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")  
    
    hashMap.put("88_SearchPRI_InteractionIngress_ds","struct<id:bigint,timestamp:bigint,IngressProspectEntityId:bigint,IngressNeighborEntityId:bigint,IngressAS:bigint,IngressIP:bigint,IngressRTR:bigint,IncomingIF:bigint,FlowDirection:bigint>")  
    hashMap.put("88_SearchPRI_InteractionIngress_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")                                                                                    
    
    hashMap.put("89_SearchPRI_InteractionEgress_ds","struct<id:bigint,timestamp:bigint,EgressProspectEntityId:bigint,EgressNeighborEntityId:bigint,EgressAS:bigint,EgressIP:bigint,EgressRTR:bigint,OutgoingIF:bigint,FlowDirection:bigint>")    
    hashMap.put("89_SearchPRI_InteractionEgress_ms","struct<id:bigint,timestamp:bigint,TTS_B:bigint>")                  
    
    hashMap.put("95_DummyCube_ds","struct<id:bigint,timestamp:bigint,DummyDimension:bigint>")  
    hashMap.put("95_DummyCube_ms","struct<id:bigint,timestamp:bigint,DummyMeasure:bigint>")
  }
}




