//package com.guavus.acume.disk.schema
//
//import scala.collection.mutable.Map
//import org.apache.spark.SparkContext
//import org.apache.hadoop.io.NullWritable
//import org.apache.hadoop.hive.ql.io.orc.OrcStruct
//import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
//import org.apache.spark.rdd.RDD
//import java.lang.RuntimeException
//import com.guavus.acume.gen.AcumeCubes
//import scala.collection.JavaConversions._
//import org.apache.spark.sql.catalyst.types.StructType
//import org.apache.spark.sql.catalyst.types.StructField
//
//class OrcDefinition(acumeCube: AcumeCubes) extends DefinitionTrait(acumeCube) {
//
////  private val orcDDef = new JHashMap[String, SchemaDefinition]
////  private val orcMDef = new JHashMap[String, SchemaDefinition]
//  
//  private val orcDef = Map[String, SchemaDefinition]()
////  init(acumeCube);
//  
////  override def getDimensionSchema(cubeId: String): SchemaDefinition = {
////
////    if(!init) 
////      throw new Exception("OrcDefinition not initialized.")
////    orcDDef.get(cubeId).asInstanceOf[SchemaDefinition]
////  }
////  
////  override def getMeasureSchema(cubeId: String): SchemaDefinition = { 
////    
////    if(!init) 
////      throw new Exception("OrcDefinition not initialized.")
////    orcMDef.get(cubeId).asInstanceOf[SchemaDefinition]
////  }
//
//  override def getSchema(cubeId: String) = {
//    
//    val definition = orcDef.get(cubeId)
//    definition match { 
//      
//      case Some(_$x) => _$x
//      case None => throw new RuntimeException("Schema for cubeId = " + cubeId + " is not defined.")
//    }
//  }
//  
//  override def get(sc: SparkContext, file: String) = { 
//    
//    sc.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](file).asInstanceOf[RDD[Any]]
//  }
//  
////  def init(acumeCube: AcumeCubes) = {
////    
////    val list = acumeCube.getORCCube()
////    StructType(List(StructField()))
////    for(_$gc <- list){
////      
////      _$gc.getCubeName()
////      _$gc.getDimensionSet()
////    }
////    orcDDef.put("searchIngressCustCube",new SchemaDefinition(List(("id",DataType.EBigInt),("timestamp",DataType.EBigInt),("IngressCustomerEntityId",DataType.EBigInt),("IngressAS",DataType.EBigInt),("IngressIP",DataType.EBigInt),("IngressRTR",DataType.EBigInt),("IncomingIF",DataType.EBigInt),("IngressRuleId",DataType.EBigInt),("FlowDirection",DataType.EBigInt))))
////    orcMDef.put("searchIngressCustCube", new SchemaDefinition(List(("id", DataType.EBigInt),("timestamp",DataType.EBigInt),("TTS_B",DataType.EBigInt),("On_net_B",DataType.EBigInt),("Off_net_B",DataType.EBigInt),("Local_B",DataType.EBigInt),("Regional_B",DataType.EBigInt),("Continental_B",DataType.EBigInt),("XAtlantic_B",DataType.EBigInt),("XPacific_B",DataType.EBigInt),("XOthers_B",DataType.EBigInt))))
////  }
//}
//
//
//	