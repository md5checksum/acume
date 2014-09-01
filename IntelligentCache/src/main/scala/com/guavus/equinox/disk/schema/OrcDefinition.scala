package com.guavus.equinox.disk.schema

import java.util.{HashMap => JHashMap}
import org.apache.spark.SparkContext
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import org.apache.spark.rdd.RDD

class OrcDefinition extends DefinitionTrait {

  private val orcDDef = new JHashMap[String, SchemaDefinition]
  private val orcMDef = new JHashMap[String, SchemaDefinition]
  private var init = false
  
  override def Init(file: String): Unit = { 
    
    //read file and populate some datastructure.
    
    /*
     * This is just for POC and will eventually be generic enough to load from file with schemas.
     */
    init1();
    init = true
  }
  
  override def getDimensionSchema(cubeId: String): SchemaDefinition = {

    if(!init) 
      throw new Exception("OrcDefinition not initialized.")
    orcDDef.get(cubeId).asInstanceOf[SchemaDefinition]
  }
  
  override def getMeasureSchema(cubeId: String): SchemaDefinition = { 
    
    if(!init) 
      throw new Exception("OrcDefinition not initialized.")
    orcMDef.get(cubeId).asInstanceOf[SchemaDefinition]
  }
  
  override def get(sc: SparkContext, file: String) = { 
    
    sc.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](file).asInstanceOf[RDD[Any]]
  }
  
  override def DeInit(file: String): Unit = { 
    
    //close file descriptors if not done in init.
    init = false;
  }
  
  def init1() = {
    
    orcDDef.put("searchIngressCustCube",new SchemaDefinition(List(("id",DataType.EBigInt),("timestamp",DataType.EBigInt),("IngressCustomerEntityId",DataType.EBigInt),("IngressAS",DataType.EBigInt),("IngressIP",DataType.EBigInt),("IngressRTR",DataType.EBigInt),("IncomingIF",DataType.EBigInt),("IngressRuleId",DataType.EBigInt),("FlowDirection",DataType.EBigInt))))
    orcMDef.put("searchIngressCustCube", new SchemaDefinition(List(("id", DataType.EBigInt),("timestamp",DataType.EBigInt),("TTS_B",DataType.EBigInt),("On_net_B",DataType.EBigInt),("Off_net_B",DataType.EBigInt),("Local_B",DataType.EBigInt),("Regional_B",DataType.EBigInt),("Continental_B",DataType.EBigInt),("XAtlantic_B",DataType.EBigInt),("XPacific_B",DataType.EBigInt),("XOthers_B",DataType.EBigInt))))
  }
}


