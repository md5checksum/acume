package com.guavus.acume.core

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.hive.ql.io.orc.OrcStruct

case class SearchPRI_InteractionEgressMeasure(id: Long, ts: Long, TTS_B: Long)
case class SearchPRI_InteractionEgressDimension(id: Long, ts: Long, EgressProspectEntityId: Long, EgressNeighborEntityId: Long, EgressAS: Long, EgressIP: Long, EgressRTR: Long, OutgoingIF: Long, FlowDirection: Long)

case class searchIngressCustCubeDimension(id: Long, ts: Long, IngressCustomerEntityId: Long, IngressAS: Long, IngressIP: Long, IngressRTR: Long, IncomingIF: Long, IngressRuleId: Long, FlowDirection: Long)
case class searchIngressCustCubeMeasure(id: Long, ts: Long, TTS_B: Long, On_net_B: Long, Off_net_B: Long, Local_B: Long, Regional_B: Long, Continental_B: Long, XAtlantic_B: Long, XPacific_B: Long, XOthers_B: Long)

case class SearchPRI_InteractionIngressDimension(id: Long, ts: Long, IngressProspectEntityId: Long, IngressNeighborEntityId: Long, IngressAS: Long, IngressIP: Long, IngressRTR: Long, IncomingIF: Long, FlowDirection: Long)
case class SearchPRI_InteractionIngressMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchEgressCustCubeDimension(id: Long, ts: Long, EgressCustomerEntityId: Long, EgressAS: Long, EgressIP: Long, EgressRTR: Long, OutgoingIF: Long, EgressRuleId: Long, FlowDirection: Long)
case class searchEgressCustCubeMeasure(id: Long, ts: Long, TTS_B: Long, On_net_B: Long, Off_net_B: Long, Local_B: Long, Regional_B: Long, Continental_B: Long, XAtlantic_B: Long, XPacific_B: Long, XOthers_B: Long)

case class searchEgressEntityCubeDimension(id: Long, ts: Long, EgressNeighborEntityId: Long, EgressAS: Long, EgressIP: Long, EgressRTR: Long, OutgoingIF: Long, FlowDirection: Long)
case class searchEgressEntityCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchEgressPeerCubeDimension(id: Long, ts: Long, EgressPeerEntityId: Long, EgressAS: Long, EgressIP: Long, EgressRTR: Long, OutgoingIF: Long, EgressRuleId: Long, FlowDirection: Long)
case class searchEgressPeerCubeMeasure(id: Long, ts: Long, TTS_B: Long, On_net_B: Long, Off_net_B: Long, Local_B: Long, Regional_B: Long, Continental_B: Long, XAtlantic_B: Long, XPacific_B: Long, XOthers_B: Long)

case class searchEgressProsCubeDimension(id: Long, ts: Long, EgressProspectEntityId: Long, EgressAS: Long, Dst2HopAS: Long, DstFinalAS: Long, EgressIP: Long, EgressRTR: Long, OutgoingIF: Long, FlowDirection:Long)
case class searchEgressProsCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchIngressEntityCubeDimension(id: Long, ts: Long, IngressNeighborEntityId: Long, IngressAS: Long, IngressIP: Long, IngressRTR: Long, IncomingIF: Long, FlowDirection: Long)
case class searchIngressEntityCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchIngressProsCubeDimension(id: Long, ts: Long, IngressProspectEntityId: Long, IngressAS: Long, Src2HopAS: Long, SrcFinalAS: Long, IngressIP: Long, IngressRTR: Long, IncomingIF: Long, FlowDirection: Long)
case class searchIngressProsCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchPrefixEgressCustCubeDimension(id: Long, ts: Long, EgressAS: Long, EgressCustomerEntityId: Long, FlowDirection: Long)
case class searchPrefixEgressCustCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchPrefixEgressPeerCubeDimension(id: Long, ts: Long, EgressAS: Long, EgressPeerEntityId: Long, FlowDirection: Long)
case class searchPrefixEgressPeerCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchPrefixEgressProsCubeDimension(id: Long, ts: Long, DstFinalAS: Long, EgressProspectEntityId: Long, FlowDirection: Long)
case class searchPrefixEgressProsCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchPrefixIngressCustCubeDimension(id: Long, ts: Long, IngressAS: Long, IngressCustomerEntityId: Long, FlowDirection: Long)
case class searchPrefixIngressCustCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchPrefixIngressPeerCubeDimension(id: Long, ts: Long, IngressAS: Long, IngressPeerEntityId: Long, FlowDirection: Long)
case class searchPrefixIngressPeerCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class searchPrefixIngressProsCubeDimension(id: Long, ts: Long, SrcFinalAS: Long, IngressProspectEntityId: Long, FlowDirection: Long)
case class searchPrefixIngressProsCubeMeasure(id: Long, ts: Long, TTS_B: Long)

case class DummyCubeDimension(id: Long, ts: Long, DummyDimension: Long)
case class DummyCubeMeasure(id: Long, ts: Long, DummyMeasure: Long)

object CubeORC {

 
  def to_long(str: String) = { 
    try{
      str.toLong
    } catch {
      case ex: NumberFormatException => Int.MinValue 	
    }
  }
   
  def _$isearchIngressCustCubeDimension(tuple: (NullWritable, OrcStruct)) = { 
      
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressCustCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)))
  }
  
  def _$isearchIngressCustCubeMeasure(tuple: (NullWritable, OrcStruct)) = { 
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressCustCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)), to_long(token(9)), to_long(token(10)))
  }
  
  def _$iDummyCubeDimension(tuple: (NullWritable, OrcStruct)) = { 
      
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    DummyCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)))
  }
  
  def _$iDummyCubeMeasure(tuple: (NullWritable, OrcStruct)) = { 
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    DummyCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
  }
  
  def _$iSearchPRI_InteractionEgressDimension(tuple: (NullWritable, OrcStruct)) = { 
    
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    SearchPRI_InteractionEgressDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)))
  }
  def _$iSearchPRI_InteractionEgressMeasure(tuple: (NullWritable, OrcStruct)) = { 
    
     
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    SearchPRI_InteractionEgressMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
  
  }

def _$iSearchPRI_InteractionIngressDimension(tuple: (NullWritable, OrcStruct)) = { 
  
   val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    SearchPRI_InteractionIngressDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)))
}

def _$iSearchPRI_InteractionIngressMeasure(tuple: (NullWritable, OrcStruct)) = { 
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    SearchPRI_InteractionIngressMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchEgressCustCubeDimension(tuple: (NullWritable, OrcStruct)) = { 
  
   val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchEgressCustCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)))
}

def _$isearchEgressCustCubeMeasure(tuple: (NullWritable, OrcStruct)) = { 
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchEgressCustCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)), to_long(token(9)), to_long(token(10)))
}

def _$isearchEgressEntityCubeDimension(tuple: (NullWritable, OrcStruct)) = { 
  
   val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchEgressEntityCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)))
}

def _$isearchEgressEntityCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
  searchEgressEntityCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchEgressPeerCubeDimension(tuple: (NullWritable, OrcStruct)) = { 
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
  searchEgressPeerCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)))

}

def _$isearchEgressPeerCubeMeasure(tuple: (NullWritable, OrcStruct)) = { 
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchEgressPeerCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)), to_long(token(9)), to_long(token(10)))
}


def _$isearchIngressEntityCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressEntityCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)))

}

def _$isearchIngressEntityCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressEntityCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchIngressProsCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressProsCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)), to_long(token(9)))
}


def _$isearchIngressProsCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressProsCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchPrefixEgressCustCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixEgressCustCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)))
}

def _$isearchPrefixEgressCustCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixEgressCustCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchPrefixEgressPeerCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixEgressPeerCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)))
}

def _$isearchPrefixEgressPeerCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixEgressPeerCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchPrefixEgressProsCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixEgressProsCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)))
}

def _$isearchPrefixEgressProsCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixEgressProsCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchPrefixIngressCustCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixIngressCustCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)))
}

def _$isearchPrefixIngressCustCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixIngressCustCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchPrefixIngressPeerCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixIngressPeerCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)))
}

def _$isearchPrefixIngressPeerCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixIngressPeerCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchPrefixIngressProsCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixIngressProsCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)))
}

def _$isearchPrefixIngressProsCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchPrefixIngressProsCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}

def _$isearchEgressProsCubeDimension(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchEgressProsCubeDimension(to_long(token(0)), to_long(token(1)), to_long(token(2)), to_long(token(3)), to_long(token(4)), to_long(token(5)), to_long(token(6)), to_long(token(7)), to_long(token(8)), to_long(token(9)))
}

def _$isearchEgressProsCubeMeasure(tuple: (NullWritable, OrcStruct)) = {
  
  val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchEgressProsCubeMeasure(to_long(token(0)), to_long(token(1)), to_long(token(2)))
}
}