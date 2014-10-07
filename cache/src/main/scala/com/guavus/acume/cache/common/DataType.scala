package com.guavus.acume.cache.common

import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.NullType
import org.apache.spark.sql.catalyst.types.LongType
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.catalyst.types.TimestampType
import org.apache.spark.sql.catalyst.types.DoubleType
import org.apache.spark.sql.catalyst.types.BinaryType
import org.apache.spark.sql.catalyst.types.BooleanType
import org.apache.spark.sql.catalyst.types.ByteType
import org.apache.spark.sql.catalyst.types.FloatType
import org.apache.spark.sql.catalyst.types.ShortType
import java.lang.RuntimeException

object DataType extends Enumeration {

  val ACInt = new DataType("int")
  val ACLong = new DataType("long")
  val ACString = new DataType("string")
  val ACFloat = new DataType("float")
  val ACDouble = new DataType("double") 
  val ACByteBuffer = new DataType("bytebuffer")
  val ACPCSA = new DataType("pcsa")
  val ACNull = new DataType("null")
  val ACBinary = new DataType("binary")
  val ACBoolean = new DataType("boolean")
  val ACTimestamp = new DataType("timestamp")
  val ACShort = new DataType("short")
  val ACByte = new DataType("byte")
  
  class DataType(val typeString: String) extends Val
  
  def getDataType(datatype: String): DataType = {
    for(typeValue <- DataType.values if(datatype == typeValue.typeString))
      return typeValue
    ACDouble
  }
  
  implicit def convertValue(v: Value): DataType = v.asInstanceOf[DataType]
}

object ConversionToSpark { 
  
  def convertToSparkDataType(dataType: DataType.Value) = { 
    
    dataType match { 
      
      case DataType.ACInt => IntegerType
      case DataType.ACLong => LongType
      case DataType.ACString => StringType
      case DataType.ACFloat => FloatType
      case DataType.ACDouble => DoubleType  
      case DataType.ACNull => NullType
      case DataType.ACBinary => BinaryType
      case DataType.ACBoolean => BooleanType
      case DataType.ACTimestamp => TimestampType
      case DataType.ACShort => ShortType
      case DataType.ACByte => ByteType
      case _ => throw new RuntimeException("Not Supported Datatype: " + dataType)
    }
  }
}