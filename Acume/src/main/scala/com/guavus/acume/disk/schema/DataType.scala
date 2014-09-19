package com.guavus.acume.disk.schema

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

  type DataType = Value
  val AcumeInt = Value("int")
  val AcumeLong = Value("long")
  val AcumeString = Value("string")
  val AcumeFloat = Value("float")
  val AcumeDouble = Value("double") 
  val AcumeByteBuffer = Value("bytebuffer")
  val AcumePCSA = Value("pcsa")
  val AcumeNull = Value("null")
  val AcumeBinary = Value("binary")
  val AcumeBoolean = Value("boolean")
  val AcumeTimestamp = Value("timestamp")
  val AcumeShort = Value("short")
  val AcumeByte = Value("byte")

//  def getDataType(str: String) = { 
//    
//    for(_$<- DataType.values)
//  }
}

object ConversionToSpark { 
  
  def convertToSparkDataType(dataType: DataType.Value) = { 
    
    dataType match { 
      
      case DataType.AcumeInt => IntegerType
      case DataType.AcumeLong => LongType
      case DataType.AcumeString => StringType
      case DataType.AcumeFloat => FloatType
      case DataType.AcumeDouble => DoubleType  
      case DataType.AcumeNull => NullType
      case DataType.AcumeBinary => BinaryType
      case DataType.AcumeBoolean => BooleanType
      case DataType.AcumeTimestamp => TimestampType
      case DataType.AcumeShort => ShortType
      case DataType.AcumeByte => ByteType
      case _ => throw new RuntimeException("Not Supported Datatype: " + dataType)
    }
  }
}