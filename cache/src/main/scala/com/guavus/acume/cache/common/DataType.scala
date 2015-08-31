package com.guavus.acume.cache.common

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.ShortType
import java.lang.RuntimeException
import com.guavus.crux.core.ByteBuffer
import com.guavus.crux.jaxb.classes.{DataType => CruxDataType}
import com.guavus.crux.df.core.FieldDataType
import com.guavus.crux.df.core.FieldDataType._
import com.guavus.crux.df.util.DataTypeConverter

/**
 * @author archit.thakur
 *
 */
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
  
  def convertToLong(value: Any, datatype: DataType.DataType) : Long = {
    datatype match {
      case ACInt => value.asInstanceOf[Int].toLong
      case ACLong => value.asInstanceOf[Long].toLong
      case ACString => value.asInstanceOf[String].toLong
    }
  }
  
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
  
object ConversionToCrux { 
  
  def convertToCruxFieldDataType(dataType: com.guavus.acume.cache.common.DataType.DataType): FieldDataType = 
    DataTypeConverter.cruxToFieldDataType(CruxDataType.fromValue(dataType.typeString))
}
