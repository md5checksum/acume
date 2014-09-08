package com.guavus.acume.disk.schema

object DataType extends Enumeration {

  type DataType = Value
  val EInt = Value("int")
  val EBigInt = Value("bigint")
  val ELong = Value("long")
  val EString = Value("string")
  val EFloat = Value("float")
  val EDouble = Value("double") 
  val EByteBuffer = Value("bytebuffer")
  val EPCSA = Value("pcsa")

}