package com.aquto.slipstream.couch

case class SerializedData(data:Array[Byte], flags:Option[Int] = None)

object DefaultSerializer {

  def serialize(input:Any):SerializedData = {
    SerializedData(input.toString.getBytes())
  }
  
  def deserialize(result:SerializedData):Any = {
    return new String(result.data, "utf-8")
  }
}