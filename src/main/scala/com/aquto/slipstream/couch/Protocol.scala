package com.aquto.slipstream.couch

import akka.util.ByteString
import scala.util.Try
import java.nio.ByteOrder
import akka.util.ByteStringBuilder
import akka.util.ByteIterator
import scala.concurrent.duration._

object BinaryProtocolHandling{
  val EmptyBytes = Array[Byte]()
  val MinRecvPacket = 24
  val ReqMagic = 0x80.toByte
  val ResMagic = 0x81.toByte
  val DummyOpCode = 0xff.toByte 
  val ZeroByte = 0.toByte
  val NoExpiration = 0 seconds
  
  val AuthCmd = 0x21.toByte 
  val GetCmd = 0x00.toByte
  val SetCmd = 0x01.toByte
  //val GetQCmd = 0x09.toByte
  //val NoopCmd = 0x0a.toByte
  
  val SUCCESS = 0x00;
  val ERR_NOT_FOUND = 0x01;
  val ERR_EXISTS = 0x02;
  val ERR_2BIG = 0x03;
  val ERR_INVAL = 0x04;
  val ERR_NOT_STORED = 0x05;
  val ERR_DELTA_BADVAL = 0x06;
  val ERR_NOT_MY_VBUCKET = 0x07;
  val ERR_UNKNOWN_COMMAND = 0x81;
  val ERR_NO_MEM = 0x82;
  val ERR_NOT_SUPPORTED = 0x83;
  val ERR_INTERNAL = 0x84;
  val ERR_BUSY = 0x85;
  val ERR_TEMP_FAIL = 0x86; 
  val StatusMap = Map(
    SUCCESS -> "Success",
    ERR_NOT_FOUND -> "Error, Not Found",
    ERR_EXISTS -> "Error, Exists",
    ERR_2BIG -> "Error, Too Big",
    ERR_INVAL -> "Error, Invalid",
    ERR_NOT_STORED -> "Error, Not stored",
    ERR_DELTA_BADVAL -> "Error, Delta Bad Val",
    ERR_NOT_MY_VBUCKET -> "Error, Not My Vbucket",
    ERR_UNKNOWN_COMMAND -> "Error, Unknown Command",
    ERR_NO_MEM -> "Error, No Memory",
    ERR_NOT_SUPPORTED -> "Error, Not Supported",
    ERR_INTERNAL -> "Error, Interval",
    ERR_BUSY -> "Error, Busy",
    ERR_TEMP_FAIL -> "Error, Temp Fail"
  )
  
  case class MemcachedHeader(magic:Byte, opCode:Byte, keyLen:Short, extrasLen:Byte, dataType:Byte, statusOrVbucket:Short, totalBodyLen:Int, opaque:Int, casId:Long)
  object MemcachedHeader{
    val HeaderLength = 24
    
    def apply(data:ByteString):MemcachedHeader = {
      implicit val byteOrder = ByteOrder.BIG_ENDIAN
      val it = data.iterator
      val magic = it.getByte
      val respOp = it.getByte
      val keyLen = it.getShort
      val extrasLen = it.getByte
      val dataType = it.getByte
      val respStatus = it.getShort
      
      val totalBodyLen = it.getInt
      val respOpaq = it.getInt
      val respCas = it.getLong
      MemcachedHeader(magic, respOp, keyLen, extrasLen, dataType, respStatus, totalBodyLen, respOpaq, respCas)
    }
  }
  
  case class Request(header:MemcachedHeader, key:String, value:Array[Byte], extras:Array[Byte]){
    def toByteString = {
      implicit val byteOrder = ByteOrder.BIG_ENDIAN
      val builder = new ByteStringBuilder
      builder.putByte(ReqMagic)  
      builder.putByte(header.opCode)
      builder.putShort(header.keyLen)
      builder.putByte(header.extrasLen)
      builder.putByte(header.dataType) 
      builder.putShort(header.statusOrVbucket)
      builder.putInt(header.totalBodyLen)
      builder.putInt(header.opaque)     
      builder.putLong(header.casId) 
      if (extras.length > 0){
        builder.putBytes(extras)
      }
      builder.putBytes(key.getBytes) 
      builder.putBytes(value) 
      builder.result      
    }
  } 
  
  object Request{
    def apply(key:String, cmd:Byte, opaque:Int, cas:Long, vbucket:Byte, value:Array[Byte], extras:Array[Byte] = EmptyBytes):Request = {
      val keyBytes = key.getBytes
      val extrasLen = extras.length.toByte
      val totalBodyLen = keyBytes.length + value.length + extrasLen
      Request(MemcachedHeader(ReqMagic, cmd, keyBytes.length.toShort, extrasLen, ZeroByte, vbucket, totalBodyLen, opaque, cas), key, value, extras)
    }
  }  
  
  case class Response(header:MemcachedHeader, payload:Array[Byte])
  
  object Response{
    import MemcachedHeader._
    
    def apply(header:MemcachedHeader, data:ByteString):Response = {
      implicit val byteOrder = ByteOrder.BIG_ENDIAN
      val header = MemcachedHeader(data)
      
      
      val extras = 
        if (header.extrasLen == 0) EmptyBytes
        else{
          val extrasString = data.slice(HeaderLength, HeaderLength + header.extrasLen)
          extrasString.toArray
        }
        
      val payloadLen = header.totalBodyLen - header.extrasLen
      val payload = data.slice(HeaderLength + header.extrasLen, HeaderLength + header.extrasLen + payloadLen)
      
      Response(header, payload.toArray)
    }
  }  
}


trait Operation[RT]{
  val command:Byte
  def value:SerializedData  
  def key:String
  def extras(ser:SerializedData):Array[Byte] = BinaryProtocolHandling.EmptyBytes
  
  def processResponse(resp:BinaryProtocolHandling.Response):Try[RT]
}

trait NoValue{ self:Operation[_] =>
  def value = SerializedData(BinaryProtocolHandling.EmptyBytes, None)
}

case class Get[T](key:String, deser:(SerializedData) => T = DefaultSerializer.deserialize _) extends Operation[Option[T]] with NoValue{
  val command = BinaryProtocolHandling.GetCmd
  def processResponse(resp:BinaryProtocolHandling.Response) = {
    //TODO - Proper response handling logic
    resp.header.statusOrVbucket match{
      case BinaryProtocolHandling.ERR_NOT_FOUND => util.Success(None)
      case _ => util.Success(Some(deser(SerializedData(resp.payload))))
    }    
  }
}

case class StorageResult(success:Boolean, cas:Option[Long] = None)

case class Set[T](key:String, v:T, exp:Duration = BinaryProtocolHandling.NoExpiration, ser:(T) => SerializedData = DefaultSerializer.serialize _) extends Operation[StorageResult] {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN
  
  val command = BinaryProtocolHandling.SetCmd
  def value = ser(v)
  override def extras(ser:SerializedData) = {
    val builder = new ByteStringBuilder
    builder.putInt(ser.flags.getOrElse(0)) //flags
    builder.putInt(exp.toSeconds.toInt) //expiration
    builder.result.toArray
  }
  def processResponse(resp:BinaryProtocolHandling.Response) = {
    //TODO - Proper response handling logic
    util.Success(StorageResult(true, Some(resp.header.casId)))
  }
}

