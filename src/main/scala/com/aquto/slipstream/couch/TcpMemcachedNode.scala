package com.aquto.slipstream.couch
import akka.actor._
import akka.io.{ IO, Tcp }
import java.net.InetSocketAddress
import javax.security.sasl.SaslClient
import javax.security.sasl.Sasl
import javax.security.auth.callback.CallbackHandler
import akka.util.ByteStringBuilder
import java.nio.ByteOrder
import akka.util.ByteString
import akka.util.ByteIterator
import akka.util.Timeout
import scala.concurrent.Await
import java.util.concurrent.CountDownLatch
import scala.concurrent.ExecutionContext
import java.util.Queue
import java.util.LinkedList
import com.typesafe.config.Config
import akka.dispatch.UnboundedPriorityMailbox
import akka.dispatch.PriorityGenerator
import akka.event.LoggingReceive

object TcpMemcachedNode{
  case class ExecuteOp[T](op:Operation[T], opaque:Int)
  case object WriteAck extends Tcp.Event
  
  def apply(fact:ActorRefFactory, client:ActorRef, address:InetSocketAddress, bucket:CouchbaseClient.Bucket) = {
    fact.actorOf(Props(classOf[TcpMemcachedNode], client, address, bucket))
  }
}

class TcpMemcachedNode(client:ActorRef, address:InetSocketAddress, bucket:CouchbaseClient.Bucket) 
  extends Actor with Stash with ActorLogging{
  import context.system
  import Tcp._
  import BinaryProtocolHandling._
  import TcpMemcachedNode._
  
  val manager = IO(Tcp)  
  manager ! Connect(address, pullMode = true)
  implicit val byteOrder = ByteOrder.BIG_ENDIAN
  var queue:Vector[ByteString] = Vector.empty 
  var pipelineBuffer = new ByteStringBuilder
  var currentPipelineSize = 0
  
  var requestOffset = 0
  var leftovers:Option[ByteString] = None
  val MaxPipelineSize = 250 //should be 10 ops per request
  var responses = 0
  
  /**
   * Get the current offset of the write buffer, incrementing the value for the next time this is called
   * @return an Int representing the current write buffer offset
   */
  def currentOffset = {
    val curr = requestOffset
    requestOffset += 1
    curr
  }
  
  def receive = connecting
  
  /**
   * Receive for handling messages after while we are trying to connect
   */
  def connecting:Receive = {
    //Handles a failure to connect 
    case CommandFailed(_:Connect) => 
      println("failed to connect")
      
    //Handles a successful connection
    case  c @ Connected(remote, local) =>
      log.info("Successfully connected to bucket {} on node {}", bucket.name, address)
      val connection = sender      
      connection ! Register(self)
      connection ! ResumeReading
      context.become(authenticating(connection))
      self ! "auth"
      
    //While not connected, stash any operation
    case ExecuteOp(op, opaque) => stash()
  }
  
  /**
   * Receive for handling messages while we are authenticatign against the bucket
   */
  def authenticating(connection:ActorRef):Receive = {
    //Handles the response from the auth request
    case Received(data) =>
      log.info("Successfully authenticated against bucket {} on node {}", bucket.name, address)
      val header = MemcachedHeader(data)
      val resp = Response(header, data)
      //unstashAll()
      context.become(writing(connection))
      
    //Kicks off the auth request
    case "auth" => 
      log.info("Initiating authentication against bucket {} on node {}", bucket.name, address)
      val cbh = new PlainCallbackHandler(bucket.name, bucket.password.toCharArray)
      val sc = Sasl.createSaslClient(Array("PLAIN"), null, "memcached", address.toString(), null, cbh)
      val response = if (sc.hasInitialResponse()) sc.evaluateChallenge(bucket.password.getBytes()) else EmptyBytes
      val req = Request(sc.getMechanismName(), AuthCmd, 1, 0, 0, response)        
      connection ! Write(req.toByteString)      
      
    //If we get any other op while authenticating, stash it                  
    case ExecuteOp(op, opaque) => stash()
  }
 
  /**
   * Receive pf for when we are optimistically writing data to output
   */
  def writing(connection:ActorRef):Receive = LoggingReceive {    
    //Handles an op to send over to memcached
    case ExecuteOp(op, opaque) =>
      val bytes = toBytes(op, opaque)
      
      if (queue.isEmpty){
        connection ! Write(bytes, WriteAck)
        buffer(bytes)
      }
      else{
        
        pipelineBuffer.append(bytes)
        currentPipelineSize += bytes.length
        if (currentPipelineSize >= MaxPipelineSize){
          addPipelineToBuffer
        }
      }
      
    //Handles the ack of a write
    case WriteAck =>
      connection ! ResumeReading
     
    //Handles a response from a request against couch
    case Received(data) =>
      acknowledge(1, connection)
      responses += 1
      handleResponse(data)
 
    //Handles a PeerClosed event
    case PeerClosed =>
      println("Peer closed, shutting down...")
      context stop self    
      
    case other =>
      println("other: " + other)
  }
  
  def addPipelineToBuffer = {
    val next = pipelineBuffer.result
    buffer(next)
    pipelineBuffer = new ByteStringBuilder
    currentPipelineSize = 0 
    next
  }
  
  /**
   * Called to handle response data coming back from memcached
   */
  def handleResponse(data:ByteString){
    //See if we have leftover data from a previous response and if so, join it in the total
    //data to be processed
    var allData = leftovers.fold(data){ bs =>
      bs ++ data   
    }    
    var bytesConsumed = 0
    var dataSlice = allData
    
    //Comsume until we get to the end of the data or we hit a data slice that is less than a full header
    while(bytesConsumed < allData.length && dataSlice.length >= MemcachedHeader.HeaderLength){
      val header = MemcachedHeader(dataSlice)
      val totalBytes = MemcachedHeader.HeaderLength + header.totalBodyLen
      if (dataSlice.length >= totalBytes){
        val currentData = dataSlice.take(totalBytes)     
        
        val resp = Response(header, currentData)   
        client ! resp
        dataSlice = dataSlice.drop(totalBytes)
        bytesConsumed += totalBytes        
      }
      else{
        bytesConsumed = allData.length
      }

    }
    
    //If we have any partial data, save it or else reset the leftovers back to None
    if (!dataSlice.isEmpty){      
      leftovers = Some(dataSlice)
    }
    else leftovers = None 
  }   
  
  /**
   * Converts an operation to the corresponding memcached binary protocol byte string
   * @param op The op to convert
   * @param opaque The opaque to use for the request
   * @return a ByteString representing the binary protovol bytes to send for the op
   */
  def toBytes(op:Operation[_], opaque:Int) ={
    val sd = op.value
    val extras = op.extras(sd)
    val bytes = Request(op.key, op.command, opaque, 0, 0, sd.data, extras).toByteString
    
    
    bytes
  }
  
  /**
   * Called to store a sent set of data until we have ack'd it
   */
  private def buffer(data: ByteString): Unit = {
    queue :+= data    
  } 
  
  private def acknowledge(ack: Int, connection:ActorRef): Unit = {    
    responses = 0
    queue = queue drop 1
    
    if (!queue.isEmpty){
      val next = queue.head
      val co = currentOffset
      log.info("writing {} bytes, {} messages remaining in queue", next.length, queue.size)
      connection ! Write(next, WriteAck)
    }
    else if (currentPipelineSize != 0){
      val co = currentOffset
      val next = addPipelineToBuffer      
      log.info("writing last set of {} bytes", next.length)
      connection ! Write(next, WriteAck)
    }
    else{
      log.info("queue is done, nothing to do...")
    }
  }  
}