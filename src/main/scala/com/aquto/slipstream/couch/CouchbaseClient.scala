package com.aquto.slipstream.couch

import akka.actor._
import java.net.InetSocketAddress


object CouchbaseClient{
  case class RequestContext[T](op:Operation[T], respondTo:ActorRef)
  case class Bucket(name:String, password:String)
}

class CouchbaseClient(nodeAddresses:List[InetSocketAddress], bucket:CouchbaseClient.Bucket) extends Actor{
  import CouchbaseClient._
  import TcpMemcachedNode._
  import BinaryProtocolHandling._
  
  //Just assume one for now...
  val node = TcpMemcachedNode(context, self, nodeAddresses.head, bucket)
  var opaque = 1
  var requests:Map[Int, RequestContext[_]] = Map.empty
  
  def nextOpaque = {
    val next = opaque
    opaque += 1
    next
  }
  
  def noMatchingRequest(opaque:Int, resp:Response) = {
    println(s"No matching request found for response with opaque $opaque ($resp)")
  }  
  
  def receive = {
    case op:Operation[_] =>
      val opq = nextOpaque
      node ! ExecuteOp(op, opq)
      requests += ((opq, RequestContext(op, sender)))
      
    case resp @ Response(header, value) =>
      requests.get(header.opaque).fold(noMatchingRequest(header.opaque, resp)){ ctx =>
        val value = ctx.op.processResponse(resp)
        val respondWith = value match{
          case util.Success(v) => v
          case util.Failure(ex) => Status.Failure(ex)
        }        
        ctx.respondTo ! respondWith
      }
  }
}
