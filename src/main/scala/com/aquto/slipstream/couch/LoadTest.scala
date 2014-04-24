package com.aquto.slipstream.couch

import com.typesafe.config.ConfigFactory
import akka.actor._
import akka.util.Timeout
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch

object LoadTest extends App{
  import concurrent.duration._
  
  val cfgString = """          
    akka {
        loglevel = "DEBUG"    
        stdout-loglevel = "DEBUG"     
      
      io{
        tcp {
          trace-logging = on
        }
      }
    }        
  """
  val cfg = ConfigFactory.parseString(cfgString)
  val system = ActorSystem("couch", cfg)
  
    
  import system.dispatcher
  val client = system.actorOf(Props(classOf[CouchbaseClient], List(new InetSocketAddress("localhost", 11211)), CouchbaseClient.Bucket(args(0), args(1))))
  Thread.sleep(2000)
  println("About to send ops")
  val latch = new CountDownLatch(400000)
  for(i <- 1 to 4){
    system.actorOf(Props(classOf[RequestActor], i, client, latch))
  }
  latch.await()
  
  println("done")  
  system.shutdown
}

class RequestActor(id:Int, client:ActorRef, latch:CountDownLatch) extends Actor{
  for(i<- 1 to 100000) self ! "req" + i
  var count = 1
  def receive = {
    case s:String => 
      client ! Get(id.toString)
    case resp =>
      count += 1
      latch.countDown()
  }
  
}