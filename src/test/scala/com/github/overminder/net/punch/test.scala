package com.github.overminder.net.punch.test

import org.scalatest._
import akka.testkit._
import akka.actor._
import akka.util.ByteString
import akka.pattern.ask
import com.github.overminder.net.punch.peers._

class Spec(_system: ActorSystem) extends TestKit(_system) with
  ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
  import ReliablePeer._
  def this() = this(ActorSystem("Spec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def mkFixture = new {
    val rPeer = TestActorRef(Props(classOf[ReliablePeer], 
      self, self))
    val payload = "hello" getBytes "UTF-8"
  }

  "A ReliablePeer on its own" must {
    "send a data packet to the remote side when the app sends it an " +
    "AppStream" in {
      val f = mkFixture
      f.rPeer ! AppStream(f.payload)
      expectMsg(DataPacket(1, ByteString(f.payload)))
      system.stop(f.rPeer)
    }

    "resend data packets to the remote side if ACK is not received" in {
      val f = mkFixture
      f.rPeer ! AppStream(f.payload)
      expectMsg(DataPacket(1, ByteString(f.payload)))
      expectMsg(DataPacket(1, ByteString(f.payload)))
      expectMsg(DataPacket(1, ByteString(f.payload)))
      system.stop(f.rPeer)
    }

    "stop resending data packets to the remote side if ACK is received" in {
      val f = mkFixture
      f.rPeer ! AppStream(f.payload)
      expectMsg(DataPacket(1, ByteString(f.payload)))
      f.rPeer ! DataAckPacket(1)
      expectNoMsg()
      system.stop(f.rPeer)
      //val bss = fishForMessage() {
      //  case s: AppStreams => true
      //  case _p: Packet => false
      //}.asInstanceOf[AppStreams].bss
      //bss.length should be (1)
      //ByteString(bss(0)) should be (ByteString(f.payload))
    }
  }
}
