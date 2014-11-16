package com.github.overminder.net.punch.test

import scala.language.reflectiveCalls

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

  "A ReliablePeer on its own" must {
    case class Fixture(rPeer: ActorRef, payload: ByteString)
    def withFixture(run: Fixture => Unit) {
      val fixture = {
        val rPeer = TestActorRef(Props(classOf[ReliablePeer],
          self, self))
        val payload = ByteString("hello")
        Fixture(rPeer, payload)
      }
      run(fixture)
      system.stop(fixture.rPeer)
    }

    "send a data packet to the remote side when the app sends it an " +
    "AppStream" in withFixture { f =>
      f.rPeer ! AppStream(f.payload)
      expectMsg(DataPacket(1, f.payload))
    }

    "resend data packets to the remote side if ACK is not " +
    "received" in withFixture { f =>
      f.rPeer ! AppStream(f.payload)
      expectMsg(DataPacket(1, f.payload))
      expectMsg(DataPacket(1, f.payload))
    }

    "stop resending data packets to the remote side if ACK " +
    "is received" in withFixture { f =>
      f.rPeer ! AppStream(f.payload)
      expectMsg(DataPacket(1, f.payload))
      f.rPeer ! DataAckPacket(1)
      expectNoMsg()
    }
  }

  "Two ReliablePeers connected with a reliable transport" must {
    case class Fixture(remote: ActorRef, local: ActorRef, payload: ByteString)
    def withFixture(run: Fixture => Unit) {
      val f = {
        val r = TestActorRef(Props(classOf[ReliablePeer],
          system.deadLetters, self))
        val l = TestActorRef(Props(classOf[ReliablePeer],
          r, self))
        r ! SetRemote(l)
        Fixture(r, l, ByteString("YO"))
      }
      run(f)
      system.stop(f.remote)
      system.stop(f.local)
    }

    "cause one side to deliver exactly one AppStream app " +
    "when the other side sends an AppStream" in withFixture { f =>
      f.local ! AppStream(f.payload)
      expectMsg(AppStream(f.payload))
      expectNoMsg()
    }

    "deliver the AppStreams in order" in withFixture { f =>
      val payloads = Seq("First", "Second", "Third") map { case pl =>
        AppStream(ByteString(pl))
      }
      payloads foreach (f.local ! _)
      payloads foreach (expectMsg(_))
      expectNoMsg()
    }
  }
}
