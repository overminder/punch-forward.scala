package com.github.overminder.net.punch.peers

import akka.actor._
import akka.io._
import akka.util.ByteString
import akka.serialization._
import scala.concurrent.duration.{DurationInt, Duration => SDuration}
import com.github.nscala_time.time.Imports.{Duration, DateTime}
import java.net.InetSocketAddress
import scala.util.Random
import org.joda.time.DateTimeComparator

object Util {
  def serialize(msg: AnyRef)(implicit system: ActorSystem): Array[Byte] = {
    SerializationExtension(system).serialize(msg).get
  }
  def deserialize[A](bs: Array[Byte], clazz: Class[A])
      (implicit system: ActorSystem) = {
    SerializationExtension(system).deserialize(bs, clazz).get
  }
}

object RawPeer {
  sealed trait Msg extends Serializable
  case class HowAreYou(key: String) extends Msg
  case class FineThanksAndYou(key: String) extends Msg
  case object Bye extends Msg

  case class UnmatchedKey(expected: String, got: String) extends Exception
  case object RemoteSaidBye extends Exception
  case object RemoteDisconnected extends Exception

  case class Punched(peer: ActorRef)

  case class SetReceiver(newReceiver: ActorRef)

  def props(localPort: Int, remotePort: Int, lKey: String,
      rKey: String, receiver: ActorRef) = {
    val local = new InetSocketAddress(localPort)
    val remote = new InetSocketAddress(remotePort)
    Props(classOf[RawPeer], receiver, remote, local, lKey, rKey)
  }
}

// receiver: notified when the hole is punched.
class RawPeer(
  var receiver: ActorRef,
  remote: InetSocketAddress,
  local: InetSocketAddress,
  myKey: String,
  expectedKey: String
) extends Actor with ActorLogging {

  import RawPeer._
  import UdpConnected.{Connect, Connected, Send, Received, Disconnect,
    Disconnected}
  import context.system

  IO(UdpConnected) ! Connect(self, remoteAddress = remote,
    localAddress = Some(local))

  override def receive = {
    case Connected =>
      startHandshake(sender())
  }

  def sendMsg(conn: ActorRef, msg: Msg) {
    conn ! Send(ByteString(Util.serialize(msg)))
  }

  def startHandshake(conn: ActorRef) {
    sendMsg(conn, HowAreYou(myKey))
    context.become(handshakeSent(conn))
  }

  def handshakeSent(conn: ActorRef): Receive = {
    case Received(data) =>
      Util.deserialize(data.toArray, classOf[Msg]) match {
        case HowAreYou(remoteKey) =>
          if (remoteKey == expectedKey) {
            sendMsg(conn, FineThanksAndYou(myKey))
            // Wait for another fineThanks
          }
          else {
            sendMsg(conn, Bye)
            // XXX: Reason
            throw UnmatchedKey(expectedKey, remoteKey)
          }
        case FineThanksAndYou(remoteKey) =>
          if (remoteKey == expectedKey) {
            receiver ! Punched(self)
            context.become(punched(conn))
          }
          else {
            sendMsg(conn, Bye)
            // XXX: Reason
            throw UnmatchedKey(expectedKey, remoteKey)
          }

        case Bye =>
          throw RemoteSaidBye
      }
  }

  def punched(conn: ActorRef): Receive = {
    case SetReceiver(newReceiver) =>
      receiver = newReceiver
    case bs: ByteString =>
      conn.forward(Send(bs))
    case r @ Received(data) =>
      receiver ! r
    case d @ Disconnect =>
      conn ! d
      context.stop(self)
    case d @ Disconnected =>
      receiver ! d
      throw RemoteDisconnected
  }
}

object ReliablePeer {
  // Interacts with the app
  case class AppStream(bs: ByteString)

  case class SetRemote(r: ActorRef)

  // Interacts with the remote
  sealed trait Packet extends Serializable {
    val id: Int
  }
  case class DataPacket(id: Int, payload: ByteString) extends Packet
  case class DataAckPacket(id: Int) extends Packet
  case class FinPacket(id: Int) extends Packet
  case class FinAckPacket(id: Int) extends Packet
  case class RstPacket(id: Int) extends Packet

  case class Resend(pkt: Packet)

  // XXX: Using become will cause a lot of code dup.
  trait State
  case object StateOpen extends State
  case object StateFinSent extends State
  case object StateCloseWait extends State
  case object StateClosed extends State

  case object ConnectionResetByPeer extends Exception

  object Resender {
    case object Tick

    case class RemoveQueued(seqNo: Int)

    case class Queued(nextTime: DateTime, pkt: Packet,
      nextBackOff: SDuration, nRetries: Int) {

      def makeNext(now: DateTime): Queued = {
        Queued.make(now, pkt, nextBackOff, nRetries)
      }
    }

    object Queued {
      def make(now: DateTime, pkt: Packet, nextBackOff: SDuration,
        nRetries: Int = 0) = {
        import com.github.nscala_time.time.Implicits._
        val backOffAugmented = nextBackOff * (
          0.75 + 0.5 * Random.nextDouble())
        Queued(now.plus(new Duration(backOffAugmented.toMillis)), pkt,
          2 * backOffAugmented, nRetries)
      }
    }

    object DateTimeDesc extends Ordering[DateTime] {
      override def compare(a: DateTime, b: DateTime) =
        DateTimeComparator.getInstance.compare(b, a)
    }

    // So that pqueue takes the packet with the smallest nextTime first
    implicit object QueuedDesc extends Ordering[Queued] {
      override def compare(a: Queued, b: Queued) =
        DateTimeDesc.compare(a.nextTime, b.nextTime)
    }
  }

  class Resender(parent: ActorRef) extends Actor with ActorLogging {
    import scala.collection.mutable.{HashSet, PriorityQueue}
    import Resender._
    import context.dispatcher

    context.watch(parent)

    val ticker = context.system.scheduler.schedule(
      100.millis, 100.millis, self, Tick)

    val queued = PriorityQueue[Queued]()

    val queuedIds = HashSet[Int]()

    override def postStop() {
      ticker.cancel()
    }

    def resend() {
      val now = DateTime.now
      def go() {
        if (queued.isEmpty || DateTimeDesc.gt(now, queued.head.nextTime)) {
        }
        else {
          val head = queued.dequeue()
          if (queuedIds contains head.pkt.id) {
            // If not contain: the parent has asked us to stop resending
            // this packet.
            // XXX: Shall we send in batch or one by one?
            parent ! Resend(head.pkt)
            queued.enqueue(head.makeNext(now))
          }
          else {
          }
          go()
        }
      }
      go()
    }

    override def receive = {
      case Tick =>
        resend()
      case RemoveQueued(seqNo) =>
        queuedIds -= seqNo
      case q: Queued =>
        queued += q
        queuedIds += q.pkt.id
    }
  }
}

class ReliablePeer(var remote: ActorRef, app: ActorRef)
  extends Actor with ActorLogging {

  import scala.collection.mutable.HashMap

  import ReliablePeer._
  import Resender._

  var seqGen: Int = 0
  val deliveryQ = HashMap[Int, ByteString]()
  var lastDeliverySeq: Int = 0
  var finSeq: Int = -1
  var state: State = StateOpen

  val resender = context.actorOf(Props(classOf[Resender], self))

  // Sign death pacts
  context.watch(remote)
  context.watch(app)

  def onAppStream(bs: ByteString) {
    seqGen = seqGen + 1
    val pkt = DataPacket(seqGen, bs)
    remote ! pkt
    resender ! Queued.make(DateTime.now, pkt, 100.millis)
  }

  def mergeExtract(): ByteString = {
    val buf = ByteString.newBuilder
    def go(i: Int): Int = {
      val i2 = i + 1
      deliveryQ.remove(i2) match {
        case Some(payload) =>
          buf.append(payload)
          go(i2)
        case None =>
          i
      }
    }
    lastDeliverySeq = go(lastDeliverySeq)
    buf.result
  }

  override def receive = {
    case SetRemote(newRemote) =>
      remote = newRemote

    case AppStream(bs) if state == StateOpen =>
      onAppStream(bs)

    case Resend(pkt) =>
      remote ! pkt

    case dpkt: DataPacket =>
      // Send an ACK anytime a DATA packet is received
      remote ! DataAckPacket(dpkt.id)

      // Check if we need to deliver this payload
      if (lastDeliverySeq < dpkt.id) {
        deliveryQ += ((dpkt.id, dpkt.payload))
        val payloadsToSend = mergeExtract()
        app ! AppStream(payloadsToSend)
      }

    case dack: DataAckPacket =>
      resender ! RemoveQueued(dack.id)

    case fpkt: FinPacket =>
      finSeq = fpkt.id
      remote ! FinAckPacket(finSeq)
      state = StateCloseWait

    case fack: FinAckPacket if state == StateFinSent =>
      finSeq = fack.id
      state = StateCloseWait
      resender ! RemoveQueued(finSeq)

    case rst: RstPacket =>
      throw ConnectionResetByPeer
  }
}

