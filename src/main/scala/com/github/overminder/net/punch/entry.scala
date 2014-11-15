package com.github.overminder.net.punch.entry

import akka.actor._
import akka.util.ByteString
import akka.pattern.after
import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

import com.github.overminder.net.punch.peers._

class Printer extends Actor with ActorLogging {
  override def receive = {
    case any => println(s"printer: $any")
  }
}

object Entry {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()
    val printer = system.actorOf(Props[Printer])
    val a1 = system.actorOf(RawPeer.props(1234, 1235, "l", "r", printer))
    val a2 = system.actorOf(RawPeer.props(1235, 1234, "r", "l", printer))

    a1 ! ByteString("HAI")

    import system.dispatcher
    after(5.second, system.scheduler) (Future {
      system.shutdown()
    })
    Thread.sleep(500)
  }
}

